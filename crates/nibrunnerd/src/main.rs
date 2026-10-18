#![cfg_attr(test, allow(clippy::unwrap_used, clippy::panic, clippy::expect_used))]

mod cli;

use clap::Parser;
use cli::{Cli, Command};
use nibrunnerd::config::HostConfig;
use nibrunnerd::controllers::lifecycle_controller::LifecycleController;
use nibrunnerd::install::Origin;
use nibrunnerd::{install, run, start};

fn main() -> std::process::ExitCode {
    let command = Cli::parse().command;
    nibrunnerd::install_crypto_provider();

    // An installer talks to whoever ran it, and a daemon talks to the log store. Structured JSON
    // on stderr is right for one and unreadable for the other.
    if command.is_none() {
        install_logger();
    }

    let (config, origin) = match configuration(command.as_ref()) {
        Ok(config) => config,
        Err(code) => return code,
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("the runtime could not be started: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };

    match command {
        None => runtime.block_on(serve(config)),
        Some(Command::Install { force, release }) => {
            runtime.block_on(lay_out(config, force, release, origin))
        }
        Some(Command::Start { force }) => runtime.block_on(bring_up(config, force)),
    }
}

async fn lay_out(
    config: HostConfig,
    force: bool,
    release: Option<std::path::PathBuf>,
    origin: Origin,
) -> std::process::ExitCode {
    let config_file = HostConfig::configured_file();
    match install::run(&config, &config_file, force, release.as_deref()).await {
        Ok(laid) => {
            print_laid(&laid);
            print!("{}", install::next_steps(&config, &config_file, origin));
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{}", error.message());
            std::process::ExitCode::FAILURE
        }
    }
}

/// The layout again, so an edited `config.toml` is what comes up, then systemd.
async fn bring_up(config: HostConfig, force: bool) -> std::process::ExitCode {
    let config_file = HostConfig::configured_file();
    let laid = match install::run(&config, &config_file, force, None).await {
        Ok(laid) => laid,
        Err(error) => {
            eprintln!("{}", error.message());
            return std::process::ExitCode::FAILURE;
        }
    };
    print_laid(&laid);
    let report = match start::run(&config, &install::environment_file(&config_file), &laid) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("{error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    println!();
    for (unit, outcome) in &report.units {
        println!("  {unit:<32} {}", outcome.said());
    }
    if let Some(kept) = &report.kept_credentials {
        println!("\n{}", kept.said());
    }
    if report.all_up() {
        println!(
            "\nUp. It serves what {} says; `journalctl -u nibrunnerd -f` follows it.",
            config.desired_state_file.display()
        );
        return std::process::ExitCode::SUCCESS;
    }
    eprintln!("\nNot up. What went wrong: {}", report.journal());
    std::process::ExitCode::FAILURE
}

/// Each step said itself as it happened; what is left to say is that there were none, and what
/// this host holds against the number it is laid out for.
fn print_laid(laid: &install::Laid) {
    if laid.done.is_empty() {
        println!("  nothing to change");
    }
    if let Some(measured) = &laid.measured {
        println!("  {measured}");
    }
}

/// A host with no configuration at all is given the starting point this binary carries and laid out
/// from it in the same run — so what bootstraps this binary ends with a host that is laid out,
/// rather than one that is half way and waiting to be told to finish.
///
/// A configuration that is *there and wrong* is never written over: that is somebody's file, and
/// the only useful thing to do with it is say which key is wrong.
fn configuration(command: Option<&Command>) -> Result<(HostConfig, Origin), std::process::ExitCode> {
    let refused = |error: &nibrunnerd::config::ConfigError| {
        match command {
            None => tracing::error!(error = %error.message(), "this host is not configured"),
            Some(Command::Install { .. } | Command::Start { .. }) => {
                eprintln!("this host is not configured: {}", error.message())
            }
        }
        std::process::ExitCode::FAILURE
    };

    let path = HostConfig::configured_file();
    match HostConfig::load() {
        Ok(config) => Ok((config, Origin::Operator)),
        Err(_) if matches!(command, Some(Command::Install { .. })) && !path.exists() => {
            install::write_starter_configuration(&path).map_err(|error| {
                eprintln!("{}", error.message());
                std::process::ExitCode::FAILURE
            })?;
            println!("  {} written, because this host had none", path.display());
            HostConfig::load()
                .map(|config| (config, Origin::Starter))
                .map_err(|error| refused(&error))
        }
        Err(error) => Err(refused(&error)),
    }
}

async fn serve(config: HostConfig) -> std::process::ExitCode {
    let inputs = match nibrunnerd::reload::Inputs::read(&config) {
        Ok(inputs) => inputs,
        Err(error) => {
            tracing::error!(%error, "host startup inputs could not be read");
            return std::process::ExitCode::FAILURE;
        }
    };
    let host = match run::build_host(config).await {
        Ok(host) => host,
        Err(error) => {
            tracing::error!(error = %error.to_string(), "this host could not be brought up");
            return std::process::ExitCode::FAILURE;
        }
    };
    tracing::info!(
        state_dir = %host.config.state_dir.display(),
        desired_state_file = %host.config.desired_state_file.display(),
        guest_memory_mib = host.guest_memory_mib,
        firecracker = nibrunnerd::adapters::vm::process::FIRECRACKER_VERSION,
        "nibrunnerd starting"
    );

    let lifecycle = LifecycleController::new(host.clone(), run::host_versions(&host));
    lifecycle.start().await;
    let running: Vec<_> = lifecycle
        .controllers()
        .into_iter()
        .map(|controller| {
            tracing::info!(controller = controller.name(), "controller started");
            tokio::spawn(async move { controller.run().await })
        })
        .collect();

    let reloading = match nibrunnerd::reload::serve(&host, inputs).await {
        Ok(task) => task,
        Err(error) => {
            tracing::error!(%error, "host configuration could not be served");
            lifecycle.stop().await;
            return std::process::ExitCode::FAILURE;
        }
    };

    ready();
    shutdown().await;
    reloading.abort();
    tracing::info!("nibrunnerd stopping; every microVM on this host keeps running");
    host.cron_runs.shutdown().await;
    for task in running {
        task.abort();
    }
    lifecycle.stop().await;
    std::process::ExitCode::SUCCESS
}

/// Said to systemd once the host is built and every controller is running, which is what
/// `Type=notify` makes "started" mean: a `systemctl start` that returns is one this daemon
/// serves after, and one that exits binding a listener it was given returns as the failure it is.
/// Nothing is listening when this is not run under systemd, and nothing is said.
#[cfg(target_os = "linux")]
fn ready() {
    use std::os::linux::net::SocketAddrExt;
    use std::os::unix::net::{SocketAddr, UnixDatagram};

    let Some(socket) = std::env::var_os("NOTIFY_SOCKET") else {
        return;
    };
    let address = match socket.as_encoded_bytes().strip_prefix(b"@") {
        Some(abstract_name) => SocketAddr::from_abstract_name(abstract_name),
        None => SocketAddr::from_pathname(&socket),
    };
    let sent = address.and_then(|address| {
        let sender = UnixDatagram::unbound()?;
        sender.send_to_addr(b"READY=1", &address)
    });
    if let Err(error) = sent {
        tracing::warn!(error = %error, "systemd was not told this host is ready");
    }
}

#[cfg(not(target_os = "linux"))]
fn ready() {}

#[cfg(unix)]
async fn shutdown() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(terminate) => terminate,
        Err(_) => return std::future::pending().await,
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

/// backhand narrates thirteen lines for every layer it packs, and rustls warns for every client
/// that presents an IP literal as SNI — a client quirk the proxy handles, not this host
/// complaining. An error from either still reaches the journal, and both come before the filter
/// that was asked for, so `NIBRUNNER_LOG` can name either crate again and have that answer win.
const QUIETED_DEPENDENCIES: &str = "backhand=warn,rustls=error";

const DEFAULT_LOG_FILTER: &str = "info";

fn log_filter(requested: Option<&str>) -> tracing_subscriber::EnvFilter {
    let quieted =
        |filter: &str| tracing_subscriber::EnvFilter::try_new(format!("{QUIETED_DEPENDENCIES},{filter}"));
    requested
        .and_then(|filter| quieted(filter).ok())
        .unwrap_or_else(|| {
            quieted(DEFAULT_LOG_FILTER).expect("the filter a host falls back to is written in this file")
        })
}

fn install_logger() {
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(log_filter(std::env::var("NIBRUNNER_LOG").ok().as_deref()))
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::stderr)
                .with_current_span(false),
        )
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn through_the_filter(requested: Option<&str>) -> Vec<String> {
        use tracing_subscriber::prelude::*;

        let said = nibrunnerd::test_support::Said::default();
        let guard = nibrunnerd::test_support::listen(
            tracing_subscriber::registry()
                .with(log_filter(requested))
                .with(said.clone()),
        );

        tracing::info!(target: "backhand::v4::filesystem::writer", "Writing Data");
        tracing::error!(target: "backhand::v4::filesystem::writer", "the layer could not be packed");
        tracing::warn!(target: "rustls::msgs::handshake", "Illegal SNI extension");
        tracing::error!(target: "rustls::server::hs", "the handshake could not be finished");
        tracing::info!(target: "nibrunnerd::services::layers", "layer image ready");

        drop(guard);
        said.lines()
    }

    #[test]
    fn the_squashfs_packer_and_rustls_are_quiet_while_this_host_is_not() {
        let said = through_the_filter(None);
        assert!(!said.iter().any(|line| line.contains("Writing Data")), "{said:?}");
        assert!(
            !said.iter().any(|line| line.contains("Illegal SNI extension")),
            "{said:?}"
        );
        assert!(
            said.iter().any(|line| line.contains("layer image ready")),
            "{said:?}"
        );
    }

    #[test]
    fn a_quieted_crate_that_actually_fails_still_reaches_the_journal() {
        let said = through_the_filter(None);
        assert!(
            said.iter()
                .any(|line| line.contains("the layer could not be packed")),
            "{said:?}"
        );
        assert!(
            said.iter()
                .any(|line| line.contains("the handshake could not be finished")),
            "{said:?}"
        );
    }

    #[test]
    fn the_filter_a_host_asks_for_wins_over_the_levels_these_crates_are_pinned_to() {
        let said = through_the_filter(Some("info,rustls=trace"));
        assert!(
            said.iter().any(|line| line.contains("Illegal SNI extension")),
            "{said:?}"
        );
    }

    #[test]
    fn a_filter_that_does_not_parse_falls_back_to_info_with_both_crates_still_quiet() {
        let said = through_the_filter(Some("nibrunnerd=loud"));
        assert!(
            said.iter().any(|line| line.contains("layer image ready")),
            "{said:?}"
        );
        assert!(
            !said.iter().any(|line| line.contains("Illegal SNI extension")),
            "{said:?}"
        );
    }
}
