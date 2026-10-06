#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Boot {
    VirtualMachine,
    PreparedRoot,
}

impl Boot {
    pub(crate) fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, &'static str> {
        let arguments: Vec<_> = arguments.into_iter().collect();
        match arguments.as_slice() {
            [] => Ok(Self::VirtualMachine),
            [argument] if argument == "--prepared-root" => Ok(Self::PreparedRoot),
            _ => Err("usage: nibrunner-init [--prepared-root]"),
        }
    }

    pub(crate) fn transport(self) -> guest_contract::channels::ChannelTransport {
        match self {
            Self::VirtualMachine => guest_contract::channels::ChannelTransport::Vsock,
            Self::PreparedRoot => guest_contract::channels::ChannelTransport::Unix,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_roots_are_explicit_and_unknown_modes_cannot_boot_a_vm() {
        assert_eq!(Boot::parse([]), Ok(Boot::VirtualMachine));
        assert_eq!(Boot::parse(["--prepared-root".into()]), Ok(Boot::PreparedRoot));
        for args in [vec!["--process"], vec!["--prepared-root", "extra"]] {
            assert!(Boot::parse(args.into_iter().map(str::to_owned)).is_err());
        }
        assert_eq!(
            Boot::PreparedRoot.transport(),
            guest_contract::channels::ChannelTransport::Unix
        );
    }
}
