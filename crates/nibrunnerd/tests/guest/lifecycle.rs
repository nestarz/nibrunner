//! The document is the only input: what it names is served, and what it stops naming is gone.

use protocol::InstanceState;

#[tokio::test(flavor = "multi_thread")]
async fn an_app_the_document_names_answers_on_its_hostname() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    let took = host.until_state(&app.app_id, InstanceState::Running).await;
    println!("running {took:?} after the document named it");

    let answer = host.get(&app, "/").await.expect("the proxy answers");
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(answer.body, "ok");

    host.stop().await;
}
