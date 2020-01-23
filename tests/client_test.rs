use futures::stream::StreamExt;

use crate::test::TestClient;

mod test;

#[tokio::test]
async fn health() {
    let client = TestClient::no_destructor();
    let mut health = client.health();

    while let Some(response) = health.next().await {
        assert_eq!(response.unwrap().data.health, "true");
    }
}

#[tokio::test]
async fn versions() {
    let client = TestClient::no_destructor();
    let mut versions = client.versions();

    while let Some(response) = versions.next().await {
        let response = response.unwrap();

        assert_eq!(response.data.cluster_version, "2.3.0");
        assert_eq!(response.data.server_version, "2.3.8");
    }
}
