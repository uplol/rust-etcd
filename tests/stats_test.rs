use etcd::stats;

use crate::test::TestClient;
use futures::stream::StreamExt;

mod test;

#[tokio::test]
async fn leader_stats() {
    let client = TestClient::no_destructor();
    stats::leader_stats(&client).await.unwrap();
}

#[tokio::test]
async fn self_stats() {
    let client = TestClient::no_destructor();
    let mut stats = stats::self_stats(&client);

    while let Some(s) = stats.next().await {
        s.unwrap();
    }
}

#[tokio::test]
async fn store_stats() {
    let client = TestClient::no_destructor();
    let mut stats = stats::store_stats(&client);

    while let Some(s) = stats.next().await {
        s.unwrap();
    }
}
