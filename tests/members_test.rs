use etcd::members;

use crate::test::TestClient;

mod test;

#[tokio::test]
async fn list() {
    let client = TestClient::no_destructor();
    let members = members::list(&client).await.unwrap().data;
    let member = &members[0];

    assert_eq!(member.name, "default");
}
