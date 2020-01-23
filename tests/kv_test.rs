use std::time::Duration;

use etcd::kv::{self, Action, GetOptions, KeyValueInfo, WatchError, WatchOptions};
use etcd::{Error, Response};
use futures::future::try_join_all;
use tokio::task::spawn;
use tokio::time::delay_for;

use crate::test::TestClient;

mod test;

#[tokio::test]
async fn create() {
    let client = TestClient::new().await;

    let res = kv::create(&client, "/test/foo", "bar", Some(60))
        .await
        .unwrap();
    let node = res.data.node;

    assert_eq!(res.data.action, Action::Create);
    assert_eq!(node.value.unwrap(), "bar");
    assert_eq!(node.ttl.unwrap(), 60);
}

#[tokio::test]
async fn create_does_not_replace_existing_key() {
    let client = TestClient::new().await;

    kv::create(&client, "/test/foo", "bar", Some(60))
        .await
        .unwrap();

    let errors = kv::create(&client, "/test/foo", "bar", Some(60))
        .await
        .expect_err("expected EtcdError due to pre-existing key");

    for error in errors {
        match error {
            Error::Api(ref error) => assert_eq!(error.message, "Key already exists"),
            _ => panic!("expected EtcdError due to pre-existing key"),
        }
    }
}

#[tokio::test]
async fn create_in_order() {
    let client = TestClient::new().await;

    let requests = (1..4).map(|_| kv::create_in_order(&client, "/test/foo", "bar", None));
    let results: Vec<Response<KeyValueInfo>> = try_join_all(requests).await.unwrap();
    let mut kvis: Vec<KeyValueInfo> = results.into_iter().map(|response| response.data).collect();

    kvis.sort_by_key(|ref kvi| kvi.node.modified_index);

    let keys: Vec<String> = kvis.into_iter().map(|kvi| kvi.node.key.unwrap()).collect();

    assert!(keys[0] < keys[1]);
    assert!(keys[1] < keys[2]);
}

#[tokio::test]
async fn create_in_order_must_operate_on_a_directory() {
    let client = TestClient::new().await;
    kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    kv::create_in_order(&client, "/test/foo", "baz", None)
        .await
        .unwrap_err();
}

#[tokio::test]
async fn compare_and_delete() {
    let client = TestClient::new().await;
    let res = kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let index = res.data.node.modified_index;
    let res = kv::compare_and_delete(&client, "/test/foo", Some("bar"), index)
        .await
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndDelete);
}

#[tokio::test]
async fn compare_and_delete_only_index() {
    let client = TestClient::new().await;

    let res = kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let index = res.data.node.modified_index;
    let res = kv::compare_and_delete(&client, "/test/foo", None, index)
        .await
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndDelete);
}

#[tokio::test]
async fn compare_and_delete_only_value() {
    let client = TestClient::new().await;

    kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let res = kv::compare_and_delete(&client, "/test/foo", Some("bar"), None)
        .await
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndDelete);
}

#[tokio::test]
async fn compare_and_delete_requires_conditions() {
    let client = TestClient::new().await;

    kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let errors = kv::compare_and_delete(&client, "/test/foo", None, None)
        .await
        .expect_err("expected Error::InvalidConditions");

    if errors.len() == 1 {
        match errors[0] {
            Error::InvalidConditions => {}
            _ => panic!("expected Error::InvalidConditions"),
        }
    } else {
        panic!("expected a single error: Error::InvalidConditions");
    }
}

#[tokio::test]
async fn test_compare_and_swap() {
    let client = TestClient::new().await;
    let res = kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let index = res.data.node.modified_index;

    let res = kv::compare_and_swap(&client, "/test/foo", "baz", Some(100), Some("bar"), index)
        .await
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndSwap);
}

#[tokio::test]
async fn test_compare_and_swap_only_index() {
    let client = TestClient::new().await;
    let res = kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let index = res.data.node.modified_index;

    let res = kv::compare_and_swap(&client, "/test/foo", "baz", None, None, index)
        .await
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndSwap);
}

#[tokio::test]
async fn test_compare_and_swap_only_value() {
    let client = TestClient::new().await;
    kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let res = kv::compare_and_swap(&client, "/test/foo", "baz", None, Some("bar"), None)
        .await
        .unwrap();
    assert_eq!(res.data.action, Action::CompareAndSwap);
}

#[tokio::test]
async fn compare_and_swap_requires_conditions() {
    let client = TestClient::new().await;
    kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let errors = kv::compare_and_swap(&client, "/test/foo", "baz", None, None, None)
        .await
        .expect_err("expected Error::InvalidConditions");

    if errors.len() == 1 {
        match errors[0] {
            Error::InvalidConditions => {}
            _ => panic!("expected Error::InvalidConditions"),
        }
    } else {
        panic!("expected a single error: Error::InvalidConditions");
    }
}

#[tokio::test]
async fn get() {
    let client = TestClient::new().await;
    kv::create(&client, "/test/foo", "bar", Some(60))
        .await
        .unwrap();
    let res = kv::get(&client, "/test/foo", GetOptions::default())
        .await
        .unwrap();
    assert_eq!(res.data.action, Action::Get);

    let node = res.data.node;

    assert_eq!(node.value.unwrap(), "bar");
    assert_eq!(node.ttl.unwrap(), 60);
}

#[tokio::test]
async fn get_non_recursive() {
    let client = TestClient::new().await;
    kv::set(&client, "/test/dir/baz", "blah", None)
        .await
        .unwrap();
    kv::set(&client, "/test/foo", "bar", None).await.unwrap();
    let res = kv::get(
        &client,
        "/test",
        GetOptions {
            sort: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let node = res.data.node;

    assert_eq!(node.dir.unwrap(), true);

    let nodes = node.nodes.unwrap();

    assert_eq!(nodes[0].clone().key.unwrap(), "/test/dir");
    assert_eq!(nodes[0].clone().dir.unwrap(), true);
    assert_eq!(nodes[1].clone().key.unwrap(), "/test/foo");
    assert_eq!(nodes[1].clone().value.unwrap(), "bar");
}

#[tokio::test]
async fn get_recursive() {
    let client = TestClient::new().await;

    kv::set(&client, "/test/dir/baz", "blah", None)
        .await
        .unwrap();

    let res = kv::get(
        &client,
        "/test",
        GetOptions {
            recursive: true,
            sort: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nodes = res.data.node.nodes.unwrap();

    assert_eq!(
        nodes[0].clone().nodes.unwrap()[0].clone().value.unwrap(),
        "blah"
    );
}

#[tokio::test]
async fn get_root() {
    let client = TestClient::new().await;

    kv::create(&client, "/test/foo", "bar", Some(60))
        .await
        .unwrap();
    let res = kv::get(&client, "/", GetOptions::default()).await.unwrap();
    assert_eq!(res.data.action, Action::Get);

    let node = res.data.node;

    assert!(node.created_index.is_none());
    assert!(node.modified_index.is_none());
    assert_eq!(node.nodes.unwrap().len(), 1);
    assert_eq!(node.dir.unwrap(), true);
}

#[tokio::test]
async fn https() {
    let client = TestClient::https(true);
    kv::set(&client, "/test/foo", "bar", Some(60))
        .await
        .unwrap();
}

#[tokio::test]
async fn https_without_valid_client_certificate() {
    let client = TestClient::https(false);

    kv::set(&client, "/test/foo", "bar", Some(60))
        .await
        .unwrap_err();
}

#[tokio::test]
async fn set() {
    let client = TestClient::new().await;

    let res = kv::set(&client, "/test/foo", "baz", None).await.unwrap();
    assert_eq!(res.data.action, Action::Set);

    let node = res.data.node;

    assert_eq!(node.value.unwrap(), "baz");
    assert!(node.ttl.is_none());
}

#[tokio::test]
async fn set_and_refresh() {
    let client = TestClient::new().await;

    let res = kv::set(&client, "/test/foo", "baz", Some(30))
        .await
        .unwrap();
    assert_eq!(res.data.action, Action::Set);

    let node = res.data.node;

    assert_eq!(node.value.unwrap(), "baz");
    assert!(node.ttl.is_some());

    let res = kv::refresh(&client, "/test/foo", 30).await.unwrap();
    assert_eq!(res.data.action, Action::Update);

    let node = res.data.node;

    assert_eq!(node.value.unwrap(), "baz");
    assert!(node.ttl.is_some());
}

#[tokio::test]
async fn set_dir() {
    let client = TestClient::new().await;
    kv::set_dir(&client, "/test", None).await.unwrap();
    kv::set_dir(&client, "/test", None)
        .await
        .expect_err("set_dir should fail on an existing dir");
    kv::set(&client, "/test/foo", "bar", None).await.unwrap();
    kv::set_dir(&client, "/test/foo", None).await.unwrap();
}

#[tokio::test]
async fn update() {
    let client = TestClient::new().await;
    kv::create(&client, "/test/foo", "bar", None).await.unwrap();

    let res = kv::update(&client, "/test/foo", "blah", Some(30))
        .await
        .unwrap();
    assert_eq!(res.data.action, Action::Update);

    let node = res.data.node;

    assert_eq!(node.value.unwrap(), "blah");
    assert_eq!(node.ttl.unwrap(), 30);
}

#[tokio::test]
async fn update_requires_existing_key() {
    let client = TestClient::new().await;

    let errors = kv::update(&client, "/test/foo", "bar", None)
        .await
        .expect_err("expected EtcdError due to missing key");

    match errors[0] {
        Error::Api(ref error) => assert_eq!(error.message, "Key not found"),
        _ => panic!("expected EtcdError due to missing key"),
    }
}

#[tokio::test]
async fn update_dir() {
    let client = TestClient::new().await;

    kv::create_dir(&client, "/test", None).await.unwrap();
    let res = kv::update_dir(&client, "/test", Some(60)).await.unwrap();
    assert_eq!(res.data.node.ttl.unwrap(), 60);
}

#[tokio::test]
async fn update_dir_replaces_key() {
    let client = TestClient::new().await;

    kv::set(&client, "/test/foo", "bar", None).await.unwrap();
    let res = kv::update_dir(&client, "/test/foo", Some(60))
        .await
        .unwrap();
    let node = res.data.node;

    assert_eq!(node.value.unwrap(), "");
    assert_eq!(node.ttl.unwrap(), 60);
}

#[tokio::test]
async fn update_dir_requires_existing_dir() {
    let client = TestClient::new().await;
    kv::update_dir(&client, "/test", None).await.unwrap_err();
}

#[tokio::test]
async fn delete() {
    let client = TestClient::new().await;
    kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let res = kv::delete(&client, "/test/foo", false).await.unwrap();
    assert_eq!(res.data.action, Action::Delete);
}

#[tokio::test]
async fn create_dir() {
    let client = TestClient::new().await;

    let res = kv::create_dir(&client, "/test/dir", None).await.unwrap();
    assert_eq!(res.data.action, Action::Create);

    let node = res.data.node;

    assert!(node.dir.is_some());
    assert!(node.value.is_none());
}

#[tokio::test]
async fn delete_dir() {
    let client = TestClient::new().await;

    kv::create_dir(&client, "/test/dir", None).await.unwrap();
    let res = kv::delete_dir(&client, "/test/dir").await.unwrap();
    assert_eq!(res.data.action, Action::Delete);
}

#[tokio::test]
async fn watch() {
    let client = TestClient::new().await;
    kv::create(&client, "/test/foo", "bar", None).await.unwrap();
    let child = spawn(async {
        let client = TestClient::no_destructor();
        kv::set(&client, "/test/foo", "baz", None).await.unwrap();
    });

    let res = kv::watch(&client, "/test/foo", WatchOptions::default())
        .await
        .unwrap();
    assert_eq!(res.data.node.value.unwrap(), "baz");
    child.await.unwrap()
}

#[tokio::test]
async fn watch_cancel() {
    let client = TestClient::new().await;
    kv::create(&client, "/test/foo", "bar", None).await.unwrap();

    let err = kv::watch(
        &client,
        "/test/foo",
        WatchOptions {
            timeout: Some(Duration::from_millis(1)),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    match err {
        WatchError::Timeout => {}
        _ => panic!("unexpected error"),
    }
}

#[tokio::test]
async fn watch_index() {
    let client = TestClient::new().await;
    let res = kv::set(&client, "/test/foo", "bar", None).await.unwrap();

    let index = res.data.node.modified_index;

    let res = kv::watch(
        &client,
        "/test/foo",
        WatchOptions {
            index,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let node = res.data.node;

    assert_eq!(node.modified_index, index);
    assert_eq!(node.value.unwrap(), "bar");
}

#[tokio::test]
async fn watch_recursive() {
    let child = spawn(async {
        let client = TestClient::no_destructor();
        delay_for(Duration::from_millis(100)).await;
        kv::set(&client, "/test/foo/bar", "baz", None)
            .await
            .unwrap()
    });

    let client = TestClient::new().await;
    let res = kv::watch(
        &client,
        "/test",
        WatchOptions {
            recursive: true,
            timeout: Some(Duration::from_millis(1000)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let node = res.data.node;

    assert_eq!(node.key.unwrap(), "/test/foo/bar");
    assert_eq!(node.value.unwrap(), "baz");
    child.await.unwrap();
}
