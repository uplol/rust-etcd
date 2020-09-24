//! etcd's key-value API.
//!
//! The term "node" in the documentation for this module refers to a key-value pair or a directory
//! of key-value pairs. For example, "/foo" is a key if it has a value, but it is a directory if
//! there other other key-value pairs "underneath" it, such as "/foo/bar".

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use hyper::client::connect::Connect;
use hyper::{StatusCode, Uri};
use serde_derive::{Deserialize, Serialize};
use serde_json;
use std::future::Future;
use tokio::time::timeout;
use url::Url;

pub use crate::error::WatchError;
pub use crate::options::ComparisonConditions;

use crate::client::{Client, ClusterInfo, Response};
use crate::error::{ApiError, Error};
use crate::first_ok::{first_ok, Result};
use crate::options::{DeleteOptions, GetOptions as InternalGetOptions, SetOptions};
use url::form_urlencoded::Serializer;

/// Information about the result of a successful key-value API operation.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct KeyValueInfo {
    /// The action that was taken, e.g. `get`, `set`.
    pub action: Action,
    /// The etcd `Node` that was operated upon.
    pub node: Node,
    /// The previous state of the target node.
    #[serde(rename = "prevNode")]
    pub prev_node: Option<Node>,
}

/// The type of action that was taken in response to a key value API request.
///
/// "Node" refers to the key or directory being acted upon.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Action {
    /// Atomic deletion of a node based on previous state.
    #[serde(rename = "compareAndDelete")]
    CompareAndDelete,
    /// Atomtic update of a node based on previous state.
    #[serde(rename = "compareAndSwap")]
    CompareAndSwap,
    /// Creation of a node that didn't previously exist.
    #[serde(rename = "create")]
    Create,
    /// Deletion of a node.
    #[serde(rename = "delete")]
    Delete,
    /// Expiration of a node.
    #[serde(rename = "expire")]
    Expire,
    /// Retrieval of a node.
    #[serde(rename = "get")]
    Get,
    /// Assignment of a node, which may have previously existed.
    #[serde(rename = "set")]
    Set,
    /// Update of an existing node.
    #[serde(rename = "update")]
    Update,
}

/// An etcd key or directory.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Node {
    /// The new value of the etcd creation index.
    #[serde(rename = "createdIndex")]
    pub created_index: Option<u64>,
    /// Whether or not the node is a directory.
    pub dir: Option<bool>,
    /// An ISO 8601 timestamp for when the key will expire.
    pub expiration: Option<String>,
    /// The name of the key.
    pub key: Option<String>,
    /// The new value of the etcd modification index.
    #[serde(rename = "modifiedIndex")]
    pub modified_index: Option<u64>,
    /// Child nodes of a directory.
    pub nodes: Option<Vec<Node>>,
    /// The key's time to live in seconds.
    pub ttl: Option<i64>,
    /// The value of the key.
    pub value: Option<String>,
}

/// Options for customizing the behavior of `kv::get`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct GetOptions {
    /// If true and the node is a directory, child nodes will be returned as well.
    pub recursive: bool,
    /// If true and the node is a directory, any child nodes returned will be sorted
    /// alphabetically.
    pub sort: bool,
    /// If true, the etcd node serving the response will synchronize with the quorum before
    /// returning the value.
    ///
    /// This is slower but avoids possibly stale data from being returned.
    pub strong_consistency: bool,
}

/// Options for customizing the behavior of `kv::watch`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct WatchOptions {
    /// If given, the watch operation will return the first change at the index or greater,
    /// allowing you to watch for changes that happened in the past.
    pub index: Option<u64>,
    /// Whether or not to watch all child keys as well.
    pub recursive: bool,
    /// If given, the watch operation will time out if it's still waiting after the duration.
    pub timeout: Option<Duration>,
}

/// Deletes a node only if the given current value and/or current modified index match.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to delete.
/// * current_value: If given, the node must currently have this value for the operation to
/// succeed.
/// * current_modified_index: If given, the node must currently be at this modified index for the
/// operation to succeed.
///
/// # Errors
///
/// Fails if the conditions didn't match or if no conditions were given.
pub fn compare_and_delete<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    current_value: Option<&'a str>,
    current_modified_index: Option<u64>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_delete(
        client,
        key,
        DeleteOptions {
            conditions: Some(ComparisonConditions {
                value: current_value,
                modified_index: current_modified_index,
            }),
            ..Default::default()
        },
    )
}

/// Updates a node only if the given current value and/or current modified index
/// match.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to update.
/// * value: The new value for the node.
/// * ttl: If given, the node will expire after this many seconds.
/// * current_value: If given, the node must currently have this value for the operation to
/// succeed.
/// * current_modified_index: If given, the node must currently be at this modified index for the
/// operation to succeed.
///
/// # Errors
///
/// Fails if the conditions didn't match or if no conditions were given.
pub fn compare_and_swap<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    value: &'a str,
    ttl: Option<u64>,
    current_value: Option<&'a str>,
    current_modified_index: Option<u64>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_set(
        client,
        key,
        SetOptions {
            conditions: Some(ComparisonConditions {
                value: current_value,
                modified_index: current_modified_index,
            }),
            ttl: ttl,
            value: Some(value),
            ..Default::default()
        },
    )
}

/// Creates a new key-value pair.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the key-value pair to create.
/// * value: The new value for the node.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the key already exists.
pub fn create<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    value: &'a str,
    ttl: Option<u64>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_set(
        client,
        key,
        SetOptions {
            prev_exist: Some(false),
            ttl,
            value: Some(value),
            ..Default::default()
        },
    )
}

/// Creates a new empty directory.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the directory to create.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the key already exists.
pub fn create_dir<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    ttl: Option<u64>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_set(
        client,
        key,
        SetOptions {
            dir: Some(true),
            prev_exist: Some(false),
            ttl: ttl,
            ..Default::default()
        },
    )
}

/// Creates a new key-value pair in a directory with a numeric key name larger than any of its
/// sibling key-value pairs.
///
/// For example, the first value created with this function under the directory "/foo" will have a
/// key name like "00000000000000000001" automatically generated. The second value created with
/// this function under the same directory will have a key name like "00000000000000000002".
///
/// This behavior is guaranteed by the server.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the directory to create a key-value pair in.
/// * value: The new value for the key-value pair.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the key already exists and is not a directory.
pub fn create_in_order<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    value: &'a str,
    ttl: Option<u64>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_set(
        client,
        key,
        SetOptions {
            create_in_order: true,
            ttl: ttl,
            value: Some(value),
            ..Default::default()
        },
    )
}

/// Deletes a node.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to delete.
/// * recursive: If true, and the key is a directory, the directory and all child key-value
/// pairs and directories will be deleted as well.
///
/// # Errors
///
/// Fails if the key is a directory and `recursive` is `false`.
pub fn delete<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    recursive: bool,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_delete(
        client,
        key,
        DeleteOptions {
            recursive: Some(recursive),
            ..Default::default()
        },
    )
}

/// Deletes an empty directory or a key-value pair at the given key.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to delete.
///
/// # Errors
///
/// Fails if the directory is not empty.
pub fn delete_dir<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_delete(
        client,
        key,
        DeleteOptions {
            dir: Some(true),
            ..Default::default()
        },
    )
}

/// Gets the value of a node.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to retrieve.
/// * options: Options to customize the behavior of the operation.
///
/// # Errors
///
/// Fails if the key doesn't exist.
pub fn get<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    options: GetOptions,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_get(
        client,
        key,
        InternalGetOptions {
            recursive: options.recursive,
            sort: Some(options.sort),
            strong_consistency: options.strong_consistency,
            ..Default::default()
        },
    )
}

/// Sets the value of a key-value pair.
///
/// Any previous value and TTL will be replaced.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the key-value pair to set.
/// * value: The new value for the key-value pair.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the node is a directory.
pub fn set<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    value: &'a str,
    ttl: Option<u64>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_set(
        client,
        key,
        SetOptions {
            ttl: ttl,
            value: Some(value),
            ..Default::default()
        },
    )
}

/// Refreshes the already set etcd key, bumping its TTL without triggering watcher updates.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the key-value pair to set.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the node does not exist.
pub fn refresh<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    ttl: u64,
    conditions: Option<ComparisonConditions<'a>>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_set(
        client,
        key,
        SetOptions {
            ttl: Some(ttl),
            refresh: true,
            prev_exist: Some(true),
            conditions,
            ..Default::default()
        },
    )
}

/// Sets the key to an empty directory.
///
/// An existing key-value pair will be replaced, but an existing directory will not.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the directory to set.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the node is an existing directory.
pub fn set_dir<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    ttl: Option<u64>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_set(
        client,
        key,
        SetOptions {
            dir: Some(true),
            ttl: ttl,
            ..Default::default()
        },
    )
}

/// Updates an existing key-value pair.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the key-value pair to update.
/// * value: The new value for the key-value pair.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the key does not exist.
pub fn update<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    value: &'a str,
    ttl: Option<u64>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_set(
        client,
        key,
        SetOptions {
            prev_exist: Some(true),
            ttl: ttl,
            value: Some(value),
            ..Default::default()
        },
    )
}

/// Updates a directory.
///
/// If the directory already existed, only the TTL is updated. If the key was a key-value pair, its
/// value is removed and its TTL is updated.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to update.
/// * ttl: If given, the node will expire after this many seconds.
///
/// # Errors
///
/// Fails if the node does not exist.
pub fn update_dir<'a, C>(
    client: &'a Client<C>,
    key: &'a str,
    ttl: Option<u64>,
) -> impl Future<Output = Result<KeyValueInfo>> + 'a
where
    C: Clone + Connect + Send + Sync + 'static,
{
    raw_set(
        client,
        key,
        SetOptions {
            dir: Some(true),
            prev_exist: Some(true),
            ttl: ttl,
            ..Default::default()
        },
    )
}

/// Watches a node for changes and returns the new value as soon as a change takes place.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * key: The name of the node to watch.
/// * options: Options to customize the behavior of the operation.
///
/// # Errors
///
/// Fails if `options.index` is too old and has been flushed out of etcd's internal store of the
/// most recent change events. In this case, the key should be queried for its latest
/// "modified index" value and that should be used as the new `options.index` on a subsequent
/// `watch`.
///
/// Fails if a timeout is specified and the duration lapses without a response from the etcd
/// cluster.
pub async fn watch<C>(
    client: &Client<C>,
    key: &str,
    options: WatchOptions,
) -> std::result::Result<Response<KeyValueInfo>, WatchError>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let work = raw_get(
        client,
        key,
        InternalGetOptions {
            recursive: options.recursive,
            wait_index: options.index,
            wait: true,
            ..Default::default()
        },
    );

    if let Some(duration) = options.timeout {
        match timeout(duration.into(), work).await {
            Ok(res) => res.map_err(WatchError::Other),
            Err(_) => Err(WatchError::Timeout),
        }
    } else {
        work.await.map_err(WatchError::Other)
    }
}

/// Constructs the full URL for an API call.
fn build_uri(endpoint: &Uri, path: &str) -> std::result::Result<Uri, http::uri::InvalidUri> {
    format!("{}v2/keys{}", endpoint, path).parse()
}

/// Handles all delete operations.
async fn raw_delete<C>(
    client: &Client<C>,
    key: &str,
    options: DeleteOptions<'_>,
) -> Result<KeyValueInfo>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    let mut query_pairs = HashMap::new();

    if options.recursive.is_some() {
        query_pairs.insert("recursive", format!("{}", options.recursive.unwrap()));
    }

    if options.dir.is_some() {
        query_pairs.insert("dir", format!("{}", options.dir.unwrap()));
    }

    if options.conditions.is_some() {
        let conditions = options.conditions.unwrap();

        if conditions.is_empty() {
            return Err(vec![Error::InvalidConditions]);
        }

        if conditions.modified_index.is_some() {
            query_pairs.insert(
                "prevIndex",
                format!("{}", conditions.modified_index.unwrap()),
            );
        }

        if conditions.value.is_some() {
            query_pairs.insert("prevValue", conditions.value.unwrap().to_owned());
        }
    }

    let http_client = client.http_client().clone();
    let key = key.to_string();

    first_ok(client.endpoints().to_vec(), move |endpoint| {
        let http_client = http_client.clone();
        let query_pairs = query_pairs.clone();
        let key = key.clone();
        async move {
            let url =
                Url::parse_with_params(&build_uri(&endpoint, &key)?.to_string(), query_pairs)?;
            let uri = url.to_string().parse()?;
            let response = http_client.delete(uri).await?;

            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::to_bytes(response).await?;
            if status == StatusCode::OK {
                match serde_json::from_slice::<KeyValueInfo>(&body) {
                    Ok(data) => Ok(Response { data, cluster_info }),
                    Err(error) => Err(Error::Serialization(error)),
                }
            } else {
                match serde_json::from_slice::<ApiError>(&body) {
                    Ok(error) => Err(Error::Api(error)),
                    Err(error) => Err(Error::Serialization(error)),
                }
            }
        }
    })
    .await
}

/// Handles all get operations.
async fn raw_get<C>(
    client: &Client<C>,
    key: &str,
    options: InternalGetOptions,
) -> Result<KeyValueInfo>
where
    C: Clone + Connect + Send + Sync + 'static,
{
    let mut query_pairs = HashMap::new();

    query_pairs.insert("recursive", format!("{}", options.recursive));

    if options.sort.is_some() {
        query_pairs.insert("sorted", format!("{}", options.sort.unwrap()));
    }

    if options.wait {
        query_pairs.insert("wait", "true".to_owned());
    }

    if options.wait_index.is_some() {
        query_pairs.insert("waitIndex", format!("{}", options.wait_index.unwrap()));
    }

    let http_client = client.http_client().clone();
    let key = key.to_string();

    first_ok(client.endpoints().to_vec(), move |endpoint| {
        let http_client = http_client.clone();
        let key = key.clone();
        let query_pairs = query_pairs.clone();

        async move {
            let url =
                Url::parse_with_params(&build_uri(&endpoint, &key)?.to_string(), query_pairs)?;
            let uri = url.to_string().parse()?;
            let response = http_client.get(uri).await?;

            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::to_bytes(response).await?;

            if status == StatusCode::OK {
                match serde_json::from_slice::<KeyValueInfo>(&body) {
                    Ok(data) => Ok(Response { data, cluster_info }),
                    Err(error) => Err(Error::Serialization(error)),
                }
            } else {
                match serde_json::from_slice::<ApiError>(&body) {
                    Ok(error) => Err(Error::Api(error)),
                    Err(error) => Err(Error::Serialization(error)),
                }
            }
        }
    })
    .await
}

/// Handles all set operations.
async fn raw_set<'a, C>(
    client: &Client<C>,
    key: &str,
    options: SetOptions<'a>,
) -> Result<KeyValueInfo>
where
    C: Clone + Connect + Send + Sync + 'static,
{
    fn bool_to_cow(value: bool) -> Cow<'static, str> {
        if value {
            Cow::Borrowed("true")
        } else {
            Cow::Borrowed("false")
        }
    }

    let mut http_options: Vec<(&'static str, Cow<'a, str>)> = vec![];

    if let Some(value) = options.value {
        http_options.push(("value", Cow::Borrowed(value)));
    }

    if let Some(ttl) = options.ttl {
        http_options.push(("ttl", Cow::Owned(ttl.to_string())));
    }

    if let Some(dir) = options.dir {
        http_options.push(("dir", bool_to_cow(dir)));
    }

    let prev_exist = match options.prev_exist {
        Some(prev_exist) => Some(prev_exist),
        None => {
            if options.refresh {
                Some(true)
            } else {
                None
            }
        }
    };

    // If we are calling refresh, we should also ensure we are setting prevExist.
    if let Some(prev_exist) = prev_exist {
        http_options.push(("prevExist", bool_to_cow(prev_exist)));
    }

    if options.refresh {
        http_options.push(("refresh", bool_to_cow(true)));
    }

    if let Some(conditions) = &options.conditions {
        if conditions.is_empty() {
            return Err(vec![Error::InvalidConditions]);
        }

        if let Some(modified_index) = conditions.modified_index {
            http_options.push(("prevIndex", Cow::Owned(modified_index.to_string())));
        }

        if let Some(value) = conditions.value {
            http_options.push(("prevValue", Cow::Borrowed(value)));
        }
    }

    let http_client = client.http_client().clone();
    let key = key.to_string();
    let create_in_order = options.create_in_order;

    first_ok(client.endpoints().to_vec(), move |endpoint| {
        let http_client = http_client.clone();
        let key = key.clone();
        let mut ser = Serializer::new(String::new());
        ser.extend_pairs(http_options.clone());
        let body = ser.finish();

        async move {
            let uri = build_uri(&endpoint, &key)?;
            let response = if create_in_order {
                http_client.post(uri, body).await?
            } else {
                http_client.put(uri, body).await?
            };

            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::to_bytes(response).await?;

            match status {
                StatusCode::CREATED | StatusCode::OK => {
                    match serde_json::from_slice::<KeyValueInfo>(&body) {
                        Ok(data) => Ok(Response { data, cluster_info }),
                        Err(error) => Err(Error::Serialization(error)),
                    }
                }
                _ => match serde_json::from_slice::<ApiError>(&body) {
                    Ok(error) => Err(Error::Api(error)),
                    Err(error) => Err(Error::Serialization(error)),
                },
            }
        }
    })
    .await
}
