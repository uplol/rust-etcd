//! etcd's members API.
//!
//! These API endpoints are used to manage cluster membership.

use hyper::client::connect::Connect;
use hyper::{StatusCode, Uri};
use serde_derive::{Deserialize, Serialize};
use serde_json;
use std::future::Future;

use crate::client::{Client, ClusterInfo, Response};
use crate::error::{ApiError, Error};
use crate::first_ok::{first_ok, Result};

/// An etcd server that is a member of a cluster.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Member {
    /// An internal identifier for the cluster member.
    pub id: String,
    /// A human-readable name for the cluster member.
    pub name: String,
    /// URLs exposing this cluster member's peer API.
    #[serde(rename = "peerURLs")]
    pub peer_urls: Vec<String>,
    /// URLs exposing this cluster member's client API.
    #[serde(rename = "clientURLs")]
    pub client_urls: Vec<String>,
}

/// The request body for `POST /v2/members` and `PUT /v2/members/:id`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct PeerUrls {
    /// The peer URLs.
    #[serde(rename = "peerURLs")]
    peer_urls: Vec<String>,
}

/// A small wrapper around `Member` to match the response of `GET /v2/members`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct ListResponse {
    /// The members.
    members: Vec<Member>,
}

/// Adds a new member to the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * peer_urls: URLs exposing this cluster member's peer API.
pub async fn add<C>(client: &Client<C>, peer_urls: Vec<String>) -> Result<()>
where
    C: Clone + Send + Sync + Connect + 'static,
{
    let peer_urls = PeerUrls { peer_urls };

    let body = match serde_json::to_string(&peer_urls) {
        Ok(body) => body,
        Err(error) => return Err(vec![Error::Serialization(error)]),
    };

    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let http_client = http_client.clone();
        let body = body.clone();

        async move {
            let uri = build_uri(&member, "")?;
            let response = http_client.post(uri, body).await?;
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::to_bytes(response).await?;

            if status == StatusCode::CREATED {
                Ok(Response {
                    data: (),
                    cluster_info,
                })
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

/// Deletes a member from the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * id: The unique identifier of the member to delete.
pub fn delete<C>(client: &Client<C>, id: String) -> impl Future<Output = Result<()>>
where
    C: Clone + Connect + Send + Sync + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let http_client = http_client.clone();
        let id = id.clone();

        async move {
            let uri = build_uri(&member, &format!("/{}", id))?;
            let response = http_client.delete(uri).await?;

            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::to_bytes(response).await?;

            if status == StatusCode::NO_CONTENT {
                Ok(Response {
                    data: (),
                    cluster_info,
                })
            } else {
                match serde_json::from_slice::<ApiError>(&body) {
                    Ok(error) => Err(Error::Api(error)),
                    Err(error) => Err(Error::Serialization(error)),
                }
            }
        }
    })
}

/// Lists the members of the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
pub fn list<C>(client: &Client<C>) -> impl Future<Output = Result<Vec<Member>>>
where
    C: Clone + Connect + Send + Sync + 'static,
{
    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let http_client = http_client.clone();

        async move {
            let uri = build_uri(&member, "")?;
            let response = http_client.get(uri).await?;

            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::to_bytes(response).await?;

            if status == StatusCode::OK {
                match serde_json::from_slice::<ListResponse>(&body) {
                    Ok(data) => Ok(Response {
                        data: data.members,
                        cluster_info,
                    }),
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
}

/// Updates the peer URLs of a member of the cluster.
///
/// # Parameters
///
/// * client: A `Client` to use to make the API call.
/// * id: The unique identifier of the member to update.
/// * peer_urls: URLs exposing this cluster member's peer API.
pub async fn update<C>(client: &Client<C>, id: String, peer_urls: Vec<String>) -> Result<()>
where
    C: Clone + Send + Sync + Connect + 'static,
{
    let peer_urls = PeerUrls { peer_urls };

    let body = match serde_json::to_string(&peer_urls) {
        Ok(body) => body,
        Err(error) => return Err(vec![Error::Serialization(error)]),
    };

    let http_client = client.http_client().clone();

    first_ok(client.endpoints().to_vec(), move |member| {
        let body = body.clone();
        let http_client = http_client.clone();
        let id = id.clone();

        async move {
            let uri = build_uri(&member, &format!("/{}", id))?;
            let response = http_client.put(uri, body).await?;

            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::to_bytes(response).await?;

            if status == StatusCode::NO_CONTENT {
                Ok(Response {
                    data: (),
                    cluster_info,
                })
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

/// Constructs the full URL for an API call.
fn build_uri(endpoint: &Uri, path: &str) -> std::result::Result<Uri, http::uri::InvalidUri> {
    format!("{}v2/members{}", endpoint, path).parse()
}
