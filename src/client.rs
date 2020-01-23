//! Contains the etcd client. All API calls are made via the client.

use std::future::Future;

use futures::stream::{self, Stream, StreamExt};
use http::header::{HeaderMap, HeaderValue};
use hyper::client::connect::{Connect, HttpConnector};
use hyper::{Client as Hyper, StatusCode, Uri};
#[cfg(feature = "tls")]
use hyper_tls::HttpsConnector;
use log::error;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json;

use crate::error::{ApiError, Error};
use crate::http::HttpClient;
use crate::version::VersionInfo;

// header! {
//     /// The `X-Etcd-Cluster-Id` header.
//     (XEtcdClusterId, "X-Etcd-Cluster-Id") => [String]
// }
const XETCD_CLUSTER_ID: &str = "X-Etcd-Cluster-Id";

// header! {
//     /// The `X-Etcd-Index` HTTP header.
//     (XEtcdIndex, "X-Etcd-Index") => [u64]
// }
const XETCD_INDEX: &str = "X-Etcd-Index";

// header! {
//     /// The `X-Raft-Index` HTTP header.
//     (XRaftIndex, "X-Raft-Index") => [u64]
// }
const XRAFT_INDEX: &str = "X-Raft-Index";

// header! {
//     /// The `X-Raft-Term` HTTP header.
//     (XRaftTerm, "X-Raft-Term") => [u64]
// }
const XRAFT_TERM: &str = "X-Raft-Term";

/// API client for etcd.
///
/// All API calls require a client.
#[derive(Clone, Debug)]
pub struct Client<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    endpoints: Vec<Uri>,
    http_client: HttpClient<C>,
}

/// A username and password to use for HTTP basic authentication.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BasicAuth {
    /// The username to use for authentication.
    pub username: String,
    /// The password to use for authentication.
    pub password: String,
}

/// A value returned by the health check API endpoint to indicate a healthy cluster member.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Health {
    /// The health status of the cluster member.
    pub health: String,
}

impl Client<HttpConnector> {
    /// Constructs a new client using the HTTP protocol.
    ///
    /// # Parameters
    ///
    /// * handle: A handle to the event loop.
    /// * endpoints: URLs for one or more cluster members. When making an API call, the client will
    /// make the call to each member in order until it receives a successful respponse.
    /// * basic_auth: Credentials for HTTP basic authentication.
    ///
    /// # Errors
    ///
    /// Fails if no endpoints are provided or if any of the endpoints is an invalid URL.
    pub fn new(
        endpoints: &[&str],
        basic_auth: Option<BasicAuth>,
    ) -> Result<Client<HttpConnector>, Error> {
        let hyper = Hyper::builder().keep_alive(true).build_http();

        Client::custom(hyper, endpoints, basic_auth)
    }
}

#[cfg(feature = "tls")]
impl Client<HttpsConnector<HttpConnector>> {
    /// Constructs a new client using the HTTPS protocol.
    ///
    /// # Parameters
    ///
    /// * handle: A handle to the event loop.
    /// * endpoints: URLs for one or more cluster members. When making an API call, the client will
    /// make the call to each member in order until it receives a successful respponse.
    /// * basic_auth: Credentials for HTTP basic authentication.
    ///
    /// # Errors
    ///
    /// Fails if no endpoints are provided or if any of the endpoints is an invalid URL.
    pub fn https(
        endpoints: &[&str],
        basic_auth: Option<BasicAuth>,
    ) -> Result<Client<HttpsConnector<HttpConnector>>, Error> {
        let connector = HttpsConnector::new();
        let hyper = Hyper::builder().keep_alive(true).build(connector);

        Client::custom(hyper, endpoints, basic_auth)
    }
}

impl<C> Client<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    /// Constructs a new client using the provided `hyper::Client`.
    ///
    /// This method allows the user to configure the details of the underlying HTTP client to their
    /// liking. It is also necessary when using X.509 client certificate authentication.
    ///
    /// # Parameters
    ///
    /// * hyper: A fully configured `hyper::Client`.
    /// * endpoints: URLs for one or more cluster members. When making an API call, the client will
    /// make the call to each member in order until it receives a successful respponse.
    /// * basic_auth: Credentials for HTTP basic authentication.
    ///
    /// # Errors
    ///
    /// Fails if no endpoints are provided or if any of the endpoints is an invalid URL.
    pub fn custom(
        hyper: Hyper<C>,
        endpoints: &[&str],
        basic_auth: Option<BasicAuth>,
    ) -> Result<Client<C>, Error> {
        if endpoints.len() < 1 {
            return Err(Error::NoEndpoints);
        }

        let mut uri_endpoints = Vec::with_capacity(endpoints.len());

        for endpoint in endpoints {
            uri_endpoints.push(endpoint.parse()?);
        }

        Ok(Client {
            endpoints: uri_endpoints,
            http_client: HttpClient::new(hyper, basic_auth),
        })
    }

    /// Lets other internal code access the `HttpClient`.
    pub(crate) fn http_client(&self) -> &HttpClient<C> {
        &self.http_client
    }

    /// Lets other internal code access the cluster endpoints.
    pub(crate) fn endpoints(&self) -> &[Uri] {
        &self.endpoints
    }

    /// Runs a basic health check against each etcd member.
    pub fn health<'a>(&'a self) -> impl Stream<Item = Result<Response<Health>, Error>> + 'a {
        stream::iter(self.endpoints.clone())
            .map(move |endpoint| async move {
                let uri = build_url(&endpoint, "health")?;
                self.request(uri).await
            })
            .buffer_unordered(self.endpoints.len())
    }

    /// Returns version information from each etcd cluster member the client was initialized with.
    pub fn versions<'a>(&'a self) -> impl Stream<Item = Result<Response<VersionInfo>, Error>> + 'a {
        stream::iter(self.endpoints.clone())
            .map(move |endpoint| async move {
                let uri = build_url(&endpoint, "version")?;
                self.request(uri).await
            })
            .buffer_unordered(self.endpoints.len())
    }

    /// Lets other internal code make basic HTTP requests.
    pub(crate) fn request<T>(&self, uri: Uri) -> impl Future<Output = Result<Response<T>, Error>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let http_client = self.http_client.clone();

        async move {
            let response = http_client.get(uri).await?;
            let status = response.status();
            let cluster_info = ClusterInfo::from(response.headers());
            let body = hyper::body::to_bytes(response).await?;
            if status == StatusCode::OK {
                match serde_json::from_slice::<T>(&body) {
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
    }
}

/// A wrapper type returned by all API calls.
///
/// Contains the primary data of the response along with information about the cluster extracted
/// from the HTTP response headers.
#[derive(Clone, Debug)]
pub struct Response<T> {
    /// Information about the state of the cluster.
    pub cluster_info: ClusterInfo,
    /// The primary data of the response.
    pub data: T,
}

/// Information about the state of the etcd cluster from an API response's HTTP headers.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ClusterInfo {
    /// An internal identifier for the cluster.
    pub cluster_id: Option<String>,
    /// A unique, monotonically-incrementing integer created for each change to etcd.
    pub etcd_index: Option<u64>,
    /// A unique, monotonically-incrementing integer used by the Raft protocol.
    pub raft_index: Option<u64>,
    /// The current Raft election term.
    pub raft_term: Option<u64>,
}

impl<'a> From<&'a HeaderMap<HeaderValue>> for ClusterInfo {
    fn from(headers: &'a HeaderMap<HeaderValue>) -> Self {
        let cluster_id = headers.get(XETCD_CLUSTER_ID).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec()) {
                Ok(s) => Some(s),
                Err(e) => {
                    error!("{} header decode error: {:?}", XETCD_CLUSTER_ID, e);
                    None
                }
            }
        });

        let etcd_index = headers.get(XETCD_INDEX).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec())
                .map_err(|e| format!("{:?}", e))
                .and_then(|s| s.parse().map_err(|e| format!("{:?}", e)))
            {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("{} header decode error: {}", XETCD_INDEX, e);
                    None
                }
            }
        });

        let raft_index = headers.get(XRAFT_INDEX).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec())
                .map_err(|e| format!("{:?}", e))
                .and_then(|s| s.parse().map_err(|e| format!("{:?}", e)))
            {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("{} header decode error: {}", XRAFT_INDEX, e);
                    None
                }
            }
        });

        let raft_term = headers.get(XRAFT_TERM).and_then(|v| {
            match String::from_utf8(v.as_bytes().to_vec())
                .map_err(|e| format!("{:?}", e))
                .and_then(|s| s.parse().map_err(|e| format!("{:?}", e)))
            {
                Ok(i) => Some(i),
                Err(e) => {
                    error!("{} header decode error: {}", XRAFT_TERM, e);
                    None
                }
            }
        });

        ClusterInfo {
            cluster_id: cluster_id,
            etcd_index: etcd_index,
            raft_index: raft_index,
            raft_term: raft_term,
        }
    }
}

/// Constructs the full URL for the versions API call.
fn build_url(endpoint: &Uri, path: &str) -> Result<Uri, http::uri::InvalidUri> {
    format!("{}{}", endpoint, path).parse()
}
