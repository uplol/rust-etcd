use std::fs::File;
use std::io::Read;
use std::ops::Deref;

use etcd::{kv, Client};
use hyper::client::connect::Connect;
use hyper::client::{Client as Hyper, HttpConnector};
use hyper_tls::HttpsConnector;
use native_tls::{Certificate, Identity, TlsConnector};

/// Wrapper around Client that automatically cleans up etcd after each test.
pub struct TestClient<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    c: Client<C>,
    run_destructor: bool,
}

impl TestClient<HttpConnector> {
    /// Creates a new client for a test.
    #[allow(dead_code)]
    pub async fn new() -> TestClient<HttpConnector> {
        let tc = TestClient {
            c: Client::new(&["http://etcd:2379"], None).unwrap(),
            run_destructor: true,
        };

        kv::delete(&tc.c, "/test", true).await.ok();
        tc
    }

    /// Creates a new client for a test that will not clean up the key space afterwards.
    #[allow(dead_code)]
    pub fn no_destructor() -> TestClient<HttpConnector> {
        TestClient {
            c: Client::new(&["http://etcd:2379"], None).unwrap(),
            run_destructor: false,
        }
    }

    /// Creates a new HTTPS client for a test.
    #[allow(dead_code)]
    pub fn https(use_client_cert: bool) -> TestClient<HttpsConnector<HttpConnector>> {
        let mut ca_cert_file = File::open("/source/tests/ssl/ca.der").unwrap();
        let mut ca_cert_buffer = Vec::new();
        ca_cert_file.read_to_end(&mut ca_cert_buffer).unwrap();

        let mut builder = TlsConnector::builder();
        builder.add_root_certificate(Certificate::from_der(&ca_cert_buffer).unwrap());

        if use_client_cert {
            let mut pkcs12_file = File::open("/source/tests/ssl/client.p12").unwrap();
            let mut pkcs12_buffer = Vec::new();
            pkcs12_file.read_to_end(&mut pkcs12_buffer).unwrap();

            builder.identity(Identity::from_pkcs12(&pkcs12_buffer, "secret").unwrap());
        }

        let tls_connector = builder.build().unwrap();

        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        let https_connector = HttpsConnector::from((http_connector, tls_connector.into()));

        let hyper = Hyper::builder().build(https_connector);

        TestClient {
            c: Client::custom(hyper, &["https://etcdsecure:2379"], None).unwrap(),
            run_destructor: true,
        }
    }
}

impl<C> Drop for TestClient<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    fn drop(&mut self) {
        if self.run_destructor {}
    }
}

impl<C> Deref for TestClient<C>
where
    C: Clone + Connect + Sync + Send + 'static,
{
    type Target = Client<C>;

    fn deref(&self) -> &Self::Target {
        &self.c
    }
}
