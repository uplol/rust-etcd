use crate::{Error, Response};
use std::future::Future;

use hyper::Uri;

/// Executes the given closure with each cluster member and short-circuit returns the first
/// successful result. If all members are exhausted without success, the final error is
/// returned.
pub async fn first_ok<F, U, V, E>(
    endpoints: Vec<Uri>,
    callback: F,
) -> std::result::Result<V, Vec<E>>
where
    F: Fn(Uri) -> U,
    U: Future<Output = std::result::Result<V, E>>,
{
    let mut errors = Vec::with_capacity(endpoints.len());

    for endpoint in endpoints {
        match (callback)(endpoint).await {
            Ok(result) => return Ok(result),
            Err(err) => errors.push(err),
        }
    }

    Err(errors)
}

pub type Result<T> = std::result::Result<Response<T>, Vec<Error>>;
