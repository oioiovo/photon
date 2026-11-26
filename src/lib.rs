pub mod error;

use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use crate::error::RpcError;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait Service: Send + Sync + 'static {
    fn call(self: Arc<Self>, method: u64, payload: Bytes)
            -> BoxFuture<'static, Result<Bytes, RpcError>>;
}
