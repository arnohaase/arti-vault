use std::pin::Pin;

use bytes::Bytes;
use futures_core::Stream;

pub struct Blob {
    pub data: Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>,
    pub md5: Option<[u8;16]>,
    pub sha1: Option<[u8;20]>,
}
