use std::fmt::Debug;
use std::hash::Hash;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;

pub struct RetrievedBlob {
    pub data: Pin<Box<dyn Stream<Item = anyhow::Result<Bytes>> + Send + 'static>>,
    pub md5: [u8;16],
    pub sha1: [u8;20],
}

#[async_trait]
pub trait BlobStorage<Key: Clone + Debug + Eq + PartialEq + Hash>: Send + Sync {
    /// The key for looking up blobs
    async fn insert(&self, data: impl Stream<Item=Bytes> + Send)-> anyhow::Result<Key>;

    async fn get(&self, key: &Key, ) -> anyhow::Result<Option<RetrievedBlob>>;

    async fn delete(&self, key: &Key) -> anyhow::Result<bool>;
}

