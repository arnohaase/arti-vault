use std::fmt::Debug;
use std::hash::Hash;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;

pub struct RetrievedBlob {
    pub data: Box<dyn Stream<Item = std::io::Result<Bytes>>>,
    pub md5: [u8;16],
    pub sha1: [u8;20],
}

#[async_trait]
pub trait BlobStorage<Key: Clone + Debug + Eq + PartialEq + Hash> {
    /// The key for looking up blobs
    async fn insert(&self, data: impl Stream<Item=Bytes> + Send)-> anyhow::Result<Key>;

    async fn get(&self, key: &Key, ) -> anyhow::Result<Option<RetrievedBlob>>;

    async fn delete(&self, key: &Key) -> anyhow::Result<bool>;
}

