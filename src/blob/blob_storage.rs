use std::fmt::Debug;
use std::hash::Hash;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;

#[async_trait]
pub trait BlobStorage<Key: Clone + Debug + Eq + PartialEq + Hash> {
    /// The key for looking up blobs
    async fn insert(&self, data: impl Stream<Item=Bytes> + Send)-> anyhow::Result<Key>;

    async fn update(&self, key: &Key, data: impl Stream<Item=Bytes> + Send) -> anyhow::Result<bool>;

    async fn get(&self, key: &Key, ) -> anyhow::Result<Option<Box<dyn Stream<Item = std::io::Result<Bytes>>>>>;

    async fn delete(&self, key: &Key) -> anyhow::Result<bool>;
}

