use std::fmt::Debug;
use std::hash::Hash;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use crate::util::blob::Blob;

#[async_trait]
pub trait BlobStorage<Key: Clone + Debug + Eq + PartialEq + Hash>: Send + Sync {
    /// The key for looking up blobs
    async fn insert(&self, data: impl Stream<Item=anyhow::Result<Bytes>> + Send)-> anyhow::Result<Key>;

    async fn get(&self, key: &Key, ) -> anyhow::Result<Option<Blob>>;

    async fn delete(&self, key: &Key) -> anyhow::Result<bool>;
}

