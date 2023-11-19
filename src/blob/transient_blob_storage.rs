use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures_core::Stream;
use sha1::{Digest, Sha1};
use uuid::Uuid;

use crate::blob::blob_storage::BlobStorage;
use crate::util::blob::Blob;

/// in-memory blob storage, neither optimized nor particularly robust - for testing purposes
pub struct TransientBlobStorage {
    data: Arc<Mutex<HashMap<Uuid, (Vec<u8>, [u8;16], [u8;20])>>>,
}
impl TransientBlobStorage {
    pub fn new() -> TransientBlobStorage {
        TransientBlobStorage {
            data: Default::default(),
        }
    }
}

#[async_trait]
impl BlobStorage<Uuid> for TransientBlobStorage {
    async fn insert(&self, data: impl Stream<Item=anyhow::Result<Bytes>> + Send) -> anyhow::Result<Uuid> {
        let mut data = Box::pin(data);

        let key = Uuid::new_v4();

        let mut data_vec = Vec::new();
        let mut sha1_hasher: Sha1 = Default::default();
        let mut md5_hasher = md5::Context::new();

        loop {
            match data.next().await {
                Some(bytes) => {
                    let bytes = bytes?;
                    sha1_hasher.update(&bytes);
                    md5_hasher.consume(&bytes);
                    data_vec.extend_from_slice(&bytes);
                }
                None =>
                    break,
            }
        }

        self.data.lock()
            .unwrap()
            .insert(
                key.clone(),
                (
                    data_vec,
                    md5_hasher.compute().into(),
                    sha1_hasher.finalize().into(),
                )
            );

        Ok(key)
    }

    async fn get(&self, key: &Uuid) -> anyhow::Result<Option<Blob>> {
        let lock = self.data.lock().unwrap();

        if let Some((data, md5, sha1)) = lock.get(key) {
            let data: Vec<u8> = data.clone();
            let bytes = Bytes::from(data);
            let stream = futures::stream::once(async move { Ok::<_, anyhow::Error>(bytes) });

            Ok(Some(Blob {
                data: Box::pin(stream),
                md5: Some(md5.clone()),
                sha1: Some(sha1.clone()),
            }))
        }
        else {
            Ok(None)
        }
    }

    async fn delete(&self, key: &Uuid) -> anyhow::Result<bool> {
        Ok(self.data.lock().unwrap()
            .remove(key)
            .is_some()
        )
    }
}