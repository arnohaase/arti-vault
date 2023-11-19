use std::pin::Pin;
use std::sync::{Arc, RwLock};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use hyper::Uri;
use uuid::Uuid;

use crate::blob::blob_storage::BlobStorage;
use crate::maven::coordinates::MavenArtifactRef;
use crate::maven::paths::as_maven_path;
use crate::util::validating_http_downloader::ValidatingHttpDownloader;

pub struct RemoteMavenRepo<S: BlobStorage<Uuid>, M: RemoteRepoMetadataStore> {
    downloader: ValidatingHttpDownloader,
    blob_storage: Arc<S>,
    metadata_store: Arc<M>, //TODO dyn without M when this is not created as a local variable in the handler method
}

impl <S: BlobStorage<Uuid>, M: RemoteRepoMetadataStore> RemoteMavenRepo<S, M> {
    pub fn new(base_uri: String, blob_storage: Arc<S>, metadata_store: M) -> anyhow::Result<RemoteMavenRepo<S, M>> {
        let mut base_uri = base_uri;
        if !base_uri.ends_with('/') {
            base_uri.push('/');
        }

        // check that the base URI is valid
        Uri::try_from(base_uri.clone())?;

        Ok(RemoteMavenRepo {
            downloader: ValidatingHttpDownloader::new(base_uri)?,
            blob_storage,
            metadata_store: Arc::new(metadata_store),
        })
    }

    //TODO get metadata
    //TODO get SHA1 / MD5

    //TODO introduce 'stream with checksum' struct
    pub async fn get_artifact(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<Pin<Box<dyn Stream <Item = anyhow::Result<Bytes>> + Send + 'static>>> {
        let asdf = self.metadata_store
            .decide_get_artifact(artifact_ref).await?;

        // let asdf = GetArtifactDecision::Download;

        match asdf
        {
            GetArtifactDecision::Local(id) => {
                match self.blob_storage.get(&id).await? {
                    Some(blob) => {
                        Ok(blob.data)
                    }
                    None => {
                        //TODO repair local metadata - the blob is referenced but does not exist
                        Err(anyhow!("TODO local blob not found")) //TODO
                    }
                }
            },
            GetArtifactDecision::Download => {
                match self.downloader.get(&as_maven_path(&artifact_ref)).await {
                    Ok(s) => {

                    }
                    Err(e) => {
                        // self.metadata_store


                    }
                }

                //TODO store locally for caching
                //TODO remember failure

                Ok(Box::pin(self.downloader.get(&as_maven_path(&artifact_ref)).await?))
            }
            GetArtifactDecision::Fail => {
                Err(anyhow!("TODO failed to download")) //TODO
            }
        }
    }
}


pub enum GetArtifactDecision {
    Local(Uuid),
    Download,
    Fail, // failed to download from remote recently, wait before retry
}

#[async_trait]
pub trait RemoteRepoMetadataStore: Send + Sync {
    async fn decide_get_artifact(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<GetArtifactDecision>;

}

pub struct DummyRemoteRepoMetadataStore {
}

#[async_trait]
impl RemoteRepoMetadataStore for DummyRemoteRepoMetadataStore {
    async fn decide_get_artifact(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<GetArtifactDecision> {
        //TODO
        Ok(GetArtifactDecision::Download)
    }
}
