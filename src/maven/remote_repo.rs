use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use hyper::Uri;
use uuid::Uuid;

use crate::blob::blob_storage::{BlobStorage, RetrievedBlob};
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

        match self.metadata_store
            .decide_get_artifact(artifact_ref).await?
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
                    Ok(stream) => {
                        let key = self.blob_storage.insert(stream)
                            .await?;
                        self.metadata_store.register_artifact(artifact_ref, &key)
                            .await?;
                        match self.blob_storage.get(&key)
                            .await?
                        {
                            None => Err(anyhow!("TODO stored but not found")),
                            Some(s) => Ok(s.data),
                        }
                    }
                    Err(e) => {
                        let _ = self.metadata_store.register_failed_download(artifact_ref)
                            .await;
                        Err(anyhow!("falied to download"))
                    }
                }
            }
            GetArtifactDecision::Fail => {
                //TODO distinguish 404 from general network failure - per-artifact retry interval vs. general 'circuit breaker'
                //  -> integrate that logic in the downloader?
                Err(anyhow!("TODO skipping due to a previous failure to download"))
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

    async fn register_artifact(&self, artifact_ref: &MavenArtifactRef, blob_key: &Uuid) -> anyhow::Result<()>;

    async fn register_failed_download(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<()>;
}



pub struct DummyRemoteRepoMetadataStore {
    local_artifacts: RwLock<HashMap<MavenArtifactRef, Uuid>>,
}

impl DummyRemoteRepoMetadataStore {
    pub fn new() -> DummyRemoteRepoMetadataStore {
        DummyRemoteRepoMetadataStore {
            local_artifacts: Default::default(),
        }
    }
}

#[async_trait]
impl RemoteRepoMetadataStore for DummyRemoteRepoMetadataStore {
    async fn decide_get_artifact(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<GetArtifactDecision> {
        //TODO
        Ok(GetArtifactDecision::Download)
    }

    async fn register_artifact(&self, artifact_ref: &MavenArtifactRef, blob_key: &Uuid) -> anyhow::Result<()> {
        // let mut asdf = self.local_artifacts.write().unwrap();
        //
        // asdf.insert(artifact_ref.clone(), blob_key.clone()); //TODO return if already exists
        //

        //TODO
        Ok(())
    }

    async fn register_failed_download(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<()> {

        //TODO
        Ok(())
    }
}
