use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use hyper::Uri;
use uuid::Uuid;

use crate::blob::blob_storage::BlobStorage;
use crate::maven::coordinates::{MavenArtifactId, MavenArtifactRef, MavenGroupId, MavenVersion};
use crate::maven::paths::as_maven_path;
use crate::util::blob::Blob;
use crate::util::change_kind::ChangeKind;
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


    //TODO distinguish between 'not found' and 'error'?

    pub async fn get_artifact(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<Blob> {
        match self.metadata_store
            .decide_get_artifact(artifact_ref).await?
        {
            GetArtifactDecision::Local(id) => {
                match self.blob_storage.get(&id).await? {
                    Some(blob) => {
                        Ok(blob)
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
                        let key = self.blob_storage.insert(stream.data)
                            .await?;
                        self.metadata_store.register_artifact(artifact_ref, &key)
                            .await?;
                        match self.blob_storage.get(&key)
                            .await?
                        {
                            None => Err(anyhow!("TODO stored but not found")),
                            Some(s) => Ok(s),
                        }
                    }
                    Err(_e) => {
                        let _ = self.metadata_store.register_failed_download(artifact_ref)
                            .await;
                        Err(anyhow!("failed to download"))
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

    pub async fn get_artifact_md5(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<[u8;16]> {
        // delegating to 'get_artifact' ensures that the artifact is downloaded if possible (it
        //  will likely be queried next after the checksum is queried), and it does not incur
        //  big overhead since the artifact's data is only fetched as a Stream, i.e. lazily
        Ok(self.get_artifact(artifact_ref)
            .await?
            .md5
            .expect("locally stored artifacts have their md5 checksum stored"))
    }

    pub async fn get_artifact_sha1(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<[u8;20]> {
        // delegating to 'get_artifact' ensures that the artifact is downloaded if possible (it
        //  will likely be queried next after the checksum is queried), and it does not incur
        //  big overhead since the artifact's data is only fetched as a Stream, i.e. lazily
        Ok(self.get_artifact(artifact_ref)
            .await?
            .sha1
            .expect("locally stored artifacts have their md5 checksum stored"))
    }

    pub async fn register_plugin(&self, group_id: MavenGroupId, plugin_metadata: MavenPluginMetadata) -> anyhow::Result<ChangeKind> {
        self.metadata_store.register_plugin(group_id, plugin_metadata).await
    }

    /// Returns true iff the given artifact was previously registered as a plugin, and false otherwise.
    ///  The outcome is no registered plugin for this group / artifact combination, regardless
    pub async fn unregister_plugin(&self, group_id: &MavenGroupId, artifact_id: &MavenArtifactId) -> anyhow::Result<bool> {
        self.metadata_store.unregister_plugin(group_id, artifact_id).await
    }

    pub async fn get_group_metadata(&self, group_id: &MavenGroupId) -> anyhow::Result<MavenGroupMetadata> {
        Ok(MavenGroupMetadata {
            plugins: self.metadata_store.get_plugins(group_id).await?
        })
    }

    pub async fn get_artifact_metadata(&self, group_id: &MavenGroupId, artifact_id: &MavenArtifactId) -> anyhow::Result<Option<MavenArtifactMetadata>> {
        Ok(self.metadata_store.get_artifact_metadata(group_id, artifact_id).await?)
    }

    //TODO get_version_metadata()
}

// https://maven.apache.org/ref/3.9.5/maven-repository-metadata/repository-metadata.html
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MavenArtifactMetadata {
    /// 'What the last version added to the directory is, including both releases and snapshots'
    pub latest_version: MavenVersion,
    /// 'What the last version added to the directory is, for the releases only'
    pub release_version: MavenVersion,
    /// 'Versions available of the artifact (both releases and snapshots)'
    pub versions: Vec<MavenVersion>,
    /// 'When the metadata was last updated. The timestamp is expressed using UTC in the format yyyyMMddHHmmss.
    pub last_updated: String,
}



#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MavenGroupMetadata {
    pub plugins: Vec<MavenPluginMetadata>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MavenPluginMetadata {
    /// 'Display name for the plugin'
    pub name: String,
    /// 'The plugin invocation prefix (i.e. eclipse for eclipse:...)'
    pub prefix: String,
    /// 'The plugin artifactId'
    pub artifact_id: MavenArtifactId,
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

    async fn register_plugin(&self, group_id: MavenGroupId, plugin_metadata: MavenPluginMetadata) -> anyhow::Result<ChangeKind>;
    async fn unregister_plugin(&self, group_id: &MavenGroupId, artifact_id: &MavenArtifactId) -> anyhow::Result<bool>;
    async fn get_plugins(&self, group_id: &MavenGroupId) -> anyhow::Result<Vec<MavenPluginMetadata>>;

    async fn get_artifact_metadata(&self, group_id: &MavenGroupId, artifact_id: &MavenArtifactId) -> anyhow::Result<Option<MavenArtifactMetadata>>;

    //TODO add / update artifact metadata
}



pub struct DummyRemoteRepoMetadataStore {
    local_artifacts: RwLock<HashMap<MavenArtifactRef, Uuid>>,
    failed_downloads: RwLock<HashMap<MavenArtifactRef, Instant>>,
    plugins: RwLock<HashMap<MavenGroupId, HashMap<MavenArtifactId, MavenPluginMetadata>>>,
    artifact_versions: RwLock<HashMap<MavenGroupId, HashMap<MavenArtifactId, Vec<(MavenVersion, String)>>>>,
}

impl DummyRemoteRepoMetadataStore {
    pub fn new() -> DummyRemoteRepoMetadataStore {
        DummyRemoteRepoMetadataStore {
            local_artifacts: Default::default(),
            failed_downloads: Default::default(),
            plugins: Default::default(),
            artifact_versions: Default::default(),
        }
    }
}

#[async_trait]
impl RemoteRepoMetadataStore for DummyRemoteRepoMetadataStore {
    async fn decide_get_artifact(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<GetArtifactDecision> {
        if let Some(key) = self.local_artifacts.read().unwrap().get(artifact_ref) {
            Ok(GetArtifactDecision::Local(key.clone()))
        }
        else if let Some(download_failure) = self.failed_downloads.read().unwrap().get(artifact_ref) {
            let now = Instant::now();

            // configurable retry interval
            if 300 < now.checked_duration_since(download_failure.clone()).unwrap_or(Duration::from_secs(0)).as_secs() {
                self.failed_downloads.write().unwrap().remove(artifact_ref);
                Ok(GetArtifactDecision::Download)
            }
            else {
                Ok(GetArtifactDecision::Fail)
            }
        }
        else {
            Ok(GetArtifactDecision::Download)
        }
    }

    async fn register_artifact(&self, artifact_ref: &MavenArtifactRef, blob_key: &Uuid) -> anyhow::Result<()> {
        //TODO clean up if the artifact was previously registered
        self.local_artifacts.write().unwrap().insert(artifact_ref.clone(), blob_key.clone());
        Ok(())
    }

    async fn register_failed_download(&self, artifact_ref: &MavenArtifactRef) -> anyhow::Result<()> {
        self.failed_downloads.write().unwrap().insert(artifact_ref.clone(), Instant::now());
        Ok(())
    }

    async fn register_plugin(&self, group_id: MavenGroupId, plugin_metadata: MavenPluginMetadata) -> anyhow::Result<ChangeKind> {
        let mut plugins = self.plugins.write().unwrap();
        match plugins.entry(group_id) {
            Entry::Occupied(mut e) => {
                let prev = e.get_mut().insert(plugin_metadata.artifact_id.clone(), plugin_metadata);
                if prev.is_some() {
                    Ok(ChangeKind::Updated)
                }
                else {
                    Ok(ChangeKind::Inserted)
                }
            }
            Entry::Vacant(e) => {
                e.insert([(plugin_metadata.artifact_id.clone(), plugin_metadata)].into());
                Ok(ChangeKind::Inserted)
            }
        }
    }

    async fn unregister_plugin(&self, group_id: &MavenGroupId, artifact_id: &MavenArtifactId) -> anyhow::Result<bool> {
        let mut plugins = self.plugins.write().unwrap();
        match plugins.get_mut(group_id) {
            None => Ok(false),
            Some(g) => {
                Ok(g.remove(artifact_id).is_some())
            }
        }
    }

    async fn get_plugins(&self, group_id: &MavenGroupId) -> anyhow::Result<Vec<MavenPluginMetadata>> {
        match self.plugins.read().unwrap()
            .get(group_id)
        {
            None => Ok(vec![]),
            Some(p) => {
                let plugins: Vec<MavenPluginMetadata> = p.values()
                    .map(|m| m.clone())
                    .collect();
                Ok(plugins)
            }
        }
    }

    async fn get_artifact_metadata(&self, group_id: &MavenGroupId, artifact_id: &MavenArtifactId) -> anyhow::Result<Option<MavenArtifactMetadata>> {
        match self.artifact_versions.read().unwrap().get(group_id) {
            None => Ok(None),
            Some(artifacts) => {
                match artifacts.get(artifact_id) {
                    None => Ok(None),
                    Some(versions) => {
                        if versions.is_empty() {
                            return Ok(None);
                        }

                        let (latest_version, last_updated) = versions.iter()
                            .max_by_key(|(_, timestamp)| timestamp)
                            .map(|(version, timestamp)| (version.clone(), timestamp.clone()))
                            .unwrap();

                        let release_version = versions.iter()
                            .filter(|(version, _)| matches!(version, MavenVersion::Release(_)))
                            .max_by_key(|(_, timestamp)| timestamp)
                            .map(|(version, _)| version.clone())
                            .unwrap();

                        let versions = versions.iter()
                            .map(|(version, _)| version.clone())
                            .collect();

                        Ok(Some(MavenArtifactMetadata {
                            latest_version,
                            release_version,
                            versions,
                            last_updated,
                        }))

                    }
                }
            }
        }
    }
}
