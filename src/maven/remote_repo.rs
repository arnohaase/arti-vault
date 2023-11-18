use std::sync::Arc;

use bytes::Bytes;
use futures_core::Stream;
use hyper::Uri;
use uuid::Uuid;

use crate::blob::blob_storage::BlobStorage;
use crate::maven::coordinates::MavenArtifactRef;
use crate::maven::paths::as_maven_path;
use crate::util::validating_http_downloader::ValidatingHttpDownloader;

pub struct RemoteMavenRepo<S: BlobStorage<Uuid>> {
    downloader: ValidatingHttpDownloader,
    blob_storage: Arc<S>,
}
impl <S: BlobStorage<Uuid>> RemoteMavenRepo<S> {
    pub fn new(base_uri: String, blob_storage: Arc<S>) -> anyhow::Result<RemoteMavenRepo<S>> {
        let mut base_uri = base_uri;
        if !base_uri.ends_with('/') {
            base_uri.push('/');
        }

        // check that the base URI is valid
        Uri::try_from(base_uri.clone())?;

        Ok(RemoteMavenRepo {
            downloader: ValidatingHttpDownloader::new(base_uri)?,
            blob_storage,
        })
    }

    pub async fn get(&self, artifact_ref: MavenArtifactRef) -> anyhow::Result<impl Stream <Item = anyhow::Result<Bytes>> + Send + 'static> {
        self.downloader.get(&as_maven_path(&artifact_ref)).await
    }
}
