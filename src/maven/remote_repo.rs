use futures_core::Stream;
use hyper::Uri;
use bytes::Bytes;

use crate::maven::coordinates::MavenArtifactRef;
use crate::util::validating_http_downloader::ValidatingHttpDownloader;

pub struct RemoteMavenRepo {
    downloader: ValidatingHttpDownloader,
}
impl RemoteMavenRepo {
    pub fn new(base_uri: String) -> anyhow::Result<RemoteMavenRepo> {
        let mut base_uri = base_uri;
        if !base_uri.ends_with('/') {
            base_uri.push('/');
        }

        // check that the base URI is valid
        Uri::try_from(base_uri.clone())?;

        Ok(RemoteMavenRepo {
            downloader: ValidatingHttpDownloader::new(base_uri)?,
        })
    }

    pub async fn get(&self, artifact_ref: MavenArtifactRef) -> anyhow::Result<impl Stream <Item = anyhow::Result<Bytes>> + Send + 'static> {
        self.downloader.get(&artifact_ref.as_path()).await
    }
}
