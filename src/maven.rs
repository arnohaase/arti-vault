

//TODO trait abstraction for remote, internal, local etc. Maven repo?

use bytes::Bytes;
use futures_core::Stream;
use hyper::Uri;
use tracing::trace;
use crate::util::validating_http_downloader::ValidatingHttpDownloader;

pub enum Sha1Handling {
    Require,
    VerifyIfPresent,
    Ignore,
}

pub struct MavenVersion(pub String);
impl MavenVersion {
    pub fn is_snapshot(&self) -> bool { //TODO unit test
        self.0.ends_with("-SNAPSHOT")
    }
}

pub struct MavenCoordinates {
    pub group_id: String,
    pub artifact_id: String,
    pub version: MavenVersion,
    //TODO classifier
}

pub struct MavenArtifactRef {
    pub coordinates: MavenCoordinates,
    pub file_name: String,
}
impl MavenArtifactRef {
    /// path is the relative path inside a maven repository, i.e. it starts with something like
    ///  "org/..." or "com/..."
    /// The second part of the returned pair is the filename
    pub fn parse_path(path: &str) -> anyhow::Result<MavenArtifactRef> { //TODO unit test
        trace!("parsing path {:?}", path);

        if let Some(last_slash) = path.rfind('/') {
            let (without_filename, file_name) = path.split_at(last_slash);
            let file_name = &file_name[1..];

            if let Some(last_slash) = without_filename.rfind('/') {
                let (without_version, version) = without_filename.split_at(last_slash);
                let version = &version[1..];

                if let Some(last_slash) = without_version.rfind('/') {
                    let (group_id, artifact_id) = without_version.split_at(last_slash);
                    let artifact_id = &artifact_id[1..];

                    return Ok(MavenArtifactRef {
                        coordinates: MavenCoordinates {
                            group_id: group_id.replace('/', "."),
                            artifact_id: artifact_id.to_string(),
                            version: MavenVersion(version.to_string())
                        },
                        file_name: file_name.to_string(),
                    });
                }
            }
        }

        Err(anyhow::Error::msg(format!("not a valid Maven artifact path: {:?}", path)))
    }
}

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
        let artifact_path = format!(
            "{}/{}/{}/{}",
            artifact_ref.coordinates.group_id.replace('.', "/"),
            artifact_ref.coordinates.artifact_id,
            artifact_ref.coordinates.version.0,
            artifact_ref.file_name,
        );

        self.downloader.get(&artifact_path).await
    }
}
