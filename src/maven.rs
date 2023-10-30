

//TODO trait abstraction for remote, internal, local etc. Maven repo?

use bytes::Bytes;
use futures_core::Stream;
use hex::FromHex;
use hyper::{Body, Client, Request, Response, Uri};
use hyper::body::to_bytes;
use hyper::client::HttpConnector;
use hyper::header::USER_AGENT;
use hyper_tls::HttpsConnector;
use crate::util::validated_http_body::{NopHttpBodyValidator, Sha1HttpBodyValidator, ValidatedHttpBody};

pub enum Sha1Handling {
    Require,
    VerifyIfPresent,
    Ignore,
}

pub struct MavenRepoPolicy {
    pub sha1_handling: Sha1Handling,
    //TODO signature checking
    //TODO web of trust (at the higher level)
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
        println!("parsing path {:?}", path);

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
    client: Client<HttpsConnector<HttpConnector>>,
    base_uri: String, // with trailing '/'
    policy: MavenRepoPolicy,
}
impl RemoteMavenRepo {
    pub fn new(base_uri: String, policy: MavenRepoPolicy) -> anyhow::Result<RemoteMavenRepo> {
        let mut base_uri = base_uri;
        if !base_uri.ends_with('/') {
            base_uri.push('/');
        }

        // check that the base URI is valid
        Uri::try_from(base_uri.clone())?;

        Ok(RemoteMavenRepo {
            client: Client::builder()
                .build::<_, Body>(HttpsConnector::new()),
            base_uri,
            policy,
        })
    }

    pub async fn get(&self, artifact_ref: MavenArtifactRef) -> anyhow::Result<impl Stream <Item = anyhow::Result<Bytes>> + Send + 'static> {
        let artifact_path = format!(
            "{}{}/{}/{}/{}",
            self.base_uri,
            artifact_ref.coordinates.group_id.replace('.', "/"),
            artifact_ref.coordinates.artifact_id,
            artifact_ref.coordinates.version.0,
            artifact_ref.file_name,
        );
        let request = Request::builder()
            .method("GET")
            .uri(Uri::try_from(artifact_path.clone())?)
            .header(USER_AGENT, "curl/7.68.0" ) //TODO Maven Central returns a 403 without a user agent - which one to use?
            .body(Body::empty())?;

        println!("getting {:?}", request);

        let artifact_response = self.client.request(request)
            .await?;

        // artifact_response.headers()
        //     .iter()
        //     .for_each(|h| println!("  header: {:?}", h));

        if !artifact_response.status().is_success() {
            return Err(anyhow::Error::msg(format!("upstream request failed: {}", artifact_response.status())));
        }

        let fail_without_sha1 = match self.policy.sha1_handling {
            Sha1Handling::Require => true,
            Sha1Handling::VerifyIfPresent => false,
            Sha1Handling::Ignore => {
                return Ok(ValidatedHttpBody::new(artifact_response.into_body(), NopHttpBodyValidator{}));
            }
        };

        let mut sha1_path = artifact_path;
        sha1_path.push_str(".sha1");
        let request = Request::builder()
            .method("GET")
            .uri(Uri::try_from(sha1_path.clone())?)
            .header(USER_AGENT, "curl/7.68.0" ) //TODO Maven Central returns a 403 without a user agent - which one to use?
            .body(Body::empty())?;

        let sha1_response = self.client.request(request) //TODO parallelize with artifact request
            .await?;

        match Self::extract_expected_sha1(sha1_response).await {
            Ok(expected_hash) => {
                Ok(ValidatedHttpBody::new(artifact_response.into_body(), Sha1HttpBodyValidator::new(expected_hash)))
            }
            Err(e)  => {
                if fail_without_sha1 {
                    Err(e) //TODO logging
                }
                else {
                    Ok(ValidatedHttpBody::new(artifact_response.into_body(), NopHttpBodyValidator{})) //TODO logging
                }
            }
        }
    }

    async fn extract_expected_sha1(response: Response<Body>) -> anyhow::Result<[u8;20]> {
        if !response.status().is_success() {
            return Err(anyhow::Error::msg("failed to retrieve SHA1 file from upstream"));
        }

        let sha1_text_bytes = to_bytes(response.into_body())
            .await?;
        let sha1_str = String::from_utf8(sha1_text_bytes.into())?;
        let expected_hash = <[u8;20]>::from_hex(sha1_str)?;
        Ok(expected_hash)
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_todo() {
        todo!()
    }
}
