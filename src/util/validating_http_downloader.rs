use hex::FromHex;
use hyper::{Body, Client, Request, Uri};
use hyper::client::HttpConnector;
use hyper::header::USER_AGENT;
use hyper_tls::HttpsConnector;
use tracing::trace;
use crate::util::blob::Blob;

use crate::util::validating_http_body::{HttpBodyValidator, Md5HttpBodyValidator, Sha1HttpBodyValidator, ValidatingHttpBody};

/// Downloads files relative to a fixed base URI, checking the body's integrity against a hashcode
///  if one is returned in a header.
///
/// Instances do HTTP connection caching internally, so keeping them alive has performance benefits.
pub struct ValidatingHttpDownloader {
    client: Client<HttpsConnector<HttpConnector>>,
    base_uri: String, // with trailing '/'
}
impl ValidatingHttpDownloader {
    pub fn new(base_uri: String) -> anyhow::Result<ValidatingHttpDownloader> {
        let mut base_uri = base_uri;
        if !base_uri.ends_with('/') {
            base_uri.push('/');
        }

        // check that the base URI is valid
        Uri::try_from(base_uri.clone())?;

        Ok(ValidatingHttpDownloader {
            client: Client::builder()
                .build::<_, Body>(HttpsConnector::new()),
            base_uri,
        })
    }

    pub async fn get(&self, path: &str) -> anyhow::Result<Blob> {
        let artifact_path = format!("{}{}", self.base_uri, path);
        let request = Request::builder()
            .method("GET")
            .uri(Uri::try_from(artifact_path.clone())?)
            .header(USER_AGENT, "curl/7.68.0" ) //TODO Maven Central returns a 403 without a user agent - which one to use?
            .body(Body::empty())?;

        trace!("getting {:?}", request);

        let artifact_response = self.client.request(request)
            .await?;

        let sha1_hash_header = artifact_response.headers().get("x-checksum-sha1")
            .or_else(|| artifact_response.headers().get("x-goog-meta-checksum-sha1"))
            .or_else(|| artifact_response.headers().get("etag"))
            ;
        let sha1_string = sha1_hash_header
            .map(|h| h.to_str().unwrap_or(""))
            .map(|s| if s.len() == 42 { &s[1..41] } else { s } );

        let md5_string = artifact_response.headers().get("x-checksum-md5")
            .or_else(|| artifact_response.headers().get("x-goog-meta-checksum-md5"))
            .map(|h| h.to_str().unwrap_or(""))
            ;

        let mut expected_sha1 = None;
        let mut expected_md5 = None;

        let mut validators: Vec<Box<dyn HttpBodyValidator>> = vec![];
        if let Some(sha1) = sha1_string {
            let expected_hash = <[u8;20]>::from_hex(sha1)?; //TODO how to handle invalid content in an sha1 tag? Reject? Fall-through to other hashes?
            expected_sha1 = Some(expected_hash.clone());
            validators.push(Box::new( Sha1HttpBodyValidator::new(expected_hash)));
        }
        if let Some(md5) = md5_string {
            let expected_hash = <[u8;16]>::from_hex(md5)?; //TODO how to handle invalid content in an sha1 tag? Reject? Fall-through to other hashes?
            expected_md5 = Some(expected_hash.clone());
            validators.push(Box::new(Md5HttpBodyValidator::new(expected_hash)));
        }
        Ok(Blob {
            data: Box::pin(ValidatingHttpBody::new(artifact_response.into_body(), validators)),
            md5: expected_md5,
            sha1: expected_sha1,
        })
    }
}
