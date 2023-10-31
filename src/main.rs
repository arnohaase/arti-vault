use std::net::SocketAddr;
use std::str::FromStr;

use axum::*;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing::{get, post};
use hyper::{Body, Client, Request, Response, Uri};
use hyper::header::USER_AGENT;
use hyper_tls::HttpsConnector;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument, Instrument, span, trace};
use tracing::Level;
use tracing_subscriber::filter::Filtered;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

use crate::maven::{MavenArtifactRef, MavenRepoPolicy, RemoteMavenRepo, Sha1Handling};

pub mod maven;
pub mod util;

#[tokio::main]
async fn main() {

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .with_ansi(true)
        .with_thread_ids(true)
        .with_thread_names(false)
        .finish();

    //TODO log level filtering

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/", get(root))
        .route("/repo/*path", get(repo))
        ;

    let addr = SocketAddr::from_str("127.0.0.1:3000").unwrap();
    info!("listening on {}", addr);
    Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Hello, World!"
}

async fn repo(Path(repo_path): Path<String>) -> Response<Body> {
    let span = span!(Level::TRACE, "repo get", repo_path, correlation_id = Uuid::new_v4().to_string());

    let artifact_ref = span.in_scope(|| {
        trace!("getting from repo: {}", repo_path);
        MavenArtifactRef::parse_path(&repo_path).unwrap()
    });

    //TODO reuse repo
    let repo = RemoteMavenRepo::new(
        "https://repo1.maven.org/maven2".to_string(),
        MavenRepoPolicy {
            sha1_handling: Sha1Handling::VerifyIfPresent,
        }
    ).unwrap();

    let data = repo.get(artifact_ref)
        .instrument(span)
        .await
        .unwrap();

    let response_body = Body::wrap_stream(data);
    Response::builder()
        .body(response_body)
        .unwrap()
}
