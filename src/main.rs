use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use axum::*;
use axum::extract::Path;
use axum::routing::get;
use hyper::{Body, Response};
use tracing::{info, Instrument, span, trace};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;
use crate::blob::transient_blob_storage::TransientBlobStorage;

use crate::maven::coordinates::MavenArtifactRef;
use crate::maven::paths::parse_maven_path;
use crate::maven::remote_repo::RemoteMavenRepo;

pub mod blob;
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
        parse_maven_path(&repo_path).unwrap()
    });

    //TODO reuse repo

    let repo = RemoteMavenRepo::new(
        "https://repo1.maven.org/maven2".to_string(),
        Arc::new(TransientBlobStorage::new()),
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
