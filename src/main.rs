use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use axum::*;
use axum::extract::{Path, State};
use axum::handler::Handler;
use axum::http::Request;
use axum::response::IntoResponse;
use axum::routing::get;
use hyper::{Body, Response};
use tokio::sync::RwLock;
use tracing::{info, Instrument, span, trace};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;
use crate::blob::transient_blob_storage::TransientBlobStorage;

use crate::maven::paths::parse_maven_path;
use crate::maven::remote_repo::{DummyRemoteRepoMetadataStore, RemoteMavenRepo};

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

    // tracing::subscriber::set_global_default(subscriber)
    //     .expect("setting default subscriber failed");
    //
    //
    // fn asdf(h: impl Handler<Arc<AppData>, Arc<AppData>>) {}


    // build our application with a route
    let app = Router::new()
        // .with_state(AppData{})
        // `GET /` goes to `root`
        .route("/", get(root))
        .route("/repo/*path", get(repo))
        .with_state(Arc::new(AppData{
            repo: RemoteMavenRepo::new(
                "https://repo1.maven.org/maven2".to_string(),
                Arc::new(TransientBlobStorage::new()),
                DummyRemoteRepoMetadataStore {},
            ).unwrap(),
        }))
        //TODO HTTP trace layer

        ;

    let addr = SocketAddr::from_str("127.0.0.1:3000").unwrap();
    info!("listening on {}", addr);
    Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

struct AppData {
    repo: RemoteMavenRepo<TransientBlobStorage, DummyRemoteRepoMetadataStore>,
}

unsafe impl Send for AppData {}
unsafe impl Sync for AppData {}


// basic handler that responds with a static string
async fn root() -> &'static str {
    "Hello, World!" //TODO
}

async fn repo(State(state): State<Arc<AppData>>, Path(repo_path): Path<String>, ) -> Response<Body> {
    let span = span!(Level::TRACE, "repo get", repo_path, correlation_id = Uuid::new_v4().to_string());

    let artifact_ref = span.in_scope(|| {
        trace!("getting from repo: {}", repo_path);
        parse_maven_path(&repo_path).unwrap()
    });

    let data = state.repo.get_artifact(&artifact_ref)
        .instrument(span)
        .await
    //     .unwrap()
    ;
    //
    // let response_body = Body::wrap_stream(data);
    // Response::builder()
    //     .body(response_body)
    //     .unwrap()

    todo!()


}


