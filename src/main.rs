pub mod maven;
pub mod util;

use axum::*;
use axum::http::StatusCode;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::str::FromStr;
use axum::extract::Path;
use hyper::{Body, Client, Request, Response, Uri};
use hyper::header::USER_AGENT;
use hyper_tls::HttpsConnector;
use crate::maven::{MavenArtifactRef, MavenRepoPolicy, RemoteMavenRepo, Sha1Handling};

#[tokio::main]
async fn main() {
    //TODO tracing_subscriber::fmt::init();

    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/", get(root))
        .route("/repo/*path", get(repo))
        // `POST /users` goes to `create_user`
        .route("/users", post(create_user));

    let addr = SocketAddr::from_str("127.0.0.1:3000").unwrap();
    println!("serving {}", addr);
    Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();


    // let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    //
    // axum::serve(listener, app).await.unwrap();
}

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Hello, World!"
}

async fn repo(Path(repo_path): Path<String>) -> Response<Body> {
    println!("getting from repo: {}", repo_path);

    let repo = RemoteMavenRepo::new(
        "https://repo1.maven.org/maven2".to_string(),
        MavenRepoPolicy {
            sha1_handling: Sha1Handling::VerifyIfPresent,
        }
    ).unwrap();

    let data = repo.get(MavenArtifactRef::parse_path(&repo_path).unwrap())
        .await
        .unwrap();

    let response_body = Body::wrap_stream(data);
    Response::builder()
        .body(response_body)
        .unwrap()
}

async fn create_user(
    // this argument tells axum to parse the request body
    // as JSON into a `CreateUser` type
    Json(payload): Json<CreateUser>,
) -> (StatusCode, Json<User>) {
    // insert your application logic here
    let user = User {
        id: 1337,
        username: payload.username,
    };

    // this will be converted into a JSON response
    // with a status code of `201 Created`
    (StatusCode::CREATED, Json(user))
}

// the input to our `create_user` handler
#[derive(Deserialize)]
struct CreateUser {
    username: String,
}

// the output to our `create_user` handler
#[derive(Serialize)]
struct User {
    id: u64,
    username: String,
}
