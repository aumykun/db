#![feature(proc_macro_hygiene, decl_macro)]
#![feature(range_contains)]

#[macro_use]
extern crate rocket_contrib;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate problem_derive;

extern crate bincode;
extern crate rand;
#[macro_use]
extern crate rocket;
extern crate rocket_cors;
extern crate serde;
extern crate sled;

extern crate serde_json;

extern crate problem;

mod db;
mod getset;
mod routes;

use rocket::http::Method;
use rocket_cors::{AllowedHeaders, AllowedOrigins};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DBError {
    OpenError,
    StoreError,
    DatabaseNotFound,
    TableNotFound,
    TableExists,
    RecordNotFound,
    TypeMismatch,
    InvalidColumn,
    ColumnExists,
    InvalidPosition
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Test {
    err: DBError
}

fn main() {
    let cors = rocket_cors::Cors {
        allowed_origins: AllowedOrigins::all(),
        allowed_headers: AllowedHeaders::some(&["Content-Type"]),
        allowed_methods: vec![Method::Get,Method::Post,Method::Put,Method::Delete,Method::Options].into_iter().map(From::from).collect(),
        allow_credentials: true,
        ..rocket_cors::Cors::default()
    };
    rocket::ignite()
        .mount("/db", routes::ROUTES.clone())
        .attach(cors)
        .launch();
}
