#![allow(clippy::needless_pass_by_value)]
use rocket_contrib::{json::{Json, JsonValue}};
use rocket::{Route, response::Responder};
use problem::{Problem, ToProblem};

use crate::db::*;

lazy_static! {
    pub static ref ROUTES: Vec<Route> = routes![getdbs, opendb, gettables, addtable, gettable, deltable, addrecord, getrecords, delrecord, updrecord, sortrecords, addcolumn, delcolumn, movecolumn, updcolumn];
}

#[derive(Debug, Serialize, Deserialize)]
struct OpenReq {
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddTableReq {
    schema: Option<Schema>
}


#[derive(Debug, Serialize, Deserialize)]
struct TablesReq {
    tables: Vec<String>
}


#[derive(Debug, Serialize, Deserialize)]
struct DbList {
    databases: Vec<String>
}

#[get("/")]
fn getdbs() -> Json<DbList> {
    Json(DbList {databases: get_dbs()})
}

#[get("/<id>/open")]
fn opendb(id: String) -> DBResult<JsonValue> {
    get_or_create_db(&mut *DATABASES.lock().unwrap(), &id)?;
    Ok(json!({"handle": &id}))
}


#[get("/<id>/tables")]
fn gettables(id: String) -> DBResult<JsonValue> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    Ok(json!({"tables": db.get_tables()?}))
}

#[post("/<id>/table/<name>", data="<data>")]
fn addtable(id: String, name: String, data: Json<AddTableReq>) -> DBResult<JsonValue> {
    let schema = data.schema.clone().unwrap_or_else(|| Schema {columns: vec![Column {name: "identifier".to_string(), ctype: Type::Integer}]});
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    db.add_table(&name, &schema)?;
    Ok(json!({"status": "ok"}))
}



#[get("/<id>/table/<name>")]
fn gettable(id: String, name: String) -> DBResult<Json<TableInfo>> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let table = db.get_table(&name)?;
    Ok(Json(table.get_info()))
}

#[delete("/<id>/table/<name>")]
fn deltable(id: String, name: String) -> DBResult<JsonValue> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    db.remove_table(&name)?;
    Ok(json!({"status": "ok"}))
}

#[derive(Serialize, Deserialize, Debug)]
struct RecordPrint {
    value: Vec<DBValue>,
}

#[derive(Serialize, Deserialize, Debug)]
struct InType {
    values: Vec<DBValue>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TypePrint {
    types: Vec<Type>,
    values: Vec<DBValue>,
}

#[derive(Serialize, Deserialize, Debug)]
struct GetRecords {
    records: Vec<Record>
}

#[derive(Serialize, Deserialize, Debug)]
struct NewRecord {
    id: u64
}

#[get("/<id>/table/<name>/records")]
fn getrecords(id: String, name: String) -> DBResult<Json<GetRecords>> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let table = db.get_table(&name)?;
    Ok(Json(GetRecords {records: table.get_records()}))
}

#[post("/<id>/table/<name>/record", data="<data>")]
fn addrecord(id: String, name: String, data: Json<RecordPrint>) -> DBResult<Json<NewRecord>> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let mut table = db.get_table(&name)?;
    Ok(Json(NewRecord {id: table.add_record(&data.value.as_slice())?}))
}

#[delete("/<id>/table/<name>/record/<idx>")]
fn delrecord(id: String, name: String, idx: u64) -> DBResult<JsonValue> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let mut table = db.get_table(&name)?;
    table.del_record_by_idx(idx)?;
    Ok(json!({"status": "ok"}))
}

#[put("/<id>/table/<name>/record/<idx>", data="<data>")]
fn updrecord(id: String, name: String, idx: u64, data: Json<RecordPrint>) -> DBResult<JsonValue> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let mut table = db.get_table(&name)?;
    table.upd_record_by_idx(idx, &data.value)?;
    Ok(json!({"status": "ok"}))
}

#[get("/<id>/table/<name>/records/sort_by/<column>")]
fn sortrecords(id: String, name: String, column: String) -> DBResult<Json<GetRecords>> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let table = db.get_table(&name)?;
    Ok(Json(GetRecords {records: table.sort_records(column)?}))
}

#[derive(Serialize, Deserialize, Debug)]
struct ColumnReq {
    column: Column,
    index: Option<usize>
}


#[post("/<id>/table/<name>/column", data="<data>")]
fn addcolumn(id: String, name: String, data: Json<ColumnReq>) -> DBResult<JsonValue> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let mut table = db.get_table(&name)?;
    table.add_column(&data.column, data.index)?;
    Ok(json!({"status": "ok"}))
}

#[delete("/<id>/table/<name>/column/<cname>")]
fn delcolumn(id: String, name: String, cname: String) -> DBResult<JsonValue> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let mut table = db.get_table(&name)?;
    table.del_column(cname)?;
    Ok(json!({"status": "ok"}))
}

#[derive(Serialize, Deserialize, Debug)]
struct MoveReq {
    index: usize
}

#[post("/<id>/table/<name>/column/<cname>/move", data="<data>")]
fn movecolumn(id: String, name: String, cname: String, data: Json<MoveReq>) -> DBResult<JsonValue> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let mut table = db.get_table(&name)?;
    table.move_column(cname, data.index)?;
    Ok(json!({"status": "ok"}))
}

#[derive(Serialize, Deserialize, Debug)]
struct UpdColumnReq {
    column: Column
}

#[put("/<id>/table/<name>/column/<cname>", data="<data>")]
fn updcolumn(id: String, name: String, cname: String, data: Json<UpdColumnReq>) -> DBResult<JsonValue> {
    let mut dbs = DATABASES.lock().unwrap();
    let db = get_db(&mut *dbs, &id)?;
    let mut table = db.get_table(&name)?;
    table.upd_column(cname, &data.column)?;
    Ok(json!({"status": "ok"}))
}
