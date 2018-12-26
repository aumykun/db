use std::collections::BTreeMap;
use std::mem::discriminant;
use std::sync::Mutex;

use rand::Rng;
use serde_derive::{Serialize, Deserialize};
use sled::Tree;
use problem::{Problem, ToProblem};

use crate::getset::{EasyGet, GetSet};
//use getset::{EasyGet, GetSet};

use self::DBError::*;

#[allow(dead_code)]
#[derive(Debug)]
pub struct DB<KV: GetSet> {
    tree: KV,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, Eq, PartialEq)]
pub enum Type {
    Integer,
    Char,
    CharInvl(char, char),
    Real,
    Str,
    StrCI(char, char),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub enum DBValue {
    Integer(i64),
    Char(char),
    CharInvl(char),
    Real(f64),
    Str(String),
    StrCI(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Column {
    pub name: String,
    pub ctype: Type
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Schema {
    pub columns: Vec<Column>
}

#[derive(Debug, Serialize, Deserialize, Clone, ToProblem)]
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

pub type DBResult<T> = Result<T, DBError>;

pub trait ITable {
    fn get_info(&self) -> TableInfo;
    fn add_record(&mut self, value: &[DBValue]) -> DBResult<u64>;
    fn upd_record(&mut self, ident: u64, value: &[DBValue]) -> DBResult<()>;
    fn del_record(&mut self, ident: u64) -> DBResult<()>;
    fn del_record_by_idx(&mut self, idx: u64) -> DBResult<()>;
    fn upd_record_by_idx(&mut self, idx: u64, value: &[DBValue]) -> DBResult<()>;
    fn sort_records(&self, key: String) -> DBResult<Vec<Record>>;
    fn get_records(&self) -> Vec<Record>;
    fn add_column(&mut self, column: &Column, idx: Option<usize>) -> DBResult<()>;
    fn del_column(&mut self, column: String) -> DBResult<()>;
    fn move_column(&mut self, column: String, idx: usize) -> DBResult<()>;
    fn upd_column(&mut self, old: String, new: &Column) -> DBResult<()>;
}

#[derive(Debug)]
struct Table<'a, KV: GetSet> {
    pub name: String,
    pub schema: Schema,
    records: Vec<u64>,
    db: &'a mut KV
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableInfo {
    name: String,
    schema: Schema
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, PartialOrd)]
pub struct Record {
    pub ident: u64,
    value: Vec<DBValue>
}

lazy_static! {
    pub static ref DATABASES: Mutex<BTreeMap<String, DB<Tree>>> = Mutex::new(BTreeMap::new());
}

pub fn get_dbs() -> Vec<String> {
    DATABASES.lock().unwrap().keys().cloned().collect()
}

pub fn get_or_create_db<'a>(dbs: &'a mut BTreeMap<String, DB<Tree>>, name: &str) -> DBResult<&'a mut DB<Tree>> {
    if !dbs.contains_key(name) {
        dbs.insert(name.to_string(), DB::new(name));
    }
    dbs.get_mut(name).ok_or(DatabaseNotFound)
}

pub fn get_db<'a>(dbs: &'a mut BTreeMap<String, DB<Tree>>, name: &str) -> DBResult<&'a mut DB<Tree>> {
    dbs.get_mut(name).ok_or(DatabaseNotFound)
}

impl DB<Tree> {
    pub fn new(name: &str) -> DB<Tree> {
        let tree = Tree::start_default(&name).unwrap();
        if !tree.has_key("/") {
            let tables: Vec<String> = Vec::new();
            tree.set_value("/", &tables);
        }
        DB {
            tree,
            name: String::from(name),
        }
    }
}

impl<KV> DB<KV>
    where KV: GetSet {
    pub fn get_tables(&self) -> DBResult<Vec<String>> {
        self.tree.get_value("/").ok_or(TableNotFound)
    }

    pub fn add_table(&mut self, name: &str, schema: &Schema) -> DBResult<()> {
        let k = format!("/{}", name);
        if self.tree.has_key(&k) {
            return Err(TableExists);
        }
        let tab = Table::new(name, schema.clone(), vec![], &mut self.tree);
        tab.update();

        let mut tv = self.get_tables()?;
        tv.push(name.to_string());
        self.tree.set_value("/", &tv);
        Ok(())
    }

    pub fn remove_table(&mut self, name: &str) -> DBResult<()> {
        let k : String = format!("/{}", name);
        if !self.tree.has_key(&k) {
            return Err(TableNotFound);
        }

        let mut tv = self.get_tables()?;
        let idx = tv.iter().position(|x| *x == name).ok_or(TableNotFound)?;
        tv.remove(idx);
        self.tree.set_value("/", &tv);

        if self.tree.del(&k) {
            Ok(())
        } else {
            Err(StoreError)
        }
    }

    pub fn get_table<'a>(&'a mut self, name: &str) -> DBResult<impl ITable + 'a> {
        let recs = self.tree.get_value(&format!("/{}", name)).ok_or(TableNotFound)?;
        let schema = self.tree.get_value(&format!("#{}", name)).ok_or(TableNotFound)?;
        Ok(Table::new(name, schema, recs, &mut self.tree))
    }
}

impl<'a, T> Table<'a, T> 
    where T: GetSet {
    pub fn new(name: &str, schema: Schema, records: Vec<u64>, db: &'a mut T) -> Table<'a, T> {
        Table {
            name: name.to_string(),
            schema,
            records,
            db
        }
    }

    fn update(&self) {
        self.db.set_value(&format!("/{}", self.name), &self.records);
        self.db.set_value(&format!("#{}", self.name), &self.schema);
    }
}

impl<'a, KV> ITable for Table<'a, KV>
    where KV: GetSet {
    fn get_info(&self) -> TableInfo {
        TableInfo {
            name: self.name.clone(),
            schema: self.schema.clone()
        }
    }

    fn add_record(&mut self, value: &[DBValue]) -> DBResult<u64> {
        if !self.schema.match_record(value) {
            return Err(TypeMismatch);
        }
        let mut k: u64 = rand::thread_rng().gen();
        while self.db.has_key(&format!("${}", k)) {
            k = rand::thread_rng().gen();
        };
        self.db.set_value(&format!("${}", k), &value.to_vec());
        self.records.push(k);
        self.update();
        Ok(k)
    }

    fn upd_record(&mut self, ident: u64, value: &[DBValue]) -> DBResult<()> {
        self.records.iter().find(|idx| **idx == ident).ok_or(RecordNotFound)?;
        if !self.schema.match_record(value) {
            return Err(TypeMismatch);
        }
        self.db.set_value(&format!("${}", ident), &value.to_vec());
        Ok(())
    }

    fn del_record(&mut self, ident: u64) -> DBResult<()> {
        let idx = self.records.iter().position(|x| *x == ident).ok_or(RecordNotFound)?;
        self.records.remove(idx);
        self.update();
        let k = format!("${}", ident);
        self.db.del(&k);
        Ok(())
    }

    fn del_record_by_idx(&mut self, idx: u64) -> DBResult<()> {
        let rid = self.records.get(idx as usize).ok_or(RecordNotFound)?;
        self.del_record(*rid)
    }

    fn upd_record_by_idx(&mut self, idx: u64, value: &[DBValue]) -> DBResult<()> {
        let rid = self.records.get(idx as usize).ok_or(RecordNotFound)?;
        self.upd_record(*rid, value)
    }

    fn sort_records(&self, key: String) -> DBResult<Vec<Record>> {
        let idx = self.schema.columns.iter().position(|c| (*c).name == key).ok_or(InvalidColumn)?; 
        let mut records = self.get_records();
        records.sort_by(|a, b| a.value[idx].partial_cmp(&b.value[idx]).unwrap());
        Ok(records)
    }

    fn get_records(&self) -> Vec<Record> {
        self.records.iter()
            .map(|idx| Record {
                ident: *idx,
                value: self.db.get_value(&format!("${}", idx)).unwrap()
            })
            .collect::<Vec<_>>()
    }

    fn add_column(&mut self, column: &Column, idx: Option<usize>) -> DBResult<()> {
        let cur_idx = self.schema.columns.iter().position(|c| (*c).name == column.name);
        let idx = idx.unwrap_or_else(|| self.schema.columns.len());
        if cur_idx.is_some() {
            return Err(ColumnExists);
        }
        if idx > self.schema.columns.len() {
            return Err(InvalidPosition);
        }
        let val = column.ctype.defvalue();
        self.schema.columns.insert(idx, column.clone());
        for Record { ident, mut value } in self.get_records() {
            value.insert(idx, val.clone());
            self.db.set_value(&format!("${}", ident), &value);
        }
        self.update();
        Ok(())
    }

    fn del_column(&mut self, column: String) -> DBResult<()> {
        let idx = self.schema.columns.iter().position(|c| (*c).name == column).ok_or(InvalidColumn)?;
        self.schema.columns.remove(idx);
        for Record { ident, mut value } in self.get_records() {
            value.remove(idx);
            self.db.set_value(&format!("${}", ident), &value);
        }
        self.update();
        Ok(())
    }

    fn move_column(&mut self, column: String, idx: usize) -> DBResult<()> {
        let old_idx = self.schema.columns.iter().position(|c| (*c).name == column).ok_or(InvalidColumn)?;
        if idx > self.schema.columns.len() {
            return Err(InvalidPosition);
        }
        let c = self.schema.columns.remove(old_idx);
        self.schema.columns.insert(idx, c);
        for Record { ident, mut value } in self.get_records() {
            let v = value.remove(old_idx);
            value.insert(idx, v);
            self.db.set_value(&format!("${}", ident), &value);
        }
        self.update();
        Ok(())
    }

    fn upd_column(&mut self, old: String, new: &Column) -> DBResult<()> {
        let idx = self.schema.columns.iter().position(|c| (*c).name == old).ok_or(InvalidColumn)?;
        let nidx = self.schema.columns.iter().position(|c| (*c).name == new.name);
        if nidx.is_some() && new.name != old {
            return Err(ColumnExists);
        }
        let recs = self.get_records();
        let mut newrs = Vec::with_capacity(recs.len());
        for Record { ident, value } in recs {
            let mut newr = value.clone();
            let v = newr.remove(idx);
            let val = v.coerce(&new.ctype).ok_or(TypeMismatch)?;
            newr.insert(idx, val);
            newrs.push(Record {ident, value: newr});
        }
        for Record { ident, value } in newrs {
            self.db.set_value(&format!("${}", ident), &value);
        }
        self.schema.columns.remove(idx);
        self.schema.columns.insert(idx, new.clone());
        self.update();
        Ok(())
    }
}

impl Schema {
    pub fn match_record(&self, values: &[DBValue]) -> bool {
        if values.len() != self.columns.len() {
            return false;
        };
        values.iter()
            .map(DBValue::get_type)
            .zip(self.column_types())
            .all(|(sub, sup)| sub.is_subtype(&sup))
    }

    fn column_types(&self) -> impl Iterator<Item = &Type> {
        self.columns.iter().map(|c| &c.ctype)
    }
}

impl DBValue {
    pub fn get_type(&self) -> Type {
        match self {
            DBValue::Integer(_) => Type::Integer,
            DBValue::Char(_) => Type::Char,
            DBValue::CharInvl(c) => Type::CharInvl(*c, *c),
            DBValue::Real(_) => Type::Real,
            DBValue::Str(_) => Type::Str,
            DBValue::StrCI(s) =>
                Type::StrCI(
                    s.chars().min().unwrap_or_else(|| '\0'),
                    s.chars().max().unwrap_or_else(|| '\0')),
        }
    }

    pub fn coerce(&self, t: &Type) -> Option<DBValue> {
        if self.get_type().is_subtype(t) {
            return Some(self.clone());
        }
        match (self, t) {
            (DBValue::Integer(a), Type::Real) => Some(DBValue::Real(*a as f64)),
            (DBValue::Integer(a), Type::Str) => Some(DBValue::Str(a.to_string())),

            (DBValue::Real(f), Type::Integer) => Some(DBValue::Integer(*f as i64)),
            (DBValue::Real(f), Type::Str) => Some(DBValue::Str(f.to_string())),

            (DBValue::Char(c), Type::Str) => Some(DBValue::Str(c.to_string())),
            (DBValue::Char(c), Type::CharInvl(f, t)) => if (f..=t).contains(&c) { Some (DBValue::CharInvl(*c)) } else { None },

            (DBValue::Char(c), Type::StrCI(f, t)) |
                (DBValue::CharInvl(c), Type::StrCI(f, t)) =>
                    if (f..=t).contains(&c) { Some (DBValue::StrCI(c.to_string())) } else { None },

            (DBValue::CharInvl(f), Type::Char) => Some(DBValue::Char(*f)),
            (DBValue::CharInvl(f), Type::Str) => Some(DBValue::Str(f.to_string())),

            (DBValue::Str(s), Type::Integer) => s.parse().ok().map(DBValue::Integer),
            (DBValue::Str(s), Type::Real) => s.parse().ok().map(DBValue::Real),
            (DBValue::Str(s), Type::StrCI(f, t)) => DBValue::StrCI(s.to_string()).coerce(&Type::StrCI(*f, *t)),

            (DBValue::StrCI(s), Type::Integer) => s.parse().ok().map(DBValue::Integer),
            (DBValue::StrCI(s), Type::Real) => s.parse().ok().map(DBValue::Real),
            (DBValue::StrCI(s), Type::Str) => Some(DBValue::Str(s.to_string())),
            (_, _) => None
        }
    }
}



impl Type {
    pub fn is_subtype(&self, other: &Type) -> bool {
        match (self, other) {
            (Type::StrCI('\0', '\0'), Type::StrCI(_, _)) => true,
            (Type::CharInvl(s1, s2), Type::CharInvl(o1, o2)) |
                (Type::StrCI(s1, s2), Type::StrCI(o1, o2)) =>
                (o1..=o2).contains(&s1) && (o1..=o2).contains(&s2),
            (a, b) => discriminant(a) == discriminant(b)
        }
    }

    pub fn defvalue(&self) -> DBValue {
        match self {
            Type::Integer => DBValue::Integer(0),
            Type::Char => DBValue::Char('\0'),
            Type::CharInvl(min, _) => DBValue::CharInvl(*min),
            Type::Real => DBValue::Real(0.0),
            Type::Str => DBValue::Str("".to_string()),
            Type::StrCI(_, _) => DBValue::StrCI("".to_string())
        }
    }
}
