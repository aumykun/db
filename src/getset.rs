use bincode::{serialize, deserialize};
use serde::{Serialize};
use serde::de::DeserializeOwned;
use sled::Tree;

pub trait GetSet {
    fn set_unsafe(&self, k: &str, v: Vec<u8>);
    fn get_unsafe(&self, k: &str) -> Vec<u8>;
    fn del(&self, k: &str) -> bool;
    fn has_key(&self, k: &str) -> bool;
}

pub trait EasyGet {
    fn get_value<T: DeserializeOwned>(&self, k: &str) -> Option<T>;
    fn set_value<T: Serialize>(&self, k: &str, v: &T);
}

impl GetSet for Tree {
    fn set_unsafe(&self, k: &str, v: Vec<u8>) {
        self.set(k.as_bytes().to_vec(), v).unwrap();
    }

    fn get_unsafe(&self, k: &str) -> Vec<u8> {
        self.get(k.as_bytes()).unwrap().unwrap().to_vec()
    }

    fn del(&self, k: &str) -> bool {
        self.del(k.as_bytes()).is_ok()
    }

    fn has_key(&self, k: &str) -> bool {
        self.get(k.as_bytes()).unwrap().is_some()
    }
}

impl<TStore> EasyGet for TStore
    where TStore: GetSet
{
    fn get_value<T: DeserializeOwned>(&self, k: &str) -> Option<T> {
        if self.has_key(k) {
            Some(deserialize(&self.get_unsafe(k)).unwrap())
        } else {
            None
        }
    }
    fn set_value<T: Serialize>(&self, k: &str, v: &T) {
        self.set_unsafe(k, serialize(v).unwrap())
    }
}
