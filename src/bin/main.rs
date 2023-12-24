use kvs::{KvStore, KvsError, Result};
use std::env::current_dir;
use std::path;
use std::process::exit;

fn main() {
    let mut KvStore = KvStore::open(path::Path::new("src/")).unwrap();
    KvStore.set("key1".to_string(), "value1".to_string()).unwrap();
    let mut v1 = KvStore.get("key1".to_string()).unwrap();
    println!("v1: {:?}", v1);
}
