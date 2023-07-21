#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use actix_web::{get, web, App, HttpServer, Responder};
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use log::info;
use riteraft::{Mailbox, Raft, Result, Store};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use std::sync::{Arc, RwLock};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    raft_addr: String,
    #[structopt(long)]
    peer_addr: Option<String>,
    #[structopt(long)]
    web_server: Option<String>,
    #[structopt(long)]
    node_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub enum Message {
    Insert { key: u64, value: String },
}

const NODE_ID_KEY: u64 = 10000000000000;
#[derive(Clone)]
struct HashStore(Arc<RwLock<HashMap<u64, String>>>);

impl HashStore {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
    fn get(&self, id: u64) -> Option<String> {
        self.0.read().unwrap().get(&id).cloned()
    }
}

#[async_trait]
impl Store for HashStore {
    async fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>> {
        let message: Message = deserialize(message).unwrap();
        let message: Vec<u8> = match message {
            Message::Insert { key, value } => {
                let mut db = self.0.write().unwrap();
                db.insert(key, value.clone());
                info!("apply inserted: ({}, {})", key, value);
                serialize(&value).unwrap()
            }
        };

        let db = self.0.write().unwrap().clone();
        let content = serialize(&db).unwrap();

        let path = format!("raft-{}.db", db.get(&NODE_ID_KEY).unwrap());
        std::fs::write(path.clone(), content).unwrap();

        Ok(message)
    }

    async fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(serialize(&self.0.read().unwrap().clone())?)
    }

    async fn restore(&mut self, id: u64, snapshot: &[u8]) -> Result<()> {
        let new: HashMap<u64, String> = deserialize(snapshot).unwrap();
        let mut db = self.0.write().unwrap();
        let _ = std::mem::replace(&mut *db, new);

        db.insert(NODE_ID_KEY, id.to_string());

        Ok(())
    }

    async fn init(&mut self, id: u64) -> Result<()> {
        let mut db = self.0.write().unwrap();

        let path = format!("raft-{}.db", id);
        if let Ok(content) = std::fs::read_to_string(path) {
            let map: HashMap<u64, String> = deserialize(content.as_bytes()).unwrap();
            *db = map;
        }

        db.insert(NODE_ID_KEY, id.to_string());

        Ok(())
    }
}

#[get("/put/{id}/{name}")]
async fn put(
    data: web::Data<(Arc<Mailbox>, HashStore)>,
    path: web::Path<(u64, String)>,
) -> impl Responder {
    info!("------- recv http client request");
    let message = Message::Insert {
        key: path.0,
        value: path.1.clone(),
    };
    let message = serialize(&message).unwrap();
    let result = data.0.send(message).await.unwrap();
    let result: String = deserialize(&result).unwrap();
    info!("------- process http client request over");
    format!("{:?}", result)
}

#[get("/get/{id}")]
async fn get(data: web::Data<(Arc<Mailbox>, HashStore)>, path: web::Path<u64>) -> impl Responder {
    let id = path.into_inner();

    let response = data.1.get(id);
    format!("{:?}", response)
}

#[get("/leave")]
async fn leave(data: web::Data<(Arc<Mailbox>, HashStore)>) -> impl Responder {
    data.0.leave().await.unwrap();
    "OK".to_string()
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // let decorator = slog_term::TermDecorator::new().build();
    // let drain = slog_term::FullFormat::new(decorator).build().fuse();
    // let drain = slog_async::Async::new(drain).build().fuse();
    // let logger = slog::Logger::root(drain, slog_o!("version" => env!("CARGO_PKG_VERSION")));
    let logger = slog::Logger::root(
        slog::Discard,
        slog_o!("version" => env!("CARGO_PKG_VERSION")),
    );

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // converts log to slog
    //let _log_guard = slog_stdlog::init().unwrap();

    let mut options = Options::from_args();
    options.raft_addr = format!("127.0.0.1:{}", options.raft_addr);
    if let Some(addr) = options.peer_addr {
        options.peer_addr = Some(format!("127.0.0.1:{}", addr));
    }
    if let Some(addr) = options.web_server {
        options.web_server = Some(format!("127.0.0.1:{}", addr));
    }

    // setup runtime for actix
    let local = tokio::task::LocalSet::new();
    let _sys = actix_rt::System::run_in_tokio("server", &local);

    let store = HashStore::new();
    let node_id = options.node_id.unwrap().parse::<u64>().unwrap();

    let raft = Raft::new(options.raft_addr, store.clone(), logger.clone());
    let mailbox = Arc::new(raft.mailbox());
    let (raft_handle, mailbox) = match options.peer_addr {
        Some(addr) => {
            info!("running in follower mode");
            let handle = tokio::spawn(raft.join(addr, node_id));
            (handle, mailbox)
        }
        None => {
            info!("running in leader mode");
            let handle = tokio::spawn(raft.lead());
            (handle, mailbox)
        }
    };

    if let Some(addr) = options.web_server {
        let _server = tokio::spawn(
            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new((mailbox.clone(), store.clone())))
                    .service(put)
                    .service(get)
                    .service(leave)
            })
            .bind(addr)
            .unwrap()
            .run(),
        );
    }

    let result = tokio::try_join!(raft_handle)?;
    result.0?;
    Ok(())
}

#[cfg(test)]
mod test {
    use heed::types::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_is_local_msg() {
        let path = PathBuf::from("./test_db");
        fs::create_dir_all(Path::new(&path)).unwrap();

        let env = heed::EnvOpenOptions::new()
            .map_size(100 * 1024 * 1024)
            .max_dbs(3000)
            .open(path)
            .unwrap();

        let meta_db = env.create_poly_database(Some("meta")).unwrap();

        for i in 0..1_000_001 {
            let key = format!("abc_123_fdsjkahfk_dfhiwqfudnfcei_key_{}", i);
            let val = format!("{}_{}", i, i);
            let mut writer = env.write_txn().unwrap();
            meta_db.put::<_, Str, Str>(&mut writer, &key, &val).unwrap();
            writer.commit().unwrap();

            if i % 100000 == 0 {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                println!("{}: {}", i, now)
            }
        }
    }
}
