//! Versioned Rust endpoint cache. Blocking work (including its path lock) lives off-runtime.
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};
use tars_core::{Result, TarsError};
use tars_protocol::Endpoint;

static LOCKS: Lazy<Mutex<BTreeMap<PathBuf, Weak<Mutex<()>>>>> =
    Lazy::new(|| Mutex::new(BTreeMap::new()));
static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone)]
pub(crate) struct DiscoveryCache {
    path: PathBuf,
    identity: String,
}
#[derive(Serialize, Deserialize)]
struct File {
    version: u32,
    records: BTreeMap<String, Vec<Record>>,
}
#[derive(Serialize, Deserialize)]
struct Record {
    host: String,
    port: u16,
    timeout: u64,
    istcp: i32,
    grid: i32,
    qos: i32,
    weight: u32,
    weight_type: i16,
    auth_type: i32,
    set_id: String,
}
impl From<&Endpoint> for Record {
    fn from(ep: &Endpoint) -> Self {
        Self {
            host: ep.host.clone(),
            port: ep.port,
            timeout: ep.timeout,
            istcp: ep.istcp,
            grid: ep.grid,
            qos: ep.qos,
            weight: ep.weight,
            weight_type: ep.weight_type,
            auth_type: ep.auth_type,
            set_id: ep.set_id.clone(),
        }
    }
}
impl From<Record> for Endpoint {
    fn from(ep: Record) -> Self {
        Self {
            host: ep.host,
            port: ep.port,
            timeout: ep.timeout,
            istcp: ep.istcp,
            grid: ep.grid,
            qos: ep.qos,
            weight: ep.weight,
            weight_type: ep.weight_type,
            auth_type: ep.auth_type,
            set_id: ep.set_id,
        }
    }
}
fn read(path: &std::path::Path) -> Result<File> {
    let file: File = toml::from_str(&std::fs::read_to_string(path)?)
        .map_err(|e| TarsError::Config(e.to_string()))?;
    if file.version != 1 {
        return Err(TarsError::Config(
            "unsupported discovery cache version".into(),
        ));
    }
    Ok(file)
}
impl DiscoveryCache {
    pub fn new(path: String, identity: String) -> Self {
        Self {
            path: PathBuf::from(path),
            identity,
        }
    }
    pub async fn load(&self) -> Result<Option<Vec<Endpoint>>> {
        let cache = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut file = read(&cache.path)?;
            Ok(file
                .records
                .remove(&cache.identity)
                .map(|eps| eps.into_iter().map(Into::into).collect()))
        })
        .await
        .map_err(|e| TarsError::Config(e.to_string()))?
    }
    pub async fn store(&self, endpoints: &[Endpoint]) -> Result<()> {
        let cache = self.clone();
        let records: Vec<Record> = endpoints.iter().map(Record::from).collect();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let path = if cache.path.is_absolute() {
                cache.path
            } else {
                std::env::current_dir()?.join(cache.path)
            };
            let parent = path
                .parent()
                .ok_or_else(|| TarsError::Config("cache has no parent".into()))?;
            std::fs::create_dir_all(parent)?;
            let path = std::fs::canonicalize(parent)?.join(
                path.file_name()
                    .ok_or_else(|| TarsError::Config("invalid cache path".into()))?,
            );
            let lock = {
                let mut locks = LOCKS.lock().unwrap();
                locks.retain(|_, lock| lock.strong_count() > 0);
                if let Some(lock) = locks.get(&path).and_then(Weak::upgrade) {
                    lock
                } else {
                    let lock = Arc::new(Mutex::new(()));
                    locks.insert(path.clone(), Arc::downgrade(&lock));
                    lock
                }
            };
            let _guard = lock.lock().unwrap();
            let mut file = read(&path).unwrap_or(File {
                version: 1,
                records: BTreeMap::new(),
            });
            file.records.insert(cache.identity, records);
            let text = toml::to_string(&file).map_err(|e| TarsError::Config(e.to_string()))?;
            let serial = SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let temporary = path.with_extension(format!("tmp-{}-{serial}", std::process::id()));
            let result = (|| -> Result<()> {
                use std::io::Write;
                let mut output = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary)?;
                output.write_all(text.as_bytes())?;
                output.sync_all()?;
                std::fs::rename(&temporary, &path)?;
                Ok(())
            })();
            if result.is_err() {
                let _ = std::fs::remove_file(&temporary);
            }
            result
        })
        .await
        .map_err(|e| TarsError::Config(e.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cache_merges_concurrent_objects_preserves_metadata_and_rejects_bad_files() {
        let path = std::env::temp_dir().join(format!(
            "tars-cache-unit-{}-{}.tarsdat",
            std::process::id(),
            SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let a = DiscoveryCache::new(path.to_string_lossy().into_owned(), "a".into());
        let b = DiscoveryCache::new(path.to_string_lossy().into_owned(), "b".into());
        let mut ep = Endpoint::tcp("host", 1234);
        ep.weight_type = 1;
        ep.weight = 70;
        ep.grid = 3;
        ep.qos = 4;
        ep.auth_type = 2;
        ep.set_id = "sz.a.1".into();
        let ea = vec![ep.clone()];
        let eb = vec![Endpoint::tcp("other", 4321)];
        let (x, y) = tokio::join!(a.store(&ea), b.store(&eb));
        x.unwrap();
        y.unwrap();
        let saved = a.load().await.unwrap().unwrap().remove(0);
        assert_eq!(
            (
                saved.port,
                saved.grid,
                saved.qos,
                saved.weight,
                saved.weight_type,
                saved.auth_type,
                saved.set_id.as_str()
            ),
            (1234, 3, 4, 70, 1, 2, "sz.a.1")
        );
        assert_eq!(b.load().await.unwrap().unwrap()[0].port, 4321);
        std::fs::write(&path, "broken").unwrap();
        assert!(a.load().await.is_err());
        std::fs::write(&path, "version = 99\n[records]\n").unwrap();
        assert!(a.load().await.is_err());
        std::fs::remove_file(path).unwrap();
    }
}
