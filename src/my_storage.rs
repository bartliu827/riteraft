use crate::error::Result;
use crate::Error;
use std::cmp;

use heed::types::*;
use heed::{Database, Env, PolyDatabase};
use heed_traits::{BytesDecode, BytesEncode};
use log::{info, warn};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use prost::Message;
use raft::{prelude::*, GetEntriesContext};

use std::borrow::Cow;
use std::fs;
use std::path::Path;
use std::sync::Arc;

const SNAPSHOT_KEY: &str = "snapshot";
const LAST_INDEX_KEY: &str = "last_index";
const HARD_STATE_KEY: &str = "hard_state";
const CONF_STATE_KEY: &str = "conf_state";
const SNAP_META_KEY: &str = "snap_meta";

macro_rules! heed_type {
    ($heed_type:ident, $type:ty) => {
        struct $heed_type;

        impl<'a> BytesEncode<'a> for $heed_type {
            type EItem = $type;
            fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
                let mut bytes = vec![];
                prost::Message::encode(item, &mut bytes).ok()?;
                Some(Cow::Owned(bytes))
            }
        }

        impl<'a> BytesDecode<'a> for $heed_type {
            type DItem = $type;
            fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
                prost::Message::decode(bytes).ok()
            }
        }
    };
}

heed_type!(HeedSnapshot, Snapshot);
heed_type!(HeedEntry, Entry);
heed_type!(HeedHardState, HardState);
heed_type!(HeedConfState, ConfState);
heed_type!(HeedSnapshotMetadata, SnapshotMetadata);

/// The Memory Storage Core instance holds the actual state of the storage struct. To access this
/// value, use the `rl` and `wl` functions on the main MemStorage implementation.
//#[derive(Default)]
pub struct MemStorageCore {
    // raft_state: RaftState,
    // // entries[i] has raft log position i+snapshot.get_metadata().index
    // entries: Vec<Entry>,
    // // Metadata of the last snapshot received.
    // snapshot_metadata: SnapshotMetadata,
    // // If it is true, the next snapshot will return a
    // // SnapshotTemporarilyUnavailable error.
    // trigger_snap_unavailable: bool,
    // // Peers that are fetching entries asynchronously.
    // trigger_log_unavailable: bool,
    // // Stores get entries context.
    // get_entries_context: Option<GetEntriesContext>,
    env: Env,
    entries_db: Database<OwnedType<u64>, HeedEntry>,
    metadata_db: PolyDatabase,
}

impl MemStorageCore {
    pub fn create(path: impl AsRef<Path>, id: u64) -> Result<Self> {
        let path = path.as_ref();
        let name = format!("raft-{}.mdb", id);

        info!(
            "-------------------- create_dir_all {:?}",
            Path::new(&path).join(&name)
        );

        fs::create_dir_all(Path::new(&path).join(&name))?;

        let path = path.join(&name);

        let env = heed::EnvOpenOptions::new()
            .map_size(100 * 4096)
            .max_dbs(3000)
            .open(path)?;
        let entries_db: Database<OwnedType<u64>, HeedEntry> =
            env.create_database(Some("entries"))?;

        let metadata_db = env.create_poly_database(Some("meta"))?;

        let hard_state = HardState::default();
        let conf_state = ConfState::default();
        let snap_meta = SnapshotMetadata::default();

        let storage = Self {
            metadata_db,
            entries_db,
            env,
        };

        let mut writer = storage.env.write_txn()?;
        storage.set_hard_state_i(&mut writer, &hard_state)?;
        storage.set_conf_state_i(&mut writer, &conf_state)?;
        storage.set_snap_meta_i(&mut writer, &snap_meta)?;
        //storage.append_i(&mut writer, &[Entry::default()])?;
        writer.commit()?;

        Ok(storage)
    }

    pub fn set_hard_state_i(&self, writer: &mut heed::RwTxn, hard_state: &HardState) -> Result<()> {
        info!("------------------set_hard_state_i {:?}", hard_state);
        self.metadata_db
            .put::<_, Str, HeedHardState>(writer, HARD_STATE_KEY, hard_state)?;
        Ok(())
    }

    pub fn hard_state_i(&self, reader: &heed::RoTxn) -> Result<HardState> {
        let hard_state = self
            .metadata_db
            .get::<_, Str, HeedHardState>(reader, HARD_STATE_KEY)?;
        Ok(hard_state.expect("missing hard_state"))
    }

    pub fn set_conf_state_i(&self, writer: &mut heed::RwTxn, conf_state: &ConfState) -> Result<()> {
        info!("------------------set_conf_state_i {:?}", conf_state);
        self.metadata_db
            .put::<_, Str, HeedConfState>(writer, CONF_STATE_KEY, conf_state)?;
        Ok(())
    }

    pub fn conf_state_i(&self, reader: &heed::RoTxn) -> Result<ConfState> {
        let conf_state = self
            .metadata_db
            .get::<_, Str, HeedConfState>(reader, CONF_STATE_KEY)?;
        Ok(conf_state.expect("there should be a conf state"))
    }

    pub fn set_snap_meta_i(
        &self,
        writer: &mut heed::RwTxn,
        snap_meta: &SnapshotMetadata,
    ) -> Result<()> {
        self.metadata_db
            .put::<_, Str, HeedSnapshotMetadata>(writer, SNAP_META_KEY, snap_meta)?;
        Ok(())
    }

    pub fn snap_meta_i(&self, reader: &heed::RoTxn) -> Result<SnapshotMetadata> {
        if let Some(snap_meta) = self
            .metadata_db
            .get::<_, Str, HeedSnapshotMetadata>(reader, SNAP_META_KEY)?
        {
            Ok(snap_meta)
        } else {
            Ok(SnapshotMetadata::default())
        }
    }

    fn set_snapshot_i(&self, writer: &mut heed::RwTxn, snapshot: &Snapshot) -> Result<()> {
        self.metadata_db
            .put::<_, Str, HeedSnapshot>(writer, SNAPSHOT_KEY, snapshot)?;
        Ok(())
    }

    pub fn snapshot_i(&self, reader: &heed::RoTxn) -> Result<Option<Snapshot>> {
        let snapshot = self
            .metadata_db
            .get::<_, Str, HeedSnapshot>(&reader, SNAPSHOT_KEY)?;
        Ok(snapshot)
    }

    fn first_index_i(&self, r: &heed::RoTxn) -> Result<u64> {
        let first_entry = self.entries_db.first(r)?;

        match first_entry {
            Some(e) => Ok(e.1.index),
            None => Ok(self.snap_meta_i(r).unwrap().index + 1),
        }
    }

    fn last_index_i(&self, r: &heed::RoTxn) -> Result<u64> {
        let first_entry = self.entries_db.last(r)?;

        match first_entry {
            Some(e) => Ok(e.1.index),
            None => Ok(self.snap_meta_i(r).unwrap().index + 1),
        }
    }

    fn append_i(&self, writer: &mut heed::RwTxn, ents: &[Entry]) -> Result<()> {
        info!("-------------- append : {:?}", ents);

        if ents.is_empty() {
            return Ok(());
        }

        if self.last_index_i(writer).unwrap() + 1 < ents[0].index {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                self.last_index(),
                ents[0].index,
            );
        }

        // Remove all entries overwritten by `ents`.
        // let diff = ents[0].index - self.first_index();
        // self.entries.drain(diff as usize..);
        // self.entries.extend_from_slice(ents);

        // let begin = ents[0].index;
        // self.entries_db.delete_range(writer, &(begin..))?;

        for entry in ents {
            let index = entry.index;
            self.entries_db.put(writer, &index, entry)?;
        }
        Ok(())
    }

    /// Saves the current HardState.
    pub fn set_hardstate(&mut self, hs: HardState) {
        let mut writer = self.env.write_txn().unwrap();
        self.set_hard_state_i(&mut writer, &hs).unwrap();
        writer.commit().unwrap();
    }

    /// Saves the current conf state.
    pub fn set_conf_state(&mut self, cs: ConfState) {
        let mut writer = self.env.write_txn().unwrap();
        self.set_conf_state_i(&mut writer, &cs).unwrap();
        writer.commit().unwrap();
    }

    fn first_index(&self) -> u64 {
        let mut reader = self.env.read_txn().unwrap();
        let idx = self.first_index_i(&mut reader).unwrap();

        idx
    }

    fn last_index(&self) -> u64 {
        let mut reader = self.env.read_txn().unwrap();
        let idx = self.last_index_i(&mut reader).unwrap();

        idx
    }

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the snapshot index is less than the storage's first index.
    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<()> {
        let mut meta = snapshot.get_metadata();
        let index = meta.index;

        if self.first_index() > index {
            return Err(Error::Common(format!(
                "apply_snapshot first index={} > index={}",
                self.first_index(),
                index
            )));
        }
        let mut writer = self.env.write_txn().unwrap();
        self.set_snap_meta_i(&mut writer, meta).unwrap();

        let mut hs = self.hard_state_i(&writer).unwrap();
        hs.term = cmp::max(hs.term, meta.term);
        hs.commit = index;

        self.set_hard_state_i(&mut writer, &hs).unwrap();
        self.set_conf_state_i(&mut writer, meta.get_conf_state())
            .unwrap();
        self.set_snapshot_i(&mut writer, &snapshot).unwrap();

        self.entries_db.clear(&mut writer).unwrap();
        writer.commit();

        info!("--------------apply snapshot={:?}", snapshot);

        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        let reader = self.env.read_txn().unwrap();
        let mut snapshot = if let Some(snap) = self.snapshot_i(&reader).unwrap() {
            snap
        } else {
            Snapshot::default()
        };

        //let mut snapshot = Snapshot::default();

        // We assume all entries whose indexes are less than `hard_state.commit`
        // have been applied, so use the latest commit index to construct the snapshot.
        // TODO: This is not true for async ready.
        let meta = snapshot.mut_metadata();
        let hard_state = self.hard_state_i(&reader).unwrap();
        let conf_state = self.conf_state_i(&reader).unwrap();
        let snapshot_metadata = self.snap_meta_i(&reader).unwrap();
        meta.index = hard_state.commit;
        meta.term = match meta.index.cmp(&snapshot_metadata.index) {
            cmp::Ordering::Equal => snapshot_metadata.term,
            cmp::Ordering::Greater => self.entry(&reader, meta.index).unwrap().unwrap().term,
            cmp::Ordering::Less => {
                panic!(
                    "commit {} < snapshot_metadata.index {}",
                    meta.index, snapshot_metadata.index
                );
            }
        };

        meta.set_conf_state(conf_state.clone());

        info!("--------------snapshot={:?}", snapshot);
        snapshot
    }

    /// Discards all log entries prior to compact_index.
    /// It is the application's responsibility to not attempt to compact an index
    /// greater than RaftLog.applied.
    ///
    /// # Panics
    ///
    /// Panics if `compact_index` is higher than `Storage::last_index(&self) + 1`.
    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        if compact_index <= self.first_index() {
            // Don't need to treat this case as an error.
            return Ok(());
        }

        if compact_index > self.last_index() + 1 {
            panic!(
                "compact not received raft logs: {}, last index: {}",
                compact_index,
                self.last_index()
            );
        }

        let mut writer = self.env.write_txn()?;
        self.entries_db
            .delete_range(&mut writer, &(..compact_index))?;
        writer.commit()?;
        Ok(())
    }

    /// Append the new entries to storage.
    ///
    /// # Panics
    ///
    /// Panics if `ents` contains compacted entries, or there's a gap between `ents` and the last
    /// received entry in the storage.
    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        let mut writer = self.env.write_txn()?;
        self.append_i(&mut writer, ents);
        writer.commit()?;

        Ok(())
    }

    fn entry(&self, reader: &heed::RoTxn, index: u64) -> Result<Option<Entry>> {
        let entry = self.entries_db.get(reader, &index)?;
        Ok(entry)
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        info!("entries requested: {}->{}", low, high);

        let reader = self.env.read_txn()?;
        let iter = self.entries_db.range(&reader, &(low..high))?;
        let max_size: Option<u64> = max_size.into();
        let mut size_count = 0;
        let mut buf = vec![];
        let entries = iter
            .filter_map(|e| match e {
                Ok((_, e)) => Some(e),
                _ => None,
            })
            .take_while(|entry| match max_size {
                Some(max_size) => {
                    entry.encode(&mut buf).unwrap();
                    size_count += buf.len() as u64;
                    buf.clear();
                    size_count < max_size
                }
                None => true,
            })
            .collect();
        Ok(entries)
    }

    pub fn set_commit(&mut self, index: u64) -> Result<()> {
        let mut writer = self.env.write_txn()?;
        let mut hard_state = self.hard_state_i(&writer)?;
        hard_state.set_commit(index);
        self.set_hard_state_i(&mut writer, &hard_state)?;
        writer.commit()?;

        Ok(())
    }
}

/// `MemStorage` is a thread-safe but incomplete implementation of `Storage`, mainly for tests.
///
/// A real `Storage` should save both raft logs and applied data. However `MemStorage` only
/// contains raft logs. So you can call `MemStorage::append` to persist new received unstable raft
/// logs and then access them with `Storage` APIs. The only exception is `Storage::snapshot`. There
/// is no data in `Snapshot` returned by `MemStorage::snapshot` because applied data is not stored
/// in `MemStorage`.
#[derive(Clone)]
pub struct MemStorage {
    core: Arc<RwLock<MemStorageCore>>,
}

impl MemStorage {
    pub fn create(path: impl AsRef<Path>, id: u64) -> Result<Self> {
        let core = MemStorageCore::create(path, id)?;
        Ok(Self {
            core: Arc::new(RwLock::new(core)),
        })
    }
    /// Opens up a read lock on the storage and returns a guard handle. Use this
    /// with functions that don't require mutation.
    pub fn rl(&self) -> RwLockReadGuard<'_, MemStorageCore> {
        self.core.read()
    }

    /// Opens up a write lock on the storage and returns guard handle. Use this
    /// with functions that take a mutable reference to self.
    pub fn wl(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
        self.core.write()
    }
}

impl Storage for MemStorage {
    /// Implements the Storage trait.
    fn initial_state(&self) -> raft::Result<RaftState> {
        let store = self.rl();
        let reader = store
            .env
            .read_txn()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;
        let raft_state = RaftState {
            hard_state: store
                .hard_state_i(&reader)
                .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?,
            conf_state: store
                .conf_state_i(&reader)
                .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?,
        };
        warn!("raft_state: {:#?}", raft_state);
        Ok(raft_state)
    }

    /// Implements the Storage trait.
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let store = self.rl();
        let entries = store
            .entries(low, high, max_size)
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(e.into())))?;
        Ok(entries)
    }

    /// Implements the Storage trait.
    fn term(&self, idx: u64) -> raft::Result<u64> {
        let store = self.rl();
        let reader = store
            .env
            .read_txn()
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let first_index = store
            .first_index_i(&reader)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let last_index = store
            .last_index_i(&reader)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        let hard_state = store
            .hard_state_i(&reader)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        if idx == hard_state.commit {
            return Ok(hard_state.term);
        }

        if idx < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        if idx > last_index {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let entry = store
            .entry(&reader, idx)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        Ok(entry.map(|e| e.term).unwrap_or(0))
    }

    /// Implements the Storage trait.
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.rl().first_index())
    }

    /// Implements the Storage trait.
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.rl().last_index())
    }

    /// Implements the Storage trait.
    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let mut core = self.wl();
        let mut snap = core.snapshot();
        if snap.get_metadata().index < request_index {
            snap.mut_metadata().index = request_index;
        }
        Ok(snap)
    }
}
