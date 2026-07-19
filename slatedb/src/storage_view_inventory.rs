//! Read-only physical inventory for a checkpoint-backed storage view.
//!
//! This module keeps SlateDB object-key encoding inside SlateDB. Callers can
//! freeze and retain the returned paths without duplicating private layout rules.

use crate::admin::Admin;
use crate::checkpoint::Checkpoint;
use crate::db_state::SsTableId;
use crate::error::SlateDBError;
use crate::manifest::store::ManifestStore;
use crate::object_stores::{ObjectStoreType, ObjectStores};
use crate::paths::PathResolver;
use object_store::path::Path;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

/// Object store containing a referenced object.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StorageViewObjectStore {
    Main,
    Wal,
}

/// Role an object has in the checkpoint-backed read view.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StorageViewObjectKind {
    Manifest,
    Wal,
    L0,
    SortedRun,
}

/// Exact physical object referenced by a storage view.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StorageViewObject {
    pub store: StorageViewObjectStore,
    pub kind: StorageViewObjectKind,
    pub path: String,
}

/// Semantic pin that keeps an external SST source manifest reachable.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExternalCheckpointPin {
    pub source_db_path: String,
    pub source_checkpoint_id: Uuid,
    pub final_checkpoint_id: Uuid,
    pub manifest_id: u64,
}

/// Latest manifest revision observed while resolving a checkpoint or external pin.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StorageViewRevision {
    pub db_path: String,
    pub manifest_id: u64,
}

/// Read-only, exact object graph for one explicit checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageViewInventory {
    pub checkpoint_id: Uuid,
    pub checkpoint_manifest_id: u64,
    pub objects: Vec<StorageViewObject>,
    pub external_checkpoint_pins: Vec<ExternalCheckpointPin>,
    pub observed_revisions: Vec<StorageViewRevision>,
}

impl Admin {
    /// Resolve an explicit checkpoint to its exact manifest, bounded WAL, local SST,
    /// external SST, and external final-checkpoint pin graph.
    ///
    /// The method rejects a capture if any latest manifest revision changes while
    /// the graph is being read. Callers may retry from the beginning.
    pub async fn read_storage_view_inventory(
        &self,
        checkpoint_id: Uuid,
    ) -> Result<StorageViewInventory, crate::Error> {
        let main_store = self.object_stores.store_of(ObjectStoreType::Main).clone();
        let root_store = ManifestStore::new(&self.path, main_store.clone());
        let root_latest = root_store.read_latest_manifest().await?;
        let checkpoint = find_checkpoint(root_latest.checkpoints(), checkpoint_id)?;
        let checkpoint_manifest = root_store.read_manifest(checkpoint.manifest_id).await?;

        let mut objects = BTreeSet::new();
        objects.insert(manifest_object(&self.path, checkpoint.manifest_id));

        let local_resolver = PathResolver::new(self.path.clone());
        for wal_id in (checkpoint_manifest.core.replay_after_wal_id + 1)
            ..checkpoint_manifest.core.next_wal_sst_id
        {
            objects.insert(StorageViewObject {
                store: StorageViewObjectStore::Wal,
                kind: StorageViewObjectKind::Wal,
                path: local_resolver
                    .table_path(&SsTableId::Wal(wal_id))
                    .to_string(),
            });
        }

        let external_ssts = external_sst_paths(&checkpoint_manifest)?;
        let resolver = PathResolver::new_with_external_ssts(self.path.clone(), external_ssts);
        for tree in checkpoint_manifest.core.trees() {
            for view in &tree.l0 {
                objects.insert(StorageViewObject {
                    store: StorageViewObjectStore::Main,
                    kind: StorageViewObjectKind::L0,
                    path: resolver.table_path(&view.sst.id).to_string(),
                });
            }
            for run in &tree.compacted {
                for view in &run.sst_views {
                    objects.insert(StorageViewObject {
                        store: StorageViewObjectStore::Main,
                        kind: StorageViewObjectKind::SortedRun,
                        path: resolver.table_path(&view.sst.id).to_string(),
                    });
                }
            }
        }

        let mut external_checkpoint_pins = BTreeSet::new();
        let mut observed_revisions = BTreeMap::new();
        observed_revisions.insert(self.path.to_string(), root_latest.id());

        for external_db in checkpoint_manifest
            .external_dbs
            .iter()
            .filter(|external_db| !external_db.sst_ids.is_empty())
        {
            let final_checkpoint_id = external_db
                .final_checkpoint_id
                .ok_or(SlateDBError::InvalidDBState)?;
            let source_path = Path::from(external_db.path.clone());
            let source_store = ManifestStore::new(&source_path, main_store.clone());
            let source_latest = source_store.read_latest_manifest().await?;
            let final_checkpoint =
                find_checkpoint(source_latest.checkpoints(), final_checkpoint_id)?;

            objects.insert(manifest_object(&source_path, final_checkpoint.manifest_id));
            external_checkpoint_pins.insert(ExternalCheckpointPin {
                source_db_path: external_db.path.clone(),
                source_checkpoint_id: external_db.source_checkpoint_id,
                final_checkpoint_id,
                manifest_id: final_checkpoint.manifest_id,
            });
            observed_revisions.insert(external_db.path.clone(), source_latest.id());
        }

        verify_revisions(&self.path, &self.object_stores, &observed_revisions).await?;

        Ok(StorageViewInventory {
            checkpoint_id,
            checkpoint_manifest_id: checkpoint.manifest_id,
            objects: objects.into_iter().collect(),
            external_checkpoint_pins: external_checkpoint_pins.into_iter().collect(),
            observed_revisions: observed_revisions
                .into_iter()
                .map(|(db_path, manifest_id)| StorageViewRevision {
                    db_path,
                    manifest_id,
                })
                .collect(),
        })
    }
}

fn find_checkpoint(
    checkpoints: &[Checkpoint],
    checkpoint_id: Uuid,
) -> Result<&Checkpoint, SlateDBError> {
    checkpoints
        .iter()
        .find(|checkpoint| checkpoint.id == checkpoint_id)
        .ok_or(SlateDBError::CheckpointMissing(checkpoint_id))
}

fn manifest_object(path: &Path, manifest_id: u64) -> StorageViewObject {
    StorageViewObject {
        store: StorageViewObjectStore::Main,
        kind: StorageViewObjectKind::Manifest,
        path: path
            .clone()
            .join("manifest")
            .join(format!("{manifest_id:020}.manifest"))
            .to_string(),
    }
}

fn external_sst_paths(
    manifest: &crate::manifest::Manifest,
) -> Result<HashMap<SsTableId, Path>, SlateDBError> {
    let mut paths = HashMap::new();
    for external_db in &manifest.external_dbs {
        for sst_id in &external_db.sst_ids {
            if paths
                .insert(*sst_id, Path::from(external_db.path.clone()))
                .is_some()
            {
                return Err(SlateDBError::InvalidDBState);
            }
        }
    }
    Ok(paths)
}

async fn verify_revisions(
    root_path: &Path,
    object_stores: &ObjectStores,
    expected: &BTreeMap<String, u64>,
) -> Result<(), SlateDBError> {
    let main_store = object_stores.store_of(ObjectStoreType::Main).clone();
    for (db_path, expected_id) in expected {
        let path = if db_path == &root_path.to_string() {
            root_path.clone()
        } else {
            Path::from(db_path.clone())
        };
        let observed = ManifestStore::new(&path, main_store.clone())
            .read_latest_manifest()
            .await?;
        if observed.id() != *expected_id {
            return Err(SlateDBError::InvalidDBState);
        }
    }
    Ok(())
}
