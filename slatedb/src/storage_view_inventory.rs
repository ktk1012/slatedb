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

/// Latest root manifest revision observed while resolving a checkpoint.
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
    pub observed_revisions: Vec<StorageViewRevision>,
}

/// Failure while resolving an exact checkpoint-backed storage view.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageViewInventoryError {
    /// The root latest manifest changed while the inventory was being captured.
    #[error(
        "storage view revision changed. db_path=`{db_path}`, expected=`{expected}`, observed=`{observed}`"
    )]
    RevisionChanged {
        db_path: String,
        expected: u64,
        observed: u64,
    },
    /// The explicit root checkpoint no longer exists.
    #[error("checkpoint missing. db_path=`{db_path}`, checkpoint_id=`{checkpoint_id}`")]
    CheckpointMissing {
        db_path: String,
        checkpoint_id: Uuid,
    },
    /// Persisted clone metadata cannot describe one unambiguous storage view.
    #[error("invalid storage view graph: {reason}")]
    InvalidGraph { reason: &'static str },
    /// Any other SlateDB storage or data error.
    #[error(transparent)]
    SlateDb(#[from] crate::Error),
}

impl From<SlateDBError> for StorageViewInventoryError {
    fn from(error: SlateDBError) -> Self {
        Self::SlateDb(error.into())
    }
}

impl Admin {
    /// Resolve an explicit checkpoint to its exact manifest, bounded WAL, local SST,
    /// and external SST physical files.
    ///
    /// The method rejects a capture if the root latest manifest revision changes
    /// while the files are being resolved. Callers may retry from the beginning.
    pub async fn read_storage_view_inventory(
        &self,
        checkpoint_id: Uuid,
    ) -> Result<StorageViewInventory, StorageViewInventoryError> {
        let main_store = self.object_stores.store_of(ObjectStoreType::Main).clone();
        let root_store = ManifestStore::new(&self.path, main_store.clone());
        let root_latest = root_store.read_latest_manifest().await?;
        let checkpoint = find_checkpoint(root_latest.checkpoints(), checkpoint_id, &self.path)?;
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

        let mut observed_revisions = BTreeMap::new();
        observed_revisions.insert(self.path.to_string(), root_latest.id());

        verify_revisions(&self.path, &self.object_stores, &observed_revisions).await?;

        Ok(StorageViewInventory {
            checkpoint_id,
            checkpoint_manifest_id: checkpoint.manifest_id,
            objects: objects.into_iter().collect(),
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

fn find_checkpoint<'a>(
    checkpoints: &'a [Checkpoint],
    checkpoint_id: Uuid,
    db_path: &Path,
) -> Result<&'a Checkpoint, StorageViewInventoryError> {
    checkpoints
        .iter()
        .find(|checkpoint| checkpoint.id == checkpoint_id)
        .ok_or_else(|| StorageViewInventoryError::CheckpointMissing {
            db_path: db_path.to_string(),
            checkpoint_id,
        })
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
) -> Result<HashMap<SsTableId, Path>, StorageViewInventoryError> {
    let mut paths = HashMap::new();
    for external_db in &manifest.external_dbs {
        for sst_id in &external_db.sst_ids {
            if paths
                .insert(*sst_id, Path::from(external_db.path.clone()))
                .is_some()
            {
                return Err(StorageViewInventoryError::InvalidGraph {
                    reason: "duplicate external SST identity",
                });
            }
        }
    }
    Ok(paths)
}

async fn verify_revisions(
    root_path: &Path,
    object_stores: &ObjectStores,
    expected: &BTreeMap<String, u64>,
) -> Result<(), StorageViewInventoryError> {
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
            return Err(StorageViewInventoryError::RevisionChanged {
                db_path: db_path.clone(),
                expected: *expected_id,
                observed: observed.id(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CheckpointOptions;
    use crate::db::builder::AdminBuilder;
    use crate::db_state::{SortedRun, SsTableHandle, SsTableInfo, SsTableView};
    use crate::format::sst::SST_FORMAT_VERSION_LATEST;
    use crate::manifest::store::StoredManifest;
    use crate::manifest::{ExternalDb, Manifest, ManifestCore};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use slatedb_common::clock::DefaultSystemClock;
    use std::sync::Arc;
    use ulid::Ulid;

    #[test]
    fn invalid_graph_is_not_classified_as_revision_change() {
        let sst_id = SsTableId::Compacted(Ulid::from_parts(1, 0));
        let mut manifest = Manifest::initial(ManifestCore::new());
        manifest.external_dbs = vec![
            ExternalDb {
                path: "source-a".into(),
                source_checkpoint_id: Uuid::from_u128(1),
                final_checkpoint_id: Some(Uuid::from_u128(2)),
                sst_ids: vec![sst_id],
            },
            ExternalDb {
                path: "source-b".into(),
                source_checkpoint_id: Uuid::from_u128(3),
                final_checkpoint_id: Some(Uuid::from_u128(4)),
                sst_ids: vec![sst_id],
            },
        ];

        assert!(matches!(
            external_sst_paths(&manifest),
            Err(StorageViewInventoryError::InvalidGraph { .. })
        ));
    }

    #[test]
    fn missing_checkpoint_preserves_identity_without_retry_classification() {
        let checkpoint_id = Uuid::from_u128(1);
        assert!(matches!(
            find_checkpoint(&[], checkpoint_id, &Path::from("root")),
            Err(StorageViewInventoryError::CheckpointMissing {
                db_path,
                checkpoint_id: observed,
            }) if db_path == "root" && observed == checkpoint_id
        ));
    }

    #[tokio::test]
    async fn external_ssts_do_not_require_source_manifest_or_final_checkpoint() {
        let root = Path::from("root");
        let source = Path::from("missing-source");
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_store = Arc::new(ManifestStore::new(&root, store.clone()));
        let external_sst_id = SsTableId::Compacted(Ulid::from_parts(1, 0));
        let external_view = SsTableView::new_projected(
            external_sst_id.unwrap_compacted_id(),
            SsTableHandle::new(
                external_sst_id,
                SST_FORMAT_VERSION_LATEST,
                SsTableInfo {
                    first_entry: Some(Bytes::from_static(b"key")),
                    ..SsTableInfo::default()
                },
            ),
            None,
        );
        let mut core = ManifestCore::new();
        Arc::make_mut(&mut core.tree).compacted.push(SortedRun {
            id: 1,
            sst_views: vec![external_view],
        });
        let mut manifest = Manifest::initial(core);
        manifest.external_dbs.push(ExternalDb {
            path: source.to_string(),
            source_checkpoint_id: Uuid::from_u128(1),
            final_checkpoint_id: None,
            sst_ids: vec![external_sst_id],
        });
        let mut stored = StoredManifest::store_uninitialized_clone(
            manifest_store,
            manifest,
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();
        let checkpoint = stored
            .write_checkpoint(Uuid::from_u128(2), &CheckpointOptions::default())
            .await
            .unwrap();
        let admin = AdminBuilder::new(root.clone(), store).build();

        let inventory = admin
            .read_storage_view_inventory(checkpoint.id)
            .await
            .unwrap();

        assert!(inventory
            .objects
            .contains(&manifest_object(&root, checkpoint.manifest_id)));
        assert!(inventory.objects.contains(&StorageViewObject {
            store: StorageViewObjectStore::Main,
            kind: StorageViewObjectKind::SortedRun,
            path: PathResolver::new(source.clone())
                .table_path(&external_sst_id)
                .to_string(),
        }));
        assert!(!inventory.objects.iter().any(|object| {
            object.kind == StorageViewObjectKind::Manifest
                && object.path.starts_with(source.as_ref())
        }));
        assert_eq!(
            inventory.observed_revisions,
            vec![StorageViewRevision {
                db_path: root.to_string(),
                manifest_id: 2,
            }]
        );
    }

    #[tokio::test]
    async fn revision_mismatch_is_the_only_typed_retry_signal() {
        let root = Path::from("root");
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_store = Arc::new(ManifestStore::new(&root, store.clone()));
        StoredManifest::create_new_db(
            manifest_store,
            ManifestCore::new(),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();
        let stores = ObjectStores::new(store, None);

        assert!(matches!(
            verify_revisions(&root, &stores, &BTreeMap::from([("root".into(), 0)])).await,
            Err(StorageViewInventoryError::RevisionChanged {
                db_path,
                expected: 0,
                observed: 1,
            }) if db_path == "root"
        ));
    }
}
