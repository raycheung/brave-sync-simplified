package datastore

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	entitiesBucketName  = "entities"
	typeMtimeBucketName = "entities_by_type_mtime"
	tagsBucketName      = "tags"
	countsBucketName    = "counts"
	disabledBucketName  = "disabled_chains"
)

// Bolt is a Datastore implementation backed by an embedded bbolt database
// file, replacing the DynamoDB-backed Dynamo implementation for small,
// single-instance deployments.
type Bolt struct {
	db *bolt.DB
}

// NewBolt opens (creating if necessary) the bbolt database at BOLT_PATH
// (default /data/brave-sync.db) and ensures the required buckets exist.
func NewBolt() (*Bolt, error) {
	path := os.Getenv("BOLT_PATH")
	if path == "" {
		path = "/data/brave-sync.db"
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("error creating bbolt database directory: %w", err)
	}

	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("error opening bbolt database: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{
			entitiesBucketName,
			typeMtimeBucketName,
			tagsBucketName,
			countsBucketName,
			disabledBucketName,
		} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return fmt.Errorf("error creating bucket %s: %w", name, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Bolt{db: db}, nil
}

// entityKey is the primary key for a sync entity: ClientID + ID, matching
// the (ClientID, ID) primary key used by the DynamoDB implementation.
func entityKey(clientID, id string) []byte {
	return []byte(clientID + "\x00" + id)
}

// typeMtimeKey is the secondary-index key used to scan a client's entities
// of a given data type in mtime order, replacing DynamoDB's
// ClientIDDataTypeMtimeIndex GSI. Zero-padded decimal encoding of dataType
// and mtime keeps the byte-lexicographic bbolt key order equal to numeric
// order.
func typeMtimeKey(clientID string, dataType int, mtime int64, id string) []byte {
	return fmt.Appendf(nil, "%s\x00%010d\x00%020d\x00%s", clientID, dataType, mtime, id)
}

// typeMtimePrefix is the shared prefix for all typeMtimeKey entries of a
// given client and data type, used to bound a prefix scan.
func typeMtimePrefix(clientID string, dataType int) []byte {
	return fmt.Appendf(nil, "%s\x00%010d\x00", clientID, dataType)
}

func tagKey(clientID, tagID string) []byte {
	return []byte(clientID + "\x00" + tagID)
}

func clientPrefix(clientID string) []byte {
	return []byte(clientID + "\x00")
}

func encodeGob(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, fmt.Errorf("error gob-encoding value: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeGob(data []byte, v any) error {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil {
		return fmt.Errorf("error gob-decoding value: %w", err)
	}
	return nil
}

// InsertSyncEntity inserts a new sync entity, along with a tag item if the
// entity carries a client-defined unique tag (mirrors Dynamo's behavior of
// writing both in a single transaction to enforce tag uniqueness).
func (b *Bolt) InsertSyncEntity(_ context.Context, entity *SyncEntity) (bool, error) {
	err := b.db.Update(func(tx *bolt.Tx) error {
		entities := tx.Bucket([]byte(entitiesBucketName))
		key := entityKey(entity.ClientID, entity.ID)
		if entities.Get(key) != nil {
			return errConflict
		}

		if entity.ClientDefinedUniqueTag != nil && *entity.DataType != HistoryTypeID {
			tags := tx.Bucket([]byte(tagsBucketName))
			tk := tagKey(entity.ClientID, clientTagItemPrefix+*entity.ClientDefinedUniqueTag)
			if tags.Get(tk) != nil {
				return errConflict
			}
			if err := tags.Put(tk, []byte{1}); err != nil {
				return fmt.Errorf("error writing client tag item: %w", err)
			}
		}

		av, err := encodeGob(entity)
		if err != nil {
			return err
		}
		if err := entities.Put(key, av); err != nil {
			return fmt.Errorf("error writing sync entity: %w", err)
		}

		typeMtime := tx.Bucket([]byte(typeMtimeBucketName))
		if err := typeMtime.Put(
			typeMtimeKey(entity.ClientID, *entity.DataType, *entity.Mtime, entity.ID),
			[]byte(entity.ID),
		); err != nil {
			return fmt.Errorf("error writing type-mtime index entry: %w", err)
		}

		return nil
	})
	if err != nil {
		if err == errConflict { //nolint:errorlint // sentinel comparison is intentional here
			return true, fmt.Errorf("error inserting sync entity: entity or tag already exists")
		}
		return false, fmt.Errorf("error inserting sync entity: %w", err)
	}
	return false, nil
}

// errConflict is a sentinel used internally to signal a conflicting write
// from within a bolt.Tx closure.
var errConflict = fmt.Errorf("conflict")

// InsertSyncEntitiesWithServerTags inserts a batch of sync entities that
// carry server-defined unique tags, writing every tag item and sync item in
// a single bbolt transaction. Callers are expected to have already checked
// tag uniqueness via HasServerDefinedUniqueTag, matching Dynamo's contract.
func (b *Bolt) InsertSyncEntitiesWithServerTags(_ context.Context, entities []*SyncEntity) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		entitiesBucket := tx.Bucket([]byte(entitiesBucketName))
		tags := tx.Bucket([]byte(tagsBucketName))
		typeMtime := tx.Bucket([]byte(typeMtimeBucketName))

		for _, entity := range entities {
			tk := tagKey(entity.ClientID, serverTagItemPrefix+*entity.ServerDefinedUniqueTag)
			if err := tags.Put(tk, []byte{1}); err != nil {
				return fmt.Errorf("error writing server tag item: %w", err)
			}

			av, err := encodeGob(entity)
			if err != nil {
				return err
			}
			if err := entitiesBucket.Put(entityKey(entity.ClientID, entity.ID), av); err != nil {
				return fmt.Errorf("error writing sync entity: %w", err)
			}

			if err := typeMtime.Put(
				typeMtimeKey(entity.ClientID, *entity.DataType, *entity.Mtime, entity.ID),
				[]byte(entity.ID),
			); err != nil {
				return fmt.Errorf("error writing type-mtime index entry: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error inserting sync entities with server tags: %w", err)
	}
	return nil
}

// UpdateSyncEntity updates an existing sync entity, applying only the
// fields the caller populated (nil fields are left unchanged), mirroring
// the partial-update semantics of Dynamo's UpdateItem expression. Returns
// conflict=true (with a nil error) if the entity does not exist or its
// stored version does not match oldVersion, matching the Dynamo contract.
func (b *Bolt) UpdateSyncEntity(
	_ context.Context, entity *SyncEntity, oldVersion int64,
) (conflict bool, deleted bool, err error) {
	txErr := b.db.Update(func(tx *bolt.Tx) error {
		entities := tx.Bucket([]byte(entitiesBucketName))
		key := entityKey(entity.ClientID, entity.ID)
		raw := entities.Get(key)
		if raw == nil {
			conflict = true
			return nil
		}

		oldEntity := &SyncEntity{}
		if err := decodeGob(raw, oldEntity); err != nil {
			return err
		}

		if *entity.DataType != HistoryTypeID {
			if oldEntity.Version == nil || *oldEntity.Version != oldVersion {
				conflict = true
				return nil
			}
		}

		wasDeleted := oldEntity.Deleted != nil && *oldEntity.Deleted
		merged := *oldEntity
		merged.Version = entity.Version
		merged.Mtime = entity.Mtime
		merged.Specifics = entity.Specifics
		merged.DataTypeMtime = entity.DataTypeMtime
		if entity.UniquePosition != nil {
			merged.UniquePosition = entity.UniquePosition
		}
		if entity.ParentID != nil {
			merged.ParentID = entity.ParentID
		}
		if entity.Name != nil {
			merged.Name = entity.Name
		}
		if entity.NonUniqueName != nil {
			merged.NonUniqueName = entity.NonUniqueName
		}
		if entity.Deleted != nil {
			merged.Deleted = entity.Deleted
		}
		if entity.Folder != nil {
			merged.Folder = entity.Folder
		}

		if entity.Deleted == nil {
			deleted = false
		} else {
			deleted = !wasDeleted && *entity.Deleted
		}

		// Soft-deleting an entity with a client tag also removes its tag item,
		// matching Dynamo's transactional delete-tag-on-soft-delete behavior.
		if deleted && oldEntity.ClientDefinedUniqueTag != nil && *entity.DataType != HistoryTypeID {
			tags := tx.Bucket([]byte(tagsBucketName))
			tk := tagKey(entity.ClientID, clientTagItemPrefix+*oldEntity.ClientDefinedUniqueTag)
			if err := tags.Delete(tk); err != nil {
				return fmt.Errorf("error deleting client tag item on soft delete: %w", err)
			}
		}

		typeMtime := tx.Bucket([]byte(typeMtimeBucketName))
		if oldEntity.DataType != nil && oldEntity.Mtime != nil {
			if err := typeMtime.Delete(
				typeMtimeKey(entity.ClientID, *oldEntity.DataType, *oldEntity.Mtime, entity.ID),
			); err != nil {
				return fmt.Errorf("error deleting old type-mtime index entry: %w", err)
			}
		}
		if err := typeMtime.Put(
			typeMtimeKey(entity.ClientID, *merged.DataType, *merged.Mtime, entity.ID),
			[]byte(entity.ID),
		); err != nil {
			return fmt.Errorf("error writing type-mtime index entry: %w", err)
		}

		av, err := encodeGob(&merged)
		if err != nil {
			return err
		}
		if err := entities.Put(key, av); err != nil {
			return fmt.Errorf("error writing updated sync entity: %w", err)
		}

		return nil
	})
	if txErr != nil {
		return false, false, fmt.Errorf("error updating sync entity: %w", txErr)
	}
	return conflict, deleted, nil
}

// GetUpdatesForType returns sync entities of a data type modified after
// clientToken for a client, ordered by mtime, along with whether more
// updates remain beyond maxSize. It scans the typeMtime secondary index
// (a prefix range restricted to this client+dataType), which keeps results
// naturally ordered by mtime without a separate sort pass.
func (b *Bolt) GetUpdatesForType(
	_ context.Context,
	dataType int,
	clientToken int64,
	fetchFolders bool,
	clientID string,
	maxSize int,
) (bool, []SyncEntity, error) {
	syncEntities := []SyncEntity{}
	hasChangesRemaining := false

	err := b.db.View(func(tx *bolt.Tx) error {
		typeMtime := tx.Bucket([]byte(typeMtimeBucketName))
		entities := tx.Bucket([]byte(entitiesBucketName))
		prefix := typeMtimePrefix(clientID, dataType)
		start := typeMtimeKey(clientID, dataType, clientToken+1, "")

		c := typeMtime.Cursor()
		scanned := 0
		for k, v := c.Seek(start); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if scanned >= maxSize {
				hasChangesRemaining = true
				break
			}
			scanned++

			raw := entities.Get(entityKey(clientID, string(v)))
			if raw == nil {
				continue
			}
			var entity SyncEntity
			if err := decodeGob(raw, &entity); err != nil {
				return err
			}

			if !fetchFolders && entity.Folder != nil && *entity.Folder {
				continue
			}
			if entity.ExpirationTime != nil && *entity.ExpirationTime > 0 &&
				*entity.ExpirationTime < time.Now().Unix() {
				continue
			}

			syncEntities = append(syncEntities, entity)
		}
		return nil
	})
	if err != nil {
		return false, syncEntities, fmt.Errorf("error querying updates: %w", err)
	}

	return hasChangesRemaining, syncEntities, nil
}

// HasServerDefinedUniqueTag checks whether a server-defined unique tag item
// already exists for a client.
func (b *Bolt) HasServerDefinedUniqueTag(_ context.Context, clientID string, tag string) (bool, error) {
	var exists bool
	err := b.db.View(func(tx *bolt.Tx) error {
		tags := tx.Bucket([]byte(tagsBucketName))
		exists = tags.Get(tagKey(clientID, serverTagItemPrefix+tag)) != nil
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("error checking server tag: %w", err)
	}
	return exists, nil
}

// HasItem checks whether a sync item exists for a client.
func (b *Bolt) HasItem(_ context.Context, clientID string, id string) (bool, error) {
	var exists bool
	err := b.db.View(func(tx *bolt.Tx) error {
		entities := tx.Bucket([]byte(entitiesBucketName))
		exists = entities.Get(entityKey(clientID, id)) != nil
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("error checking item existence: %w", err)
	}
	return exists, nil
}

// GetClientItemCount returns the count of non-deleted sync items stored for
// a client, initializing a zero-value record on first access. Unlike
// Dynamo's implementation, there is no legacy DynamoDB count record to
// migrate from, so only the period-rollover bookkeeping is replicated.
func (b *Bolt) GetClientItemCount(_ context.Context, clientID string) (*ClientItemCounts, error) {
	counts := &ClientItemCounts{}
	err := b.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte(countsBucketName)).Get([]byte(clientID))
		if raw == nil {
			return nil
		}
		return decodeGob(raw, counts)
	})
	if err != nil {
		return nil, fmt.Errorf("error getting client item count: %w", err)
	}

	if len(counts.ClientID) == 0 {
		counts.ClientID = clientID
		counts.ID = clientID
	}

	now := time.Now().Unix()
	if counts.Version < CurrentCountVersion {
		counts.LastPeriodChangeTime = now
		counts.Version = CurrentCountVersion
	} else {
		timeSinceLastChange := now - counts.LastPeriodChangeTime
		if timeSinceLastChange >= periodDurationSecs {
			changeCount := int(timeSinceLastChange / periodDurationSecs)
			for range changeCount {
				counts.HistoryItemCountPeriod1 = counts.HistoryItemCountPeriod2
				counts.HistoryItemCountPeriod2 = counts.HistoryItemCountPeriod3
				counts.HistoryItemCountPeriod3 = counts.HistoryItemCountPeriod4
				counts.HistoryItemCountPeriod4 = 0
			}
			counts.LastPeriodChangeTime += periodDurationSecs * int64(changeCount)
		}
	}

	return counts, nil
}

// UpdateClientItemCount persists the count of non-deleted sync items for a
// client.
func (b *Bolt) UpdateClientItemCount(
	_ context.Context,
	counts *ClientItemCounts,
	newNormalItemCount int,
	newHistoryItemCount int,
) error {
	counts.HistoryItemCountPeriod4 += newHistoryItemCount
	counts.ItemCount += newNormalItemCount

	av, err := encodeGob(counts)
	if err != nil {
		return err
	}

	err = b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(countsBucketName)).Put([]byte(counts.ClientID), av)
	})
	if err != nil {
		return fmt.Errorf("error updating client item count: %w", err)
	}
	return nil
}

// ClearServerData deletes all sync entities, tag items, and the item-count
// record for a client, mirroring Dynamo's behavior of deleting every item
// sharing the client's partition key except the disabled-chain marker
// (which in this schema lives in a separate bucket and is left untouched).
func (b *Bolt) ClearServerData(_ context.Context, clientID string) ([]SyncEntity, error) {
	var deleted []SyncEntity

	err := b.db.Update(func(tx *bolt.Tx) error {
		entities := tx.Bucket([]byte(entitiesBucketName))
		typeMtime := tx.Bucket([]byte(typeMtimeBucketName))
		tags := tx.Bucket([]byte(tagsBucketName))
		counts := tx.Bucket([]byte(countsBucketName))

		prefix := clientPrefix(clientID)

		ec := entities.Cursor()
		var keysToDelete [][]byte
		for k, v := ec.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = ec.Next() {
			var entity SyncEntity
			if err := decodeGob(v, &entity); err != nil {
				return err
			}
			deleted = append(deleted, entity)
			keysToDelete = append(keysToDelete, append([]byte{}, k...))

			if entity.DataType != nil && entity.Mtime != nil {
				if err := typeMtime.Delete(
					typeMtimeKey(clientID, *entity.DataType, *entity.Mtime, entity.ID),
				); err != nil {
					return fmt.Errorf("error deleting type-mtime index entry: %w", err)
				}
			}
		}
		for _, k := range keysToDelete {
			if err := entities.Delete(k); err != nil {
				return fmt.Errorf("error deleting sync entity: %w", err)
			}
		}

		tc := tags.Cursor()
		var tagKeysToDelete [][]byte
		for k, _ := tc.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = tc.Next() {
			tagKeysToDelete = append(tagKeysToDelete, append([]byte{}, k...))
		}
		for _, k := range tagKeysToDelete {
			if err := tags.Delete(k); err != nil {
				return fmt.Errorf("error deleting tag item: %w", err)
			}
		}

		if err := counts.Delete([]byte(clientID)); err != nil {
			return fmt.Errorf("error deleting item-count record: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error clearing server data: %w", err)
	}

	return deleted, nil
}

// DisableSyncChain marks a chain as disabled so no further updates or
// commits can happen.
func (b *Bolt) DisableSyncChain(_ context.Context, clientID string) error {
	err := b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(disabledBucketName)).Put([]byte(clientID), []byte{1})
	})
	if err != nil {
		return fmt.Errorf("error disabling sync chain: %w", err)
	}
	return nil
}

// IsSyncChainDisabled checks whether a given sync chain has been disabled.
func (b *Bolt) IsSyncChainDisabled(_ context.Context, clientID string) (bool, error) {
	var disabled bool
	err := b.db.View(func(tx *bolt.Tx) error {
		disabled = tx.Bucket([]byte(disabledBucketName)).Get([]byte(clientID)) != nil
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("error checking sync chain disabled state: %w", err)
	}
	return disabled, nil
}
