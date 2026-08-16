package datastore_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/suite"

	"github.com/brave/go-sync/datastore"
)

type BoltTestSuite struct {
	suite.Suite

	db *datastore.Bolt
}

func (suite *BoltTestSuite) SetupTest() {
	suite.T().Setenv("BOLT_PATH", filepath.Join(suite.T().TempDir(), "sync.db"))
	db, err := datastore.NewBolt()
	suite.Require().NoError(err, "Failed to open bolt datastore")
	suite.db = db
}

func newEntity(clientID, id string, dataType int, mtime int64) *datastore.SyncEntity {
	version := int64(1)
	deleted := false
	folder := false
	return &datastore.SyncEntity{
		ClientID:  clientID,
		ID:        id,
		Version:   &version,
		Mtime:     aws.Int64(mtime),
		Ctime:     aws.Int64(mtime),
		Deleted:   &deleted,
		Folder:    &folder,
		DataType:  aws.Int(dataType),
		Specifics: []byte("specifics"),
	}
}

func (suite *BoltTestSuite) TestInsertAndHasItem() {
	ctx := context.Background()
	entity := newEntity("client1", "item1", 100, 1000)

	has, err := suite.db.HasItem(ctx, "client1", "item1")
	suite.Require().NoError(err)
	suite.False(has)

	conflict, err := suite.db.InsertSyncEntity(ctx, entity)
	suite.Require().NoError(err)
	suite.False(conflict)

	has, err = suite.db.HasItem(ctx, "client1", "item1")
	suite.Require().NoError(err)
	suite.True(has)

	// Inserting the same (clientID, ID) again should conflict.
	conflict, err = suite.db.InsertSyncEntity(ctx, entity)
	suite.Require().Error(err)
	suite.True(conflict)
}

func (suite *BoltTestSuite) TestUpdateSyncEntityConflictOnVersionMismatch() {
	ctx := context.Background()
	entity := newEntity("client1", "item1", 100, 1000)
	_, err := suite.db.InsertSyncEntity(ctx, entity)
	suite.Require().NoError(err)

	updated := newEntity("client1", "item1", 100, 2000)
	*updated.Version = 2

	// Wrong oldVersion should conflict.
	conflict, deleted, err := suite.db.UpdateSyncEntity(ctx, updated, 99)
	suite.Require().NoError(err)
	suite.True(conflict)
	suite.False(deleted)

	// Correct oldVersion should succeed.
	conflict, deleted, err = suite.db.UpdateSyncEntity(ctx, updated, 1)
	suite.Require().NoError(err)
	suite.False(conflict)
	suite.False(deleted)
}

func (suite *BoltTestSuite) TestUpdateSyncEntityMarksDeleted() {
	ctx := context.Background()
	entity := newEntity("client1", "item1", 100, 1000)
	_, err := suite.db.InsertSyncEntity(ctx, entity)
	suite.Require().NoError(err)

	update := newEntity("client1", "item1", 100, 2000)
	*update.Version = 2
	*update.Deleted = true

	conflict, deleted, err := suite.db.UpdateSyncEntity(ctx, update, 1)
	suite.Require().NoError(err)
	suite.False(conflict)
	suite.True(deleted)
}

func (suite *BoltTestSuite) TestGetUpdatesForTypeOrderingAndPagination() {
	ctx := context.Background()
	for i, mtime := range []int64{3000, 1000, 2000, 4000, 5000} {
		entity := newEntity("client1", "item"+string(rune('a'+i)), 100, mtime)
		_, err := suite.db.InsertSyncEntity(ctx, entity)
		suite.Require().NoError(err)
	}

	hasMore, entities, err := suite.db.GetUpdatesForType(ctx, 100, 0, true, "client1", 3)
	suite.Require().NoError(err)
	suite.True(hasMore)
	suite.Require().Len(entities, 3)
	suite.Equal([]int64{1000, 2000, 3000}, []int64{*entities[0].Mtime, *entities[1].Mtime, *entities[2].Mtime})

	hasMore, entities, err = suite.db.GetUpdatesForType(ctx, 100, 3000, true, "client1", 10)
	suite.Require().NoError(err)
	suite.False(hasMore)
	suite.Require().Len(entities, 2)
	suite.Equal([]int64{4000, 5000}, []int64{*entities[0].Mtime, *entities[1].Mtime})
}

func (suite *BoltTestSuite) TestGetUpdatesForTypeFiltersFolders() {
	ctx := context.Background()
	folderEntity := newEntity("client1", "folder1", 100, 1000)
	*folderEntity.Folder = true
	_, err := suite.db.InsertSyncEntity(ctx, folderEntity)
	suite.Require().NoError(err)

	nonFolderEntity := newEntity("client1", "item1", 100, 2000)
	_, err = suite.db.InsertSyncEntity(ctx, nonFolderEntity)
	suite.Require().NoError(err)

	_, entities, err := suite.db.GetUpdatesForType(ctx, 100, 0, false, "client1", 10)
	suite.Require().NoError(err)
	suite.Require().Len(entities, 1)
	suite.Equal("item1", entities[0].ID)

	_, entities, err = suite.db.GetUpdatesForType(ctx, 100, 0, true, "client1", 10)
	suite.Require().NoError(err)
	suite.Len(entities, 2)
}

func (suite *BoltTestSuite) TestHasServerDefinedUniqueTag() {
	ctx := context.Background()
	tag := aws.String("google_chrome_nigori")
	entity := newEntity("client1", "nigori1", 100, 1000)
	entity.ServerDefinedUniqueTag = tag

	has, err := suite.db.HasServerDefinedUniqueTag(ctx, "client1", *tag)
	suite.Require().NoError(err)
	suite.False(has)

	err = suite.db.InsertSyncEntitiesWithServerTags(ctx, []*datastore.SyncEntity{entity})
	suite.Require().NoError(err)

	has, err = suite.db.HasServerDefinedUniqueTag(ctx, "client1", *tag)
	suite.Require().NoError(err)
	suite.True(has)
}

func (suite *BoltTestSuite) TestClientItemCountRoundTrip() {
	ctx := context.Background()
	counts, err := suite.db.GetClientItemCount(ctx, "client1")
	suite.Require().NoError(err)
	suite.Equal(0, counts.ItemCount)

	err = suite.db.UpdateClientItemCount(ctx, counts, 5, 2)
	suite.Require().NoError(err)

	counts, err = suite.db.GetClientItemCount(ctx, "client1")
	suite.Require().NoError(err)
	suite.Equal(5, counts.ItemCount)
	suite.Equal(2, counts.HistoryItemCountPeriod4)
}

func (suite *BoltTestSuite) TestClearServerDataAndDisableSyncChain() {
	ctx := context.Background()
	entity := newEntity("client1", "item1", 100, 1000)
	_, err := suite.db.InsertSyncEntity(ctx, entity)
	suite.Require().NoError(err)

	counts, err := suite.db.GetClientItemCount(ctx, "client1")
	suite.Require().NoError(err)
	suite.Require().NoError(suite.db.UpdateClientItemCount(ctx, counts, 1, 0))

	disabled, err := suite.db.IsSyncChainDisabled(ctx, "client1")
	suite.Require().NoError(err)
	suite.False(disabled)

	suite.Require().NoError(suite.db.DisableSyncChain(ctx, "client1"))

	disabled, err = suite.db.IsSyncChainDisabled(ctx, "client1")
	suite.Require().NoError(err)
	suite.True(disabled)

	deleted, err := suite.db.ClearServerData(ctx, "client1")
	suite.Require().NoError(err)
	suite.Require().Len(deleted, 1)
	suite.Equal("item1", deleted[0].ID)

	has, err := suite.db.HasItem(ctx, "client1", "item1")
	suite.Require().NoError(err)
	suite.False(has)

	// The disabled-chain marker must survive ClearServerData.
	disabled, err = suite.db.IsSyncChainDisabled(ctx, "client1")
	suite.Require().NoError(err)
	suite.True(disabled)

	// The item count record should have been cleared.
	counts, err = suite.db.GetClientItemCount(ctx, "client1")
	suite.Require().NoError(err)
	suite.Equal(0, counts.ItemCount)
}

func TestBoltTestSuite(t *testing.T) {
	suite.Run(t, new(BoltTestSuite))
}
