/**
 * b_plus_tree_delete_test.cpp
 */

#include <algorithm>
#include <cstdio>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

TEST(BPlusTreeTests, DISABLED_DeleteTest1) {
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DISABLED_DeleteTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  start_key = 2;
  current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

void setKeyValue(int64_t k, GenericKey<8> &index_key, RID &rid) {
    index_key.SetFromInteger(k);
    int64_t value = k & 0xFFFFFFFF;
    rid.Set((int32_t)(k >> 32), value);
}

TEST(BPlusTreeTests, DISABLED_LeafPageRemoveTest) {
  char *leaf_ptr = new char[300];

  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *leaf_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(leaf_ptr);
  leaf_page->Init(1, 2, 6);
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  RID rid;
  GenericKey<8> index_key;
  for (auto key : keys) {
    setKeyValue(key, index_key, rid);
    leaf_page->Insert(index_key, rid, comparator);
  }

  index_key.SetFromInteger(4);
  int new_size = leaf_page->RemoveAndDeleteRecord(index_key, comparator);
  EXPECT_EQ(4, new_size);

  for (auto key : keys) {
    if (key == 4) continue;
    index_key.SetFromInteger(key);
    EXPECT_EQ(leaf_page->Lookup(index_key, &rid, comparator), true);
  }
}

TEST(BPlusTreeTests, DISABLED_LeafPageMoveLastToFrontOf) {
  char *leaf_ptr = new char[300];
  char *recipient_ptr = new char[300];
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *leaf_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(leaf_ptr);
  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *recipient_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(recipient_ptr);

  leaf_page->Init(1, 2, 6);
  recipient_page->Init(2, 2, 6);
  std::vector<int64_t> leaf_keys = {1, 2, 3, 4, 5};
  GenericKey<8> index_key;
  RID rid;

  for (auto key : leaf_keys) {
    setKeyValue(key, index_key, rid);
    leaf_page->Insert(index_key, rid, comparator);
  }

  std::vector<int64_t> recipient_keys = {6, 7};
  for (auto key : recipient_keys) {
    setKeyValue(key, index_key, rid);
    recipient_page->Insert(index_key, rid, comparator);
  }

  leaf_page->MoveLastToFrontOf(recipient_page);
  EXPECT_EQ(4, leaf_page->GetSize());
  EXPECT_EQ(3, recipient_page->GetSize());

  auto item = recipient_page->KeyAt(0);
  index_key.SetFromInteger(5);
  EXPECT_EQ(item.ToString(), 5);
}

TEST(BPlusTreeTests, DISABLED_LeafPageMoveAllTo) {
  char *leaf_ptr = new char[300];
  char *recipient_ptr = new char[300];
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *leaf_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(leaf_ptr);
  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *recipient_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(recipient_ptr);

  leaf_page->Init(1, 2, 6);
  leaf_page->SetNextPageId(3);

  recipient_page->Init(2, 2, 6);
  recipient_page->SetNextPageId(1);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6};
  int recipient_size = 4;
  RID rid;

  std::vector<std::pair<GenericKey<8>, RID>> entries;
  GenericKey<8> index_key;
  for (auto key : keys) {
    setKeyValue(key, index_key, rid);
    entries.push_back(std::make_pair(index_key, rid));
  }

  for (int i = 0; i < recipient_size; ++i) {
    recipient_page->Insert(entries[i].first, entries[i].second, comparator);
  }

  for (size_t i = recipient_size; i < keys.size(); ++i) {
    leaf_page->Insert(entries[i].first, entries[i].second, comparator);
  }

  leaf_page->MoveAllTo(recipient_page);
  EXPECT_EQ(0, leaf_page->GetSize());
  EXPECT_EQ(6, recipient_page->GetSize());
  EXPECT_EQ(3, recipient_page->GetNextPageId());
  
  for (int i = 0; i < recipient_page->GetSize(); i++) {
    EXPECT_EQ(entries[i].second, recipient_page->GetItem(i).second);
  }
}

TEST(BPlusTreeTests, DISABLED_LeafPageMoveFirstToEndOf) {
  char *leaf_ptr = new char[300];
  char *recipient_ptr = new char[300];
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *leaf_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(leaf_ptr);
  BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *recipient_page = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(recipient_ptr);

  leaf_page->Init(1, 2, 6);
  recipient_page->Init(2, 2, 6);
  std::vector<int64_t> leaf_keys = {1, 2};
  GenericKey<8> index_key;
  RID rid;

  for (auto key : leaf_keys) {
    setKeyValue(key, index_key, rid);
    leaf_page->Insert(index_key, rid, comparator);
  }

  std::vector<int64_t> recipient_keys = {3, 4, 5, 6, 7};
  for (auto key : recipient_keys) {
    setKeyValue(key, index_key, rid);
    recipient_page->Insert(index_key, rid, comparator);
  }

  recipient_page->MoveFirstToEndOf(leaf_page);
  EXPECT_EQ(3, leaf_page->GetSize());
  EXPECT_EQ(4, recipient_page->GetSize());

  auto item = leaf_page->KeyAt(2);
  EXPECT_EQ(item.ToString(), 3);
}

}  // namespace bustub
