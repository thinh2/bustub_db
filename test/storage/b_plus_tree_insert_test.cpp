/**
 * b_plus_tree_insert_test.cpp
 */

#include <algorithm>
#include <random>
#include <cstdio>
#include <chrono>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

TEST(BPlusTreeTests, InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
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
    LOG_DEBUG("insert key %lld, value %lld", key, value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    tree.Print(bpm);
  }

//  LOG_DEBUG("bplus tree %s",out.c_str());

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    LOG_DEBUG("get from b+tree %lld", key);
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

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest2) {
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

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
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

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

// Test with bigger key size
TEST(BPlusTreeTests, DISABLED_InsertTest3) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(14, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 5, 6);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = std::vector<int64_t>(121, 0);
  for (size_t i = 0; i < keys.size(); i++) {
    keys[i] = static_cast<int64_t>(i) + 1;
  }
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    LOG_DEBUG("insert key %lld, value %lld", key, value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    //tree.Print(bpm);
  }

  tree.Print(bpm);
//  LOG_DEBUG("bplus tree %s",out.c_str());

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    LOG_DEBUG("get from b+tree %lld", key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  LOG_DEBUG("finish get value check");
  tree.Print(bpm);
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

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}


// Test insert suffle key
TEST(BPlusTreeTests, InsertTest4) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(20, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 11, 12);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = std::vector<int64_t>(4113, 0);
  for (size_t i = 0; i < keys.size(); i++) {
    keys[i] = static_cast<int64_t>(i) + 1;
  }
  
  //std::random_device rd;
  //std::mt19937 g(rd());

  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(seed));
  for (auto key : keys) {
    LOG_DEBUG("key %lld", key);
  }
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    LOG_DEBUG("insert key %lld, value %lld", key, value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
    //tree.Print(bpm);
  }

  tree.Print(bpm);
//  LOG_DEBUG("bplus tree %s",out.c_str());

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    LOG_DEBUG("get from b+tree %lld", key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  LOG_DEBUG("finish get value check");
  //std::string tmp;
  for (auto key : keys) {
    LOG_DEBUG("key %lld", key);
  }
  tree.Print(bpm);
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

TEST(BPlusTreeTests, LeafPageTest) {
    char *leaf_ptr = new char[300];
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    GenericKey<8> index_key;
    RID rid;

    BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(leaf_ptr);
    leaf->Init(1);
    leaf->SetMaxSize(4);

    // 测试Insert(), KeyIndex()
    index_key.SetFromInteger(3);
    EXPECT_EQ(0, leaf->KeyIndex(index_key, comparator));

    setKeyValue(1, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    EXPECT_EQ(0, leaf->KeyIndex(index_key, comparator));
    //index_key.SetFromInteger(100);
    //EXPECT_EQ(1, leaf->KeyIndex(index_key, comparator));

    setKeyValue(2, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    setKeyValue(3, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    setKeyValue(4, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    EXPECT_EQ(4, leaf->GetSize());
    index_key.SetFromInteger(2);
    EXPECT_EQ(1, leaf->KeyIndex(index_key, comparator));
    index_key.SetFromInteger(4);
    EXPECT_EQ(3, leaf->KeyIndex(index_key, comparator));
    //index_key.SetFromInteger(100);
    //EXPECT_EQ(4, leaf->KeyIndex(index_key, comparator));

    // maxSize为4，最多可以容纳5个元素，测试MoveHalfTo()
    setKeyValue(5, index_key, rid);
    leaf->Insert(index_key, rid, comparator);
    char *new_leaf_ptr = new char[300];
    BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *new_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(new_leaf_ptr);
    new_leaf->Init(2);
    new_leaf->SetMaxSize(4);
    leaf->MoveHalfTo(new_leaf, nullptr);
    EXPECT_EQ(3, leaf->GetSize());
    EXPECT_EQ(2, new_leaf->GetSize());
    for (int i = 0; i < leaf->GetSize(); i++) {
      LOG_DEBUG("leaf, key %lld", leaf->KeyAt(i).ToString());
    }
    for (int i = 0; i < new_leaf->GetSize(); i++) {
      LOG_DEBUG("new_leaf, key %lld", new_leaf->KeyAt(i).ToString());
    }
    //EXPECT_EQ(2, leaf->GetNextPageId());

    // 测试Lookup(), 当前leaf:[(1, 1), (2, 2), (3, 3)], new_leaf:[(4, 4), (5, 5)]
    /*RID value;
    setKeyValue(2, index_key, rid);
    EXPECT_TRUE(leaf->Lookup(index_key, value, comparator));
    EXPECT_EQ(rid, value);
    index_key.SetFromInteger(6);
    EXPECT_FALSE(leaf->Lookup(index_key, value, comparator));*/

/*
    // 测试RemoveAndDeleteRecord()
    index_key.SetFromInteger(100);
    EXPECT_EQ(3, leaf->RemoveAndDeleteRecord(index_key, comparator));
    index_key.SetFromInteger(2);
    EXPECT_EQ(2, leaf->RemoveAndDeleteRecord(index_key, comparator));

    index_key.SetFromInteger(1);
    EXPECT_EQ(1, leaf->RemoveAndDeleteRecord(index_key, comparator));
    EXPECT_EQ(1, leaf->GetSize());
    EXPECT_EQ(2, new_leaf->GetSize());

    // 测试MoveAllTo(), 当前leaf:[(3, 3)], new_leaf:[(4, 4), (5, 5)]
    new_leaf->MoveAllTo(leaf, 0, nullptr);
    EXPECT_EQ(0, new_leaf->GetSize());
    EXPECT_EQ(3, leaf->GetSize());
*/
    delete []leaf_ptr;
    delete []new_leaf_ptr;
}

TEST(BPlusTreeTests, DISABLED_LeafPageTestShuffle) {
    char *leaf_ptr = new char[300];
    Schema *key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);

    GenericKey<8> index_key;
    RID rid;

    BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(leaf_ptr);
    leaf->Init(1);
    int n_keys = 17;
    leaf->SetMaxSize(n_keys);

    // 测试Insert(), KeyIndex()
    
    std::vector<int64_t> keys(n_keys, 0);
    for (size_t i = 0; i < keys.size(); i++) {
      keys[i] = i + 1;
    }  
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(keys.begin(), keys.end(), std::default_random_engine(seed));

    for (auto key: keys) {
      LOG_DEBUG("keys %lld", key);
      setKeyValue(key, index_key, rid);
      leaf->Insert(index_key, rid, comparator);
    }

    char *new_leaf_ptr = new char[300];
    BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *new_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(new_leaf_ptr);
    new_leaf->Init(2);
    new_leaf->SetMaxSize(n_keys);
    leaf->MoveHalfTo(new_leaf, nullptr);
    EXPECT_EQ(n_keys / 2, leaf->GetSize());
    EXPECT_EQ(n_keys / 2, new_leaf->GetSize());
    for (int i = 0; i < leaf->GetSize(); i++) {
      LOG_DEBUG("leaf, key %lld", leaf->KeyAt(i).ToString());
    }
    for (int i = 0; i < new_leaf->GetSize(); i++) {
      LOG_DEBUG("new_leaf, key %lld", new_leaf->KeyAt(i).ToString());
    }
    delete []leaf_ptr;
    delete []new_leaf_ptr;
}

}