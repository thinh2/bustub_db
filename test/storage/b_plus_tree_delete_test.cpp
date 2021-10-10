/**
 * b_plus_tree_delete_test.cpp
 */

#include <algorithm>
#include <cstdio>
#include <chrono>
#include <random>

#include "b_plus_tree_test_util.h"  // NOLINT
#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

TEST(BPlusTreeTests, DeleteTest1) {
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

  std::vector<int64_t> remove_keys = {1, 5, 0, 6, 9};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    LOG_DEBUG("remove key %lld", key);
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

TEST(BPlusTreeTests, DeleteTest2) {
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

TEST(BPlusTreeTests, LeafPageRemoveTest) {
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

TEST(BPlusTreeTests, LeafPageMoveLastToFrontOf) {
  char *leaf_ptr = new char[300];
  char *recipient_ptr = new char[300];
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);

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

  leaf_page->MoveLastToFrontOf(recipient_page, index_key, bpm);
  EXPECT_EQ(4, leaf_page->GetSize());
  EXPECT_EQ(3, recipient_page->GetSize());

  auto item = recipient_page->KeyAt(0);
  index_key.SetFromInteger(5);
  EXPECT_EQ(item.ToString(), 5);
}

TEST(BPlusTreeTests, LeafPageMoveAllTo) {
  char *leaf_ptr = new char[300];
  char *recipient_ptr = new char[300];
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);

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

  leaf_page->MoveAllTo(recipient_page, index_key, bpm);
  EXPECT_EQ(0, leaf_page->GetSize());
  EXPECT_EQ(6, recipient_page->GetSize());
  EXPECT_EQ(3, recipient_page->GetNextPageId());
  
  for (int i = 0; i < recipient_page->GetSize(); i++) {
    EXPECT_EQ(entries[i].second, recipient_page->GetItem(i).second);
  }
}

TEST(BPlusTreeTests, LeafPageMoveFirstToEndOf) {
  char *leaf_ptr = new char[300];
  char *recipient_ptr = new char[300];
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);

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


  leaf_keys.push_back(recipient_keys[0]);
  recipient_keys.erase(recipient_keys.begin());

  recipient_page->MoveFirstToEndOf(leaf_page, index_key, bpm);
  EXPECT_EQ(3, leaf_page->GetSize());
  EXPECT_EQ(4, recipient_page->GetSize());

  auto item = leaf_page->KeyAt(2);
  EXPECT_EQ(item.ToString(), 3);

  for (size_t i = 0; i < leaf_keys.size(); ++i) {
    index_key.SetFromInteger(leaf_keys[i]);
    EXPECT_EQ(0, comparator(index_key, leaf_page->KeyAt(i)));
  }

  for (size_t i = 0; i < recipient_keys.size(); ++i) {
    index_key.SetFromInteger(recipient_keys[i]);
    EXPECT_EQ(0, comparator(index_key, recipient_page->KeyAt(i)));
  }
}

template<typename Key, typename Comparator>
page_id_t mock_child_internal_page(BufferPoolManager *bpm, Comparator &comparator, std::vector<int64_t> &keys) {
  page_id_t ret_page_id, page_id;
  auto page = bpm->NewPage(&ret_page_id);
  BPlusTreeInternalPage<Key, page_id_t, Comparator> *internal_page = reinterpret_cast<BPlusTreeInternalPage<Key, page_id_t, Comparator> *>(page->GetData());
  internal_page->Init(ret_page_id, INVALID_PAGE_ID);
  bpm->UnpinPage(ret_page_id, true);
  Key index_key;

  std::pair<Key, page_id_t> *items = new std::pair<Key, page_id_t>[keys.size() + 1];

  page = bpm->NewPage(&page_id);
  internal_page = reinterpret_cast<BPlusTreeInternalPage<Key, page_id_t, Comparator> *>(page->GetData());
  internal_page->Init(page_id, ret_page_id);
  items[0].second = page_id;
  bpm->UnpinPage(page_id, true);

  int idx = 1;
  for (auto key: keys) {
    page = bpm->NewPage(&page_id);
    internal_page = reinterpret_cast<BPlusTreeInternalPage<Key, page_id_t, Comparator> *>(page->GetData());
    internal_page->Init(page_id, ret_page_id);
    bpm->UnpinPage(page_id, true);

    index_key.SetFromInteger(key);
    items[idx] = std::make_pair(index_key, page_id);
    idx++;
  }

  page = bpm->FetchPage(ret_page_id);
  internal_page = reinterpret_cast<BPlusTreeInternalPage<Key, page_id_t, Comparator> *>(page->GetData());
  internal_page->CopyNFrom(items, keys.size() + 1, bpm);
  bpm->UnpinPage(ret_page_id, true);
  
  delete[] items;
  return ret_page_id;
}

template<typename Key, typename Comparator>
page_id_t mock_parent_internal_page(BufferPoolManager *bpm, Comparator &comparator, std::vector<int64_t> &keys, std::vector<page_id_t> &values) {
  page_id_t ret_page_id;
  auto page = bpm->NewPage(&ret_page_id);
  BPlusTreeInternalPage<Key, page_id_t, Comparator> *internal_page = reinterpret_cast<BPlusTreeInternalPage<Key, page_id_t, Comparator> *>(page->GetData());
  internal_page->Init(ret_page_id, INVALID_PAGE_ID);
  bpm->UnpinPage(ret_page_id, true);
  Key index_key;
  std::pair<Key, page_id_t> *items = new std::pair<Key, page_id_t>[values.size()];

  for (size_t i = 0; i < values.size(); ++i) {
    if (i == 0) {
      index_key.SetFromInteger(-1);
    } else {
      index_key.SetFromInteger(keys[i - 1]);
    }
    items[i] = std::make_pair(index_key, values[i]);
  }

  page = bpm->FetchPage(ret_page_id);
  internal_page = reinterpret_cast<BPlusTreeInternalPage<Key, page_id_t, Comparator> *>(page->GetData());
  internal_page->CopyNFrom(items, values.size(), bpm);
  bpm->UnpinPage(ret_page_id, true);
  
  delete[] items;
  return ret_page_id;
}

TEST(BPlusTreeTests, InternalPageMoveAllTo) {
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  page_id_t page_id;
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);

  std::vector<int64_t> left_keys = {1, 2};
  std::vector<int64_t> right_keys = {5, 7, 9};
  GenericKey<8> index_key;

  page_id_t left_page_id = mock_child_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, left_keys);
  page_id_t right_page_id = mock_child_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, right_keys);

  Page *left_page = bpm->FetchPage(left_page_id);
  Page *right_page = bpm->FetchPage(right_page_id);

  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *internal_left_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, bustub::page_id_t, GenericComparator<8>> *>(left_page->GetData());
  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *internal_right_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, bustub::page_id_t, GenericComparator<8>> *>(right_page->GetData());

  index_key.SetFromInteger(4); // middle keys
  internal_right_page->MoveAllTo(internal_left_page, index_key, bpm);
  EXPECT_EQ(7, internal_left_page->GetSize());
  EXPECT_EQ(0, internal_right_page->GetSize());

  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *internal_page;
  // check child's parent page id is updated 
  for (int i = 0; i < internal_left_page->GetSize(); ++i) {
    page_id = internal_left_page->ValueAt(i);
    auto page = bpm->FetchPage(page_id);
    internal_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(page->GetData());
    EXPECT_EQ(internal_page->GetParentPageId(), internal_left_page->GetPageId());
    bpm->UnpinPage(page_id, true);
  }

  delete bpm;
  delete disk_manager;
}

TEST(BPlusTreeTests, InternalPageMoveFirstToEndOf) {
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);

  std::vector<int64_t> left_keys = {1, 2};
  std::vector<int64_t> right_keys = {5, 7, 9, 11};
  std::vector<int64_t> parent_keys = {4};
  GenericKey<8> index_key;

  page_id_t left_page_id = mock_child_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, left_keys);
  page_id_t right_page_id = mock_child_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, right_keys);
  
  Page *left_page = bpm->FetchPage(left_page_id);
  Page *right_page = bpm->FetchPage(right_page_id);

  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *internal_left_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, bustub::page_id_t, GenericComparator<8>> *>(left_page->GetData());
  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *internal_right_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, bustub::page_id_t, GenericComparator<8>> *>(right_page->GetData());

  std::vector<page_id_t> parent_values;
  parent_values.push_back(left_page->GetPageId());
  parent_values.push_back(right_page->GetPageId());

  //page_id_t parent_page_id = mock_parent_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, parent_keys, parent_values);
  index_key.SetFromInteger(parent_keys[0]); // middle keys
  page_id_t move_page_id = internal_right_page->ValueAt(0);

  internal_right_page->MoveFirstToEndOf(internal_left_page, index_key, bpm);
  EXPECT_EQ(4, internal_left_page->GetSize());
  EXPECT_EQ(4, internal_right_page->GetSize());

  left_keys.push_back(parent_keys[0]);
  int64_t middle_key = right_keys[0];
  right_keys.erase(right_keys.begin());
  parent_keys[0] = middle_key;

  Page *page;
  BPlusTreePage *b_plus_page;
  // check left page is true
  for (int i = 1; i < internal_left_page->GetSize(); ++i) {
    index_key.SetFromInteger(left_keys[i - 1]);
    EXPECT_EQ(0, comparator(internal_left_page->KeyAt(i), index_key));
    page = bpm->FetchPage(internal_left_page->ValueAt(i));
    b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    EXPECT_EQ(b_plus_page->GetParentPageId(), internal_left_page->GetPageId());
    bpm->UnpinPage(internal_left_page->ValueAt(i), true);
  }

  // check right page is true
  for (int i = 1; i < internal_right_page->GetSize(); ++i) {
    index_key.SetFromInteger(right_keys[i - 1]);
    EXPECT_EQ(0, comparator(internal_right_page->KeyAt(i), index_key));
    page = bpm->FetchPage(internal_right_page->ValueAt(i));
    b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    EXPECT_EQ(b_plus_page->GetParentPageId(), internal_right_page->GetPageId());
    bpm->UnpinPage(internal_right_page->ValueAt(i), true);
  }

  page_id_t check_page_id = internal_left_page->ValueAt(internal_left_page->GetSize() - 1);
  EXPECT_EQ(check_page_id, move_page_id);
  page = bpm->FetchPage(check_page_id);
  b_plus_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *>(page->GetData());
  EXPECT_EQ(b_plus_page->GetParentPageId(), internal_left_page->GetPageId());
  bpm->UnpinPage(check_page_id, true);

  index_key.SetFromInteger(left_keys[left_keys.size() - 1]);
  EXPECT_EQ(0, comparator(internal_left_page->KeyAt(internal_left_page->GetSize() - 1), index_key));

  // check parent page is true
  /*page = bpm->FetchPage(parent_page_id);
  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *parent_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(page->GetData());
  index_key.SetFromInteger(parent_keys[0]);
  EXPECT_EQ(0, comparator(index_key, parent_page->KeyAt(1)));
  bpm->UnpinPage(parent_page_id, true);*/

  delete bpm;
  delete disk_manager;
}

TEST(BPlusTreeTests, InternalPageMoveLastToFrontOf) {
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);

  std::vector<int64_t> left_keys = {1, 2, 3, 4};
  std::vector<int64_t> right_keys = {6, 7};
  std::vector<int64_t> parent_keys = {5};
  GenericKey<8> index_key;

  page_id_t left_page_id = mock_child_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, left_keys);
  page_id_t right_page_id = mock_child_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, right_keys);
  
  Page *left_page = bpm->FetchPage(left_page_id);
  Page *right_page = bpm->FetchPage(right_page_id);

  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *internal_left_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, bustub::page_id_t, GenericComparator<8>> *>(left_page->GetData());
  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *internal_right_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, bustub::page_id_t, GenericComparator<8>> *>(right_page->GetData());

  std::vector<page_id_t> parent_values;
  parent_values.push_back(left_page->GetPageId());
  parent_values.push_back(right_page->GetPageId());

  //page_id_t parent_page_id = mock_parent_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, parent_keys, parent_values);
  page_id_t move_page_id = internal_left_page->ValueAt(4);

  index_key.SetFromInteger(parent_keys[0]); // middle keys
  internal_left_page->MoveLastToFrontOf(internal_right_page, index_key, bpm);

  EXPECT_EQ(4, internal_left_page->GetSize());
  EXPECT_EQ(4, internal_right_page->GetSize());

  right_keys.insert(right_keys.begin(), parent_keys[0]);
  left_keys.pop_back();

  Page *page;
  BPlusTreePage *b_plus_page;

  // check move page id is true
  EXPECT_EQ(internal_right_page->ValueAt(0), move_page_id);
  page = bpm->FetchPage(internal_right_page->ValueAt(0));
  b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  EXPECT_EQ(b_plus_page->GetParentPageId(), internal_right_page->GetPageId());
  bpm->UnpinPage(internal_right_page->ValueAt(0), true);

  index_key.SetFromInteger(right_keys[0]);
  EXPECT_EQ(0, comparator(internal_right_page->KeyAt(1), index_key));

  // check parent page is true
  /*
  page = bpm->FetchPage(parent_page_id);
  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *parent_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(page->GetData());
  index_key.SetFromInteger(parent_keys[0]);
  EXPECT_EQ(0, comparator(index_key, parent_page->KeyAt(1)));
  bpm->UnpinPage(parent_page_id, true);
  */

  // check left page is true
  for (int i = 1; i < internal_left_page->GetSize(); ++i) {
    index_key.SetFromInteger(left_keys[i - 1]);
    EXPECT_EQ(0, comparator(internal_left_page->KeyAt(i), index_key));
    page = bpm->FetchPage(internal_left_page->ValueAt(i));
    b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    EXPECT_EQ(b_plus_page->GetParentPageId(), internal_left_page->GetPageId());
    bpm->UnpinPage(internal_left_page->ValueAt(i), true);
  }

  // check right page is true
  for (int i = 1; i < internal_right_page->GetSize(); ++i) {
    index_key.SetFromInteger(right_keys[i - 1]);
    EXPECT_EQ(0, comparator(internal_right_page->KeyAt(i), index_key));
    page = bpm->FetchPage(internal_right_page->ValueAt(i));
    b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    EXPECT_EQ(b_plus_page->GetParentPageId(), internal_right_page->GetPageId());
    bpm->UnpinPage(internal_right_page->ValueAt(i), true);
  }

  delete bpm;
  delete disk_manager;
}
/*
TEST(BPlusTreeTests, BPlusTreeRedistribute) {
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(5, disk_manager);

  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);

  std::vector<int64_t> left_keys = {1, 2};
  std::vector<int64_t> right_keys = {5, 7, 9, 11};
  std::vector<int64_t> parent_keys = {4};
  GenericKey<8> index_key;

  page_id_t left_page_id = mock_child_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, left_keys);
  page_id_t right_page_id = mock_child_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, right_keys);
  
  Page *left_page = bpm->FetchPage(left_page_id);
  Page *right_page = bpm->FetchPage(right_page_id);

  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *internal_left_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, bustub::page_id_t, GenericComparator<8>> *>(left_page->GetData());
  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *internal_right_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, bustub::page_id_t, GenericComparator<8>> *>(right_page->GetData());

  std::vector<page_id_t> parent_values;
  parent_values.push_back(left_page->GetPageId());
  parent_values.push_back(right_page->GetPageId());

  page_id_t parent_page_id = mock_parent_internal_page<GenericKey<8>, GenericComparator<8>>(bpm, comparator, parent_keys, parent_values);
  index_key.SetFromInteger(parent_keys[0]); // middle keys
  page_id_t move_page_id = internal_right_page->ValueAt(0);

  internal_right_page->MoveFirstToEndOf(internal_left_page, index_key, bpm);
  tree.Redistribute(internal_left_page, internal_right_page, 0);
  EXPECT_EQ(4, internal_left_page->GetSize());
  EXPECT_EQ(4, internal_right_page->GetSize());

  left_keys.push_back(parent_keys[0]);
  int64_t middle_key = right_keys[0];
  right_keys.erase(right_keys.begin());
  parent_keys[0] = middle_key;

  Page *page;
  BPlusTreePage *b_plus_page;

  page_id_t check_page_id = internal_left_page->ValueAt(internal_left_page->GetSize() - 1);
  EXPECT_EQ(check_page_id, move_page_id);
  page = bpm->FetchPage(check_page_id);
  b_plus_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *>(page->GetData());
  EXPECT_EQ(b_plus_page->GetParentPageId(), internal_left_page->GetPageId());
  bpm->UnpinPage(check_page_id, true);

  index_key.SetFromInteger(left_keys[left_keys.size() - 1]);
  EXPECT_EQ(0, comparator(internal_left_page->KeyAt(internal_left_page->GetSize() - 1), index_key));

  //check parent page is true
  page = bpm->FetchPage(parent_page_id);
  BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8> > *parent_page = reinterpret_cast<BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>> *>(page->GetData());
  index_key.SetFromInteger(parent_keys[0]);
  EXPECT_EQ(0, comparator(index_key, parent_page->KeyAt(1)));
  bpm->UnpinPage(parent_page_id, true);

  delete bpm;
  delete disk_manager;
  remove("test.db");
}*/

// big test delete
TEST(BPlusTreeTests, CoalesceTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(20, disk_manager);
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

  std::vector<int64_t> keys(6, 0);
  for (size_t i = 0; i < keys.size(); i++) {
    keys[i] = static_cast<int64_t>(i) + 1;
  }
  
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

  std::vector<int64_t> remove_keys{4};
  // Remove all key
  for (auto key: remove_keys) {
    LOG_DEBUG("remove key %lld", key);
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  EXPECT_EQ(tree.IsEmpty(), false);
  tree.Print(bpm);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, RedistributeTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(20, disk_manager);
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

  std::vector<int64_t> keys(8, 0);
  for (size_t i = 0; i < keys.size(); i++) {
    keys[i] = static_cast<int64_t>(i) + 1;
  }
  
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

  std::vector<int64_t> remove_keys{3, 2};
  // Remove all key
  for (auto key: remove_keys) {
    LOG_DEBUG("remove key %lld", key);
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  EXPECT_EQ(tree.IsEmpty(), false);
  tree.Print(bpm);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}


// big test delete
TEST(BPlusTreeTests, DeleteTestBig) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(20, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 10, 11);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = std::vector<int64_t>(2131, 0);
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

  seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(keys.begin(), keys.end(), std::default_random_engine(seed));

  // Remove all key
  for (auto key: keys) {
    LOG_DEBUG("remove key %lld", key);
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
    //tree.Print(bpm);
  }

  EXPECT_EQ(tree.IsEmpty(), true);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
