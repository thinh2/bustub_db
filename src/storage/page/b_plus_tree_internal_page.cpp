//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>
#include "common/logger.h"

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 * 
 * what value we return if we don't find array index equal to "value" ?
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int idx = 0; idx < GetSize(); idx++) {
    if (array[idx].second == value) {
      return idx;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  if (GetSize() > 1 && comparator(key, KeyAt(1)) == -1) {
    return ValueAt(0);
  }
  for (int i = 1; i < GetSize(); i++) {
    if (comparator(KeyAt(i), key) <= 0) {
      if (i == GetSize() - 1) {
        return ValueAt(i);
      } else if (comparator(key, KeyAt(i + 1)) == -1) {
        return ValueAt(i);
      }
    }
  }
  return INVALID_PAGE_ID;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1] = std::make_pair(new_key, new_value);
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 * using insertion sort
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int new_size = GetSize() + 1;
  SetSize(new_size);
  array[GetSize() - 1] = std::make_pair(new_key, new_value);
  bool isSwap = false;
  for (int idx = 0; idx < GetSize(); idx++) {
    if (isSwap) {
      std::swap(array[idx], array[GetSize() - 1]);
    }
    if (array[idx].second == old_value) {
      isSwap = true;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int new_size = GetSize() / 2;
  int recipient_size = GetSize() - new_size;
  recipient->CopyNFrom(&array[new_size], recipient_size, buffer_pool_manager);
  LOG_DEBUG("success");
  memset(array + new_size, 0, sizeof(MappingType) * recipient_size); // zero
  SetSize(new_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 * 
 *    page_3: 1, page_1, 2, page_2
 *    Move page_4: 2, page_2
 *    page_1 (parent_page_id: page_3)
 *    page_2 (parent_pare_id: page_4)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  SetSize(size);
  memcpy(array, items, sizeof(MappingType) * size);
  for (int idx = 0; idx < size; idx++) {
    LOG_DEBUG("page id %d", array[idx].second);
    bustub::Page *page = buffer_pool_manager->FetchPage(array[idx].second);
    if (page == NULL) {
      LOG_DEBUG("null page id");
    }
    // convert page to B_plus_page
    // set parent_page_id = this->PageId;
    // persist to disk
    // Unpin page
    bustub::BPlusTreePage *bPlusPage = reinterpret_cast<BPlusTreePage*>(page->GetData());
    bPlusPage->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() { return INVALID_PAGE_ID; }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(0, middle_key);
  memcpy(recipient->array + recipient->GetSize(), this->array, sizeof(MappingType) * this->GetSize());
  for (int i = 0; i < this->GetSize(); ++i) {
    Page *page = buffer_pool_manager->FetchPage(this->ValueAt(i));
    BPlusTreePage *b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    b_plus_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(this->ValueAt(i), true);
  }
  recipient->IncreaseSize(this->GetSize());
  memset(this->array, 0, sizeof(MappingType) * this->GetSize());
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(this->array[0], buffer_pool_manager);
  this->IncreaseSize(-1);
  memmove(this->array, this->array + 1, sizeof(MappingType) * this->GetSize());
  memset(this->array + this->GetSize(), 0, sizeof(MappingType));
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  this->array[this->GetSize()] = pair;
  this->IncreaseSize(1);
  page_id_t last_page_id = this->ValueAt(this->GetSize() - 1);
  Page* page = buffer_pool_manager->FetchPage(last_page_id);
  BPlusTreePage* b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  b_plus_page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(last_page_id, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  int last_index = this->GetSize() - 1;
  recipient->SetKeyAt(0, middle_key); 
  recipient->CopyFirstFrom(this->array[last_index], buffer_pool_manager);
  
  memset(this->array + last_index, 0, sizeof(MappingType));
  this->IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  memmove(this->array + 1, this->array, sizeof(MappingType) * (this->GetSize()));
  this->array[0] = pair;
  this->IncreaseSize(1);

  page_id_t update_page_id = this->ValueAt(0);
  Page* page = buffer_pool_manager->FetchPage(update_page_id);
  BPlusTreePage* b_plus_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  b_plus_page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(update_page_id, true);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
