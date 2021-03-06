//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return (root_page_id_ == INVALID_PAGE_ID);
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *page = FindLeafPage(key);
  LOG_DEBUG("leaf page %d", page->GetPageId());
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType tmp;
  if (leaf_page->Lookup(key, &tmp, comparator_)) {
    result->push_back(tmp);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LOG_DEBUG("insert value: %s", value.ToString().c_str());
  LOG_DEBUG("root_page_id %d", root_page_id_);
  if (IsEmpty()) {
    LOG_DEBUG("start new tree");
    StartNewTree(key, value);
    return InsertIntoLeaf(key, value, transaction);
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_root_page_id;
  Page *root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
  LOG_DEBUG("new root page id %d", new_root_page_id);
  LeafPage *bplus_root_page = reinterpret_cast<LeafPage *>(root_page->GetData());
  bplus_root_page->Init(new_root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  root_page_id_ = new_root_page_id;
  UpdateRootPageId(false);
  buffer_pool_manager_->UnpinPage(new_root_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page* page = FindLeafPage(key);
  LeafPage* leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  LOG_DEBUG("insert into leaf page_id: %d, max_size: %d\n", int(leaf_page->GetPageId()), leaf_page->GetMaxSize());
  ValueType tmp;
  if (leaf_page->Lookup(key, &tmp, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  /* insert normally*/
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    leaf_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  } else {
    // Insert and split
    leaf_page->Insert(key, value, comparator_);
    LOG_DEBUG("[SPLIT] insert into leaf page_id: %d, value %s", int(leaf_page->GetPageId()), value.ToString().c_str());
    LeafPage* new_leaf_page = Split(leaf_page);
    page_id_t tmp = leaf_page->GetNextPageId();
    leaf_page->SetNextPageId(new_leaf_page->GetPageId());
    new_leaf_page->SetNextPageId(tmp);
    InsertIntoParent(reinterpret_cast<BPlusTreePage *>(leaf_page), 
                      new_leaf_page->KeyAt(0),
                      reinterpret_cast<BPlusTreePage *>(new_leaf_page));
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  auto page = buffer_pool_manager_->NewPage(&new_page_id);
  LOG_DEBUG("new_page_id %d", new_page_id);
  N *recipient_page = reinterpret_cast<N *>(page->GetData());
  if (node->IsLeafPage()) {
    recipient_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  } else {
    recipient_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  }
  node->MoveHalfTo(recipient_page, buffer_pool_manager_);
  LOG_DEBUG("split success recipient_page %d, original_page %d", new_page_id, node->GetPageId());
  return recipient_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // TODO: consider old_node is root node case

  if (old_node->IsRootPage()) {
    page_id_t new_root_page_id;
    auto new_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    if (new_page == nullptr) {
      throw std::invalid_argument("invalid page");
    }

    InternalPage *new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root_page->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(false);

    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return;
  }

  page_id_t parent_page_id = old_node->GetParentPageId();
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  // if no need split check
  LOG_DEBUG("parent_page: id %d, size %d, max_size %d", parent_page->GetPageId(), parent_page->GetSize(), parent_page->GetMaxSize());
  if (parent_page->GetSize() < parent_page->GetMaxSize()) {
    //get old_node page_id
    page_id_t old_node_page_id = old_node->GetPageId();
    parent_page->InsertNodeAfter(old_node_page_id, key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page_id);

    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    buffer_pool_manager_->UnpinPage(old_node_page_id, true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    return;
  } else {
    new_node->SetParentPageId(parent_page_id);
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    LOG_DEBUG("[InsertToParent BeforeSplit] old_node %d, key %lld, new_node %d", old_node->GetPageId(), key.ToString(), new_node->GetPageId());
    InternalPage *new_parent_page = Split(parent_page);
    LOG_DEBUG("[InsertToParent Split] old_node %d, key %lld, new_node %d", old_node->GetPageId(), key.ToString(), new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

    InsertIntoParent(reinterpret_cast<BPlusTreePage *>(parent_page),
                    new_parent_page->KeyAt(0),
                    reinterpret_cast<BPlusTreePage *>(new_parent_page),
                    transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  Page *page = FindLeafPage(key);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->RemoveAndDeleteRecord(key, comparator_);
  LOG_DEBUG("leaf page id %d, min_size %d, curr_size %d", page->GetPageId(), leaf_page->GetMinSize(), leaf_page->GetSize());

  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    if (!leaf_page->IsRootPage()) {
      CoalesceOrRedistribute(leaf_page, transaction);
    } else if (leaf_page->GetSize() == 0) {
      AdjustRoot(reinterpret_cast<BPlusTreePage *>(leaf_page));
    }
  }
  LOG_DEBUG("leaf page id %d", page->GetPageId());

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // GetParentPageId, fetch_this_page, find_sibling (left or right base on index), fetch sibling page, check size to coalesce or redistribute
  // process if it is the root page

  // no need coalesce or redistrubtion
  /*if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }*/

  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  page_id_t parent_page_id = node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(page->GetData());
  int node_idx = parent_node->ValueIndex(node->GetPageId());

  int neighbor_idx;
  if (node_idx == 0) {
    neighbor_idx = node_idx + 1;
  } else {
    neighbor_idx = node_idx - 1;
  }

  page_id_t sibling_page_id = parent_node->ValueAt(neighbor_idx);
  page = buffer_pool_manager_->FetchPage(sibling_page_id);
  N *sibling_node = reinterpret_cast<N *>(page->GetData());
  LOG_DEBUG("current_page_id %d, parent page id %d, sibling page id %d", node->GetPageId(), parent_page_id, sibling_page_id);
  if (node->GetSize() + sibling_node->GetSize() <= node->GetMaxSize()) {
    Coalesce(sibling_node, node, parent_node, node_idx, transaction);
  } else {
    Redistribute(sibling_node, node, node_idx);
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  buffer_pool_manager_->UnpinPage(sibling_page_id, false);
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              define left or right sibling node
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  if (index == 0) {
    KeyType middle_key = parent->KeyAt(index + 1);
    neighbor_node->MoveAllTo(node, middle_key, buffer_pool_manager_);
    
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
    
    parent->Remove(index + 1);
  } else {
    KeyType middle_key = parent->KeyAt(index);
    node->MoveAllTo(neighbor_node, middle_key, buffer_pool_manager_);

    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(node->GetPageId());

    parent->Remove(index);
  }
  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  page_id_t parent_page_id = neighbor_node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(page->GetData());

  if (index == 0) {
    KeyType middle_key = parent_node->KeyAt(index + 1);
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
    parent_node->SetKeyAt(index + 1, neighbor_node->KeyAt(0));
  } else {
    KeyType middle_key = parent_node->KeyAt(index);
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
    parent_node->SetKeyAt(index, node->KeyAt(0));
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  LOG_DEBUG("root_page id %d, root_page size %d", old_root_node->GetPageId(), old_root_node->GetSize());
  if (old_root_node->GetSize() > 1) {
    return false;
  }
  if (old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
  } else {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = internal_page->ValueAt(0);
    Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    b_plus_tree_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(b_plus_tree_page->GetPageId(), true);
  }

  LOG_DEBUG("delete root_page id %d", old_root_node->GetPageId());

  buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
  buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
  UpdateRootPageId(false);
  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType key;
  page_id_t left_most_page_id_ = FindLeafPage(key, true)->GetPageId();
  buffer_pool_manager_->UnpinPage(left_most_page_id_, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, left_most_page_id_, 0, false);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  LOG_DEBUG("begin iterator");
  page_id_t leaf_page_id_ = FindLeafPage(key)->GetPageId();
  LOG_DEBUG("start page_id iterator %d", leaf_page_id_);
  LeafPage *leaf_page_ = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id_)->GetData());
  int idx = leaf_page_->KeyIndex(key, comparator_);
  buffer_pool_manager_->UnpinPage(leaf_page_id_, false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page_id_, idx, false);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { 
  return INDEXITERATOR_TYPE(buffer_pool_manager_, INVALID_PAGE_ID, 0, true); 
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // start from root_page
  // TODO: consider memory leak issue
  Page* curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *curr_bplus_page = reinterpret_cast<BPlusTreePage *>(curr_page->GetData());
  while (!curr_bplus_page->IsLeafPage()) {
    InternalPage *curr_internal_page = reinterpret_cast<InternalPage *>(curr_bplus_page);
    page_id_t next_page;
    if (leftMost) {
      next_page = curr_internal_page->ValueAt(0);
    } else {
      next_page = curr_internal_page->Lookup(key, comparator_);
    }
    //LOG_DEBUG("curr_internal_page %d, next_page %d", curr_bplus_page->GetPageId(), next_page);
    if (next_page == INVALID_PAGE_ID) {
      break;
    }
    buffer_pool_manager_->UnpinPage(curr_bplus_page->GetPageId(), false);
    //LOG_DEBUG("curr_internal_page %d, next_page %d", curr_bplus_page->GetPageId(), next_page);
    curr_page = buffer_pool_manager_->FetchPage(next_page);
    curr_bplus_page = reinterpret_cast<BPlusTreePage *>(curr_page->GetData());
    //LOG_DEBUG("new_curr_page %d, new_curr_blus_page %d, parent page_id %d", curr_page->GetPageId(), curr_bplus_page->GetPageId(), curr_bplus_page->GetParentPageId());
  }
  return curr_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
