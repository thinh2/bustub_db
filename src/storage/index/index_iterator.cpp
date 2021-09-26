/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t leaf_page_id_, int idx, bool is_end_) : 
                          curr_page_id_(leaf_page_id_),
                          curr_index_(idx),
                          buffer_pool_manager_(bpm),
                          is_end_(is_end_) {};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  return is_end_;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (is_end_) {
    throw std::runtime_error("invalid operator");
  }

  auto page = buffer_pool_manager_->FetchPage(curr_page_id_);
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  iter_val_ = leaf_page_->GetItem(curr_index_);
  buffer_pool_manager_->UnpinPage(curr_page_id_, false);
  return iter_val_;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (is_end_) {
    return *this;
  }
  auto page = buffer_pool_manager_->FetchPage(curr_page_id_);
  page_id_t unpin_page_id_ = curr_page_id_;
  BPlusTreePage *bplus_page_ = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (!bplus_page_->IsLeafPage()) {
    throw std::runtime_error("not a bplus leaf page");
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bplus_page_);
  curr_index_++;
  if (curr_index_ == leaf_page_->GetSize()) {
    curr_index_ = 0;
    curr_page_id_ = leaf_page_->GetNextPageId();
    if (curr_page_id_ == INVALID_PAGE_ID) {
      is_end_ = true;
    }
  }
  buffer_pool_manager_->UnpinPage(unpin_page_id_, false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
