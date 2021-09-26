//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *bpm, page_id_t curr_page_id_, int curr_index_, bool is_end_);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    return (this->is_end_ == itr.is_end_ && itr.curr_page_id_ == this->curr_page_id_ && itr.curr_index_ == this->curr_index_);
  }

  bool operator!=(const IndexIterator &itr) const {
    return (this->is_end_ != itr.is_end_ || itr.curr_page_id_ != this->curr_page_id_ || itr.curr_index_ != this->curr_index_);
  }

 private:
  // add your own private member variables here
  page_id_t curr_page_id_;
  int curr_index_;
  BufferPoolManager *buffer_pool_manager_;
  bool is_end_;
  MappingType iter_val_;
};

}  // namespace bustub
