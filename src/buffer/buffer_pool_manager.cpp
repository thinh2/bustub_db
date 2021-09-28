//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  if (AllPagePinned()) {
    return nullptr;
  }
  frame_id_t replace_frame = GetFreeFrame();
  //LOG_DEBUG("fetch free page page_id %d, frame_id %d", page_id, replace_frame);

  if (pages_[replace_frame].IsDirty()) {
    FlushPageImpl(pages_[replace_frame].GetPageId());
  }

  // update page_table_
  page_id_t delete_page = pages_[replace_frame].GetPageId();
  page_table_.erase(delete_page);
  page_table_[page_id] = replace_frame;

  // reset and update metadata
  pages_[replace_frame].ResetMemory();
  pages_[replace_frame].is_dirty_ = false;
  pages_[replace_frame].pin_count_ = 1;
  pages_[replace_frame].page_id_ = page_id;
  auto page_data = pages_[replace_frame].GetData();
  disk_manager_->ReadPage(page_id, page_data);
  //LOG_DEBUG("page_id is %d", page_id);
  replacer_->Pin(replace_frame);

  return &pages_[replace_frame];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    replacer_->Unpin(frame_id);
    return false;
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  pages_[frame_id].pin_count_--;
  //LOG_DEBUG("page_id %d, frame_id %d, pin_count %d", page_id, frame_id, pages_[frame_id].GetPinCount());
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  frame_id_t frame_id = page_table_[page_id];
  char *page_data = pages_[frame_id].GetData();
  disk_manager_->WritePage(page_id, page_data);
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = disk_manager_->AllocatePage();
  for (auto val : page_table_) {
    LOG_DEBUG("page_table_: page_id %d, frame_id %d", val.first, val.second);
  }

  if (AllPagePinned()) {
    return nullptr;
  }

  frame_id_t free_frame = GetFreeFrame();
  Page *victim_page = &pages_[free_frame];

  LOG_DEBUG("victim page %d, replace page %d", victim_page->GetPageId(), *page_id);
  if (victim_page->IsDirty()) {
    FlushPageImpl(victim_page->GetPageId());
  }
  page_table_.erase(victim_page->page_id_);

  victim_page->ResetMemory();
  victim_page->page_id_ = *page_id;
  victim_page->pin_count_ = 0;
  victim_page->is_dirty_ = true;
  replacer_->Pin(free_frame);
  page_table_[victim_page->page_id_] = free_frame;
  LOG_DEBUG("success swap");
  return victim_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  disk_manager_->DeallocatePage(page_id);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  free_list_.push_back(frame_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (auto page : page_table_) {
    FlushPageImpl(page.first);
  }
}

bool BufferPoolManager::AllPagePinned() {
  return (replacer_->Size() == 0 && free_list_.empty());
}

frame_id_t BufferPoolManager::GetFreeFrame() {
  frame_id_t free_frame;
  //LOG_DEBUG("free_list size %d", static_cast<int>(free_list_.size()));
  if (!free_list_.empty()) {
    free_frame = free_list_.front();
    free_list_.pop_front();
    return free_frame;
  }

  replacer_->Victim(&free_frame);
  return free_frame;
}

}  // namespace bustub
