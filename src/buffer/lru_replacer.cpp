//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"
#include <unordered_map>
#include <list>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : lru_list(0), frame_map(0) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if (lru_list.empty()) {
        frame_id = nullptr;
        return false;
    }
    /*for (auto val : lru_list) {
        LOG_DEBUG("lru_list: frame_id %d", val);
    }*/
    std::list<frame_id_t>::iterator lru_iter = lru_list.end();
    std::advance(lru_iter, -1);
    
    *frame_id = *lru_iter;
    Pin(*frame_id);
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    if (frame_map.find(frame_id) == frame_map.end()) {
        return;
    }

    std::list<frame_id_t>::iterator list_iter = frame_map[frame_id];
    lru_list.erase(list_iter);
    frame_map.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    if (frame_map.find(frame_id) != frame_map.end()) {
        return;
    }
    lru_list.push_front(frame_id);
    frame_map[frame_id] = lru_list.begin();
}

size_t LRUReplacer::Size() {
    return lru_list.size();
}

}  // namespace bustub
