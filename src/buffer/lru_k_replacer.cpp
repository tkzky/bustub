//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <exception>
#include <mutex>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/*
* 驱逐页面：
* 1. 判断是否有可驱逐页面，如果没有直接返回 false
* 2. 先从history_list_中采用尾删法，判断页面是否可驱逐，可驱逐就尾删
* 3. 否则从 cache_list_中采用尾删法，判断页面是否可驱逐，可驱逐就尾删
* 注：删除一个页面之后需要更新该页面id的 access_count_ 为 0, curr_size_--, is_evictable_为false, 清空对应页面的map
*/
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);

    if(curr_size_ == 0) {
        return false;
    }

    for(auto it = history_list_.rbegin(); it != history_list_.rend(); it++) {
        auto frame = *it;
        if(is_evictable_[frame]) {
            access_count_[frame] = 0;
            curr_size_--;
            is_evictable_[frame] = false;
            history_list_.erase(history_map_[frame]);
            history_map_.erase(frame);
            *frame_id = frame;
            return true;
        }
    }

    for(auto it = cache_list_.rbegin(); it != cache_list_.rend(); it++) {
        auto frame = *it;
        if(is_evictable_[frame]) {
            access_count_[frame] = 0;
            curr_size_--;
            is_evictable_[frame] = false;
            cache_list_.erase(cache_map_[frame]);
            cache_map_.erase(frame);
            *frame_id = frame;
            return true;
        }
    }

    return false; 
}

/*
* 记录给定页面ID被访问
* 1. 判断页面是否越界，如果越界直接返回
* 2. 页面的访问次数++
* 3. 将页面的访问次数和 k 进行比较：
*   - 访问次数 < k : 
*       - 如果第一次访问该页面，就加入 history_list_ 和 history_map_;
*       - 如果不是第一次访问该页面，由于 history_list_ 采用 FIFO，所以不需要做什么
*   - 访问次数 == k : 将该页面从 history_list_ 中删除，头插到 cache_list_
*   - 访问次数 > k : cache_list_ 采用 LRU, 需要将该页面重新头插到 cache_list_
*/
void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    if(frame_id > static_cast<int>(replacer_size_)) {
        throw std::exception();
    }

    access_count_[frame_id]++;

    if(access_count_[frame_id] < k_) {
        if(history_map_.count(frame_id) == 0) {
            history_list_.push_front(frame_id);
            history_map_[frame_id] = history_list_.begin();
        }
    }
    else if(access_count_[frame_id] == k_) {
        history_list_.erase(history_map_[frame_id]);
        history_map_.erase(frame_id);
        cache_list_.push_front(frame_id);
        cache_map_[frame_id] = cache_list_.begin();
    }
    else {
        cache_list_.erase(cache_map_[frame_id]);
        cache_map_[frame_id] = cache_list_.begin();
    }
}

/*
* 设置页面是否可以被驱逐
* 1. 判断页面是否越界
* 2. 如果页面不存在，直接返回
* 3. 判断页面之前的状态 is_evictable_ 和 函数入参set_evictable :
*   - 如果页面可被驱逐 且 set_evictable 为 false 则设置该页面不可被驱逐, 同时更新记录页面是否可被驱逐的哈希 is_evictable_ 和 curr_size_
*   - 如果页面不可被驱逐 且 set_evictable 为 true 则设置该页面可以被驱逐, 同时更新一些参数 
*/
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);

    if(frame_id > static_cast<int>(replacer_size_)) {
        throw std::exception();
    }

    if(is_evictable_[frame_id] && !set_evictable) {
        is_evictable_[frame_id] = false;
        curr_size_--;
    }
    if(!is_evictable_[frame_id] && set_evictable) {
        is_evictable_[frame_id] = true;
        curr_size_--;
    }
}

/*
* 删除一个页面
* 1. 如果页面越界，抛出异常
* 2. 如果页面不存在(即 access_count_[frame_id] 为 0 )，则直接返回
* 3. 如果该页面不能被删除，抛出异常
* 4. 根据该页面的被访问次数(即 access_count_[frame_id])判断在哪个队列，删除即可
*/
void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);

    if(frame_id > static_cast<int>(replacer_size_)) {
        throw std::exception();
    }

    auto cnt = access_count_[frame_id];

    if(cnt == 0) {
        return;
    }
    if(is_evictable_[frame_id] == false) {
        throw std::exception();
    }
    if(cnt < k_) {
        history_list_.erase(history_map_[frame_id]);
        history_map_.erase(frame_id);
    }
    else {
        cache_list_.erase(cache_map_[frame_id]);
        cache_map_.erase(frame_id);
    }
    is_evictable_[frame_id] = false;
    access_count_[frame_id] = 0;
    curr_size_--;
}

/*
* 返回可驱逐的页面数
*/
auto LRUKReplacer::Size() -> size_t {
    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_;
 }

}  // namespace bustub
