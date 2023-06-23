//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
      // 使用dir_.push_back将一个新的指向Bucket类对象的std::shared_ptr类型的共享指针添加到dir_向量中
      // 这个新桶具有给定的桶大小和局部哈希前缀的位数为global_depth_，表示它是哈希表中的第一个桶
      dir_.push_back(std::make_shared<Bucket>(bucket_size, global_depth_));
    }

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock lock(latch_);
  auto index = IndexOf(key);            // 先通过 IndexOf函数 找到目录下标
  return dir_[index]->Find(key, value); // 调用Bucket的Find函数
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::unique_lock lock(latch_);
  auto index = IndexOf(key);
  return dir_[index]->Remove(key);      // 调用Bucket的Remove函数
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock lock(latch_);

  // 如果对应的桶满了的话，需要扩容。且需要一直扩容，直到对应的桶未满为止
  while(dir_[IndexOf(key)]->IsFull()) {
    // 如果对应桶的局部深度 = 全局深度，需要对目录进行扩容，分裂桶并对桶进行重新分布
    if(dir_[IndexOf(key)]->GetDepth() == global_depth_) {
      Expansion();
      RedistributeBucket(IndexOf(key));
    }
    // 如果对应桶的局部深度 < 全局深度，只需要分裂桶并对桶进行重新分布
    else {
      RedistributeBucket(IndexOf(key));
    }
  }

  // 如果对应的桶未满，直接插入即可
  dir_[IndexOf(key)]->Insert(key, value);
}

// Expansion()函数的辅助函数，用来求 idx 的前一位置1的对应的下标
// 比如 输入 001, 返回 101
template<typename K, typename V>
auto ExtendibleHashTable<K, V>::CalIndex(size_t &idx, int depth) -> size_t {
  return idx ^ 1 << (depth - 1);
}

// 辅助函数，对目录进行扩容，然后先保持桶的数量不变，对扩容的目录添加桶的指针
template<typename K, typename V>
void ExtendibleHashTable<K, V>::Expansion() {
  size_t old_size = 1 << global_depth_++;
  size_t new_size = 1 << global_depth_;
  dir_.resize(new_size);
  // 先将新扩容的目录按后 global_depth_ - 1 位来分配桶
  for(auto i = old_size; i < new_size; i++) {
    dir_.push_back(dir_[CalIndex(old_size, global_depth_)]);
  }
}

// 辅助函数，对桶进行重新分布
template<typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket(size_t idx) {
  int old_local_depth = dir_[idx]->GetDepth();
  // 低old_local_depth位 就是 old_buckt 的index
  size_t old_bucket_idx = idx & ((1 << old_local_depth) - 1);
  // 根据 old_bucket_idx 拿到 old_bucket. 将 old_bucket_idx 第一位置1,拿到 new_bucket
  auto &old_bucket = dir_[old_bucket_idx];
  auto &new_bucket = dir_[CalIndex(old_bucket_idx, old_local_depth + 1)];
  new_bucket.reset(new Bucket(bucket_size_, old_local_depth));

  // 增加新桶和旧桶的局部深度
  old_bucket->IncrementDepth();
  new_bucket->IncrementDepth();

  // 对于需要更新多条目录项，需要根据奇偶来分配指向旧桶还是新桶
  // 计算出需要更新的目录项数目 n，其中 n 等于全局深度减去原桶深度后的 2 的幂减1
  size_t diff = global_depth_ - old_local_depth;
  int n = (1 << diff) - 1;
  // 遍历需要更新的目录项，将其指向新桶或原桶，依据是目录项的编号是否为奇数，如果是，则指向新桶，否则指向原桶
  for(int i = 2; i <= n; i++) {
    size_t idx = (i << old_local_depth) + old_bucket_idx;
    dir_[idx] = static_cast<bool>(i & 1) ? new_bucket : old_bucket;
  }

  // 下面需要将桶中的元素分配到新桶和旧桶中
  // 将原桶中的所有元素移动到list中
  // 使用 std::move，因此返回的向量会被移动到一个新的对象中，而不是被拷贝。
  // 移动操作会将原始的向量置为空，因此在函数执行完毕后，旧桶中并不包含任何元素，而所有元素都被存储在了新的列表 list 中
  auto list = std::move(old_bucket->GetItems());
  // 遍历列表中的所有元素，使用 IndexOf 函数计算出它们应该插入的桶的索引，并将它们插入到哈希表中
  for(auto &[k, v] : list) {
    dir_[IndexOf(k)]->Insert(k, v);
  }

  // 更新哈希表中桶的数量 num_buckets_
  num_buckets_ ++;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::shared_lock lock(mutex_);
  for(auto &[k, v] : list_) {
    if(k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::unique_lock lock(mutex_);
  for(auto it = list_.begin(); it != list_.end(); it++) {
    if(it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::unique_lock lock(mutex_);
  for(auto &[k, v] : list_) {
    if(k == key) {
      v = value;
      return true;
    }
  }
  if(IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
