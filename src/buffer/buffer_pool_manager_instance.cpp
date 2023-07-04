//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>
#include <cstdlib>
#include <future>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
/*
* 在缓冲池中创建一个新的页面, 如果创建成功返回pageid, 失败返回 nullptr:
* 首先从 free_list_ 中找, 如果 free_list_ 不为空，就分配
* 如果 free_list_ 不为空，就从 replacer 中选择一个页面驱逐，如果该页面为脏页，需要刷到磁盘中
* call the AllocatePage() 得到一个 pageid 之后，需要相关的配置操作: page Extendiblehash LRUKReplace 相关内容
*/
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
  std::scoped_lock<std::mutex> lock(latch_);

  Page* page = nullptr;
  int frame_id = -1;
  // 先从 free_list_ 中找
  if(!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
  }
  // 如果 free_list_ 满了，就尝试利用 LRUK 替换算法驱逐出去
  else if(replacer_->Evict(&frame_id)) {
    page = pages_ + frame_id;
    page_table_->Remove(page->GetPageId()); // ExtendibleHash 来记录 pageid 和 frame 对应关系
    // 如果从缓冲池中驱逐的页是脏页，需要刷回磁盘
    if(page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->data_);
    }
  }

  // 如果成功申请到page, 更新page内容和buffer_poll的参数
  if(page != nullptr) {
    ResetPage(page);
    page->page_id_ = AllocatePage();
    *page_id = page->page_id_;
    page->pin_count_++;
    page_table_->Insert(page->page_id_, frame_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }

  return page;
 }

/*
* 申请一个已知 page_id 的页面，如果缓存池有该页面，直接返回该页面；如果没有就从内存读取
* 1. 首先从缓存池中利用 page_table_ 进行查找，如果找到直接返回即可
* 2. 如果缓存池中没有，就需要从磁盘读取：
*   - 优先从free_list_中找，如果free_list_有空位，就从磁盘读取该页并返回
*   - 如果free_list_没有空位，就利用 LRUK 算法驱逐缓存池中的某一页，然后从磁盘读取并返回;
*     如果缓存池中没有页面可以被驱逐，就返回 nullptr
*/
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  assert(page_id != INVALID_PAGE_ID);
  std::scoped_lock<std::mutex> lock(latch_);
  Page* page = nullptr;
  frame_id_t frame_id = -1;
  // 先从缓存池中找
  if(page_table_->Find(page_id, frame_id)) {
    page = pages_ + frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page->pin_count_++;
    return page;
  }

  // 如果没有找到，则从 free_list_ 中找
  if(!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
  }
  // 如果 free_list_ 为空，就需要从 bufferpool 中驱逐一个页面，如果该页为脏页，需要写回磁盘
  else if(replacer_->Evict(&frame_id)) {
    page = pages_ + frame_id;
    page_table_->Remove(frame_id);
    if(page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->data_);
      ResetPage(page);
    }
  }

  if(page != nullptr) {
    // 从磁盘中读取页面
    disk_manager_->ReadPage(page_id, page->data_);
    page->pin_count_++;
    page->page_id_ = page_id;

    page_table_->Insert(page_id, frame_id);

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }

  return page;
}

/*
* Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
* 0, return false.
* Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
* Also, set the dirty flag on the page to indicate if the page was modified.
*/
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  
  // if page_id is not in the buffer pool
  if(!page_table_->Find(page_id, frame_id)) {
    return false;
  }

  // if page_id's pin count is already 0
  Page* page = pages_ + frame_id;
  if(page->pin_count_ <= 0) {
    return false;
  }

  if(is_dirty) {
    page->is_dirty_ = true;
  }
  --page->pin_count_;
  if(page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
 }

/**
* @brief Flush the target page to disk.
* Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
* Unset the dirty flag of the page after flushing.
*
* @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
* @return false if the page could not be found in the page table, true otherwise
*/
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;
  // if the page doesn't in buffer pool
  if(page_table_->Find(page_id, frame_id)) {
    return false;
  }

  Page* page = pages_ + frame_id;
  // Use the DiskManager::WritePage() method to flush a page to disk
  disk_manager_->WritePage(page_id, page->data_);
  // Unset the dirty flag of the page after flushing
  page->is_dirty_ = false;
  
  return true;
 }

/*
* Flush all the pages in the buffer pool to disk.
*/
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for(size_t i = 0; i < pool_size_; i++) {
    Page* page = pages_ + i;
    if(page->page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

/**
* @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
* page is pinned and cannot be deleted, return false immediately.
* After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
* back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
* imitate freeing the page on the disk.
*
* @param page_id id of page to be deleted
* @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
*/
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;

  // If page_id is not in the buffer pool, do nothing and return true
  if(!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  Page* page = pages_ + frame_id;
  // If the page is pinned and cannot be deleted, return false immediately
  if(page->pin_count_ > 0) {
    return false;
  }

  // After deleting the page from the page table, stop tracking the frame in the replacer and 
  // add the frame back to the free list
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);

  // reset the page's memory and metadata
  ResetPage(page);
  // Finally, you should call DeallocatePage() to imitate freeing the page on the disk
  DeallocatePage(page_id);
  return true;
 }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
