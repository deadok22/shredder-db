#include "PagesDirectory.h"
#include "BufferManager.h"
#include "../common/Utils.h"

const std::string PagesDirectory::DIRECTORY_SUFFIX("_directory");

PagesDirectory::NotEmptyPagesIterator::NotEmptyPagesIterator(PagesDirectory& directory) :
          directory_(directory),
          current_page_number_(-1),
          current_page_(0) {
}

Page & PagesDirectory::NotEmptyPagesIterator::operator*() {
  if (-1 == current_page_number_) {
    Utils::error("[PagesDirectory] [NotEmptyPagesIterator] an attempt to dereference invalid iterator");
    Utils::critical_error();
  }
  return *current_page_;
}

Page * PagesDirectory::NotEmptyPagesIterator::operator->() {
  if (-1 == current_page_number_) {
    Utils::error("[PagesDirectory] [NotEmptyPagesIterator] an attempt to dereference invalid iterator");
    Utils::critical_error();
  }
  return current_page_;
}

bool PagesDirectory::NotEmptyPagesIterator::next() {
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] [NotEmptyPagesIterator] advancing iterator");
#endif
  if (0 != current_page_) {
    current_page_->unpin();
    current_page_ = 0;
  }
  ++current_page_number_;
  //find next not empty page
  for (; directory_.pages_count_ != (size_t)current_page_number_; ++current_page_number_) {
    size_t offset = 0;
    Page & dir_page = directory_.get_directory_page_with_data_about(current_page_number_, &offset);
    //check if page is not empty
    if (0 != *(((size_t *) dir_page.get_data(true)) + offset)) {
      dir_page.unpin();
      current_page_ = &BufferManager::get_instance().get_page(current_page_number_, directory_.heap_file_name_);
#ifdef PAGE_D_DBG
      Utils::info("[PagesDirectory] [NotEmptyPagesIterator] next not empty page is page #" + std::to_string(current_page_number_));
#endif
      return true;
    }
    dir_page.unpin();
  }
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] [NotEmptyPagesIterator] iteration is finished. No more non-empty pages");
#endif
  return false;
}

PagesDirectory::PagesDirectory(std::string const & heap_file_name) :
          heap_file_name_(heap_file_name),
          directory_file_name_(heap_file_name + DIRECTORY_SUFFIX),
          pages_count_(0),
          records_per_page_(0) {
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] instantiating PagesDirectory. Heap file: " + heap_file_name_ + "; directory file: " + directory_file_name_);
#endif
  Page& first_page = BufferManager::get_instance().get_page(0, directory_file_name_);
  size_t * data = (size_t *) first_page.get_data();
  pages_count_ = *data;
  records_per_page_ = *(data + 1);
  first_page.unpin();
}

Page& PagesDirectory::get_directory_page_with_data_about(size_t page_number, size_t *offset) {
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] fetching a directory page with data about heapfile page #" + std::to_string(page_number));
#endif
  page_number += 2;
  size_t directory_page_number = page_number / (Page::PAGE_SIZE / sizeof(size_t));
  *offset = page_number % (Page::PAGE_SIZE / sizeof(size_t));
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] directory page #" + std::to_string(directory_page_number));
  Utils::info("[PagesDirectory] directory pate's entry #" + std::to_string(*offset));
#endif
  return BufferManager::get_instance().get_page(directory_page_number, directory_file_name_);
}

void PagesDirectory::increment_records_count(size_t page_number) {
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] incrementing records count on page #" + std::to_string(page_number));
#endif
  size_t offset = 0;
  Page& p = get_directory_page_with_data_about(page_number, &offset);
  ++(*(((size_t *) p.get_data(false)) + offset));
  p.unpin();
}

void PagesDirectory::decrement_records_count(size_t page_number) {
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] decrementing records count on page #" + std::to_string(page_number));
#endif
  size_t offset = 0;
  Page& p = get_directory_page_with_data_about(page_number, &offset);
  --(*(((size_t *) p.get_data(false)) + offset));
  p.unpin();
}

Page& PagesDirectory::create_new_heap_page() {
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] creating a new heap file page");
  Utils::info("[PagesDirectory] incrementing pages count in pages directory file");
#endif
  Page& first_page = BufferManager::get_instance().get_page(0, directory_file_name_);
  ++(*((size_t *) first_page.get_data(false)));
  first_page.unpin();
  ++pages_count_;
  size_t offset = 0;
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] adding a new entry to directory file");
#endif
  Page& dir_page = get_directory_page_with_data_about(pages_count_ - 1, &offset);
  *(((size_t *) dir_page.get_data(false)) + offset) = 0;
  dir_page.unpin();
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] fetching a new heap file page");
#endif
  return BufferManager::get_instance().get_page(pages_count_ - 1, heap_file_name_);
}

Page& PagesDirectory::get_page_for_insert() {
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] getting a page for insertion");
#endif
  size_t heap_file_page_number = 0;
  size_t current_directory_file_page_number = 0;
  //search for a page with an empty slot.
  for (; pages_count_ != heap_file_page_number; ++current_directory_file_page_number) {
    Page& dir_page = BufferManager::get_instance().get_page(current_directory_file_page_number, directory_file_name_);
    size_t *data = (size_t *) dir_page.get_data(true);
    size_t current_offset = 0 == current_directory_file_page_number ? 2 : 0;
    while (pages_count_ != heap_file_page_number && current_offset != Page::PAGE_SIZE / sizeof(size_t)) {
      if (*(data + current_offset) < records_per_page_) {
        dir_page.unpin();
#ifdef PAGE_D_DBG
        Utils::info("[PagesDirectory] found a heap file page for insertion:" + std::to_string(heap_file_page_number));
#endif
        return BufferManager::get_instance().get_page(heap_file_page_number, heap_file_name_);
      }
      ++current_offset;
      ++heap_file_page_number;
    }
    dir_page.unpin();
  }
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] no free space found in a heap file. Extending.");
#endif
  return create_new_heap_page();
}

std::string PagesDirectory::get_directory_file_name(std::string const & heap_file_name) {
  return heap_file_name + DIRECTORY_SUFFIX;
}

void PagesDirectory::init_directory(std::string const & heap_file_name, size_t records_per_page) {
#ifdef PAGE_D_DBG
  Utils::info("[PagesDirectory] initializing pages directory for heap file" + heap_file_name);
#endif
  Page& first_page = BufferManager::get_instance().get_page(0, get_directory_file_name(heap_file_name));
  size_t * data = (size_t *) first_page.get_data(false);
  *data = 0;
  *(data + 1) = records_per_page;
  first_page.unpin();
}

PagesDirectory::NotEmptyPagesIterator PagesDirectory::get_iterator() {
  return PagesDirectory::NotEmptyPagesIterator(*this);
}
