#include "PagesDirectory.h"
#include "BufferManager.h"

const std::string PagesDirectory::DIRECTORY_SUFFIX("_directory");

PagesDirectory::NotEmptyPagesIterator::NotEmptyPagesIterator(PagesDirectory& directory) :
          directory_(directory),
          current_page_number_(-1),
          current_page_(0) {
}

Page & PagesDirectory::NotEmptyPagesIterator::operator*() {
  if (-1 == current_page_number_) {
    //TODO report critical error
  }
  return *current_page_;
}

Page * PagesDirectory::NotEmptyPagesIterator::operator->() {
  if (-1 == current_page_number_) {
    //TODO report critical error
  }
  return current_page_;
}

bool PagesDirectory::NotEmptyPagesIterator::next() {
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
      return true;
    }
    dir_page.unpin();
  }
  return false;
}

PagesDirectory::PagesDirectory(std::string const & heap_file_name) :
          heap_file_name_(heap_file_name),
          directory_file_name_(heap_file_name + DIRECTORY_SUFFIX),
          pages_count_(0),
          records_per_page_(0) {
  Page& first_page = BufferManager::get_instance().get_page(0, directory_file_name_);
  size_t * data = (size_t *) first_page.get_data();
  pages_count_ = *data;
  records_per_page_ = *(data + 1);
  first_page.unpin();
}

Page& PagesDirectory::get_directory_page_with_data_about(size_t page_number, size_t *offset) {
  page_number += 2;
  size_t directory_page_number = page_number / (Page::PAGE_SIZE / sizeof(size_t));
  *offset = page_number % (Page::PAGE_SIZE / sizeof(size_t));
  return BufferManager::get_instance().get_page(directory_page_number, directory_file_name_);
}

void PagesDirectory::increment_records_count(size_t page_number) {
  size_t offset = 0;
  Page& p = get_directory_page_with_data_about(page_number, &offset);
  ++(*(((size_t *) p.get_data(false)) + offset));
  p.unpin();
}

void PagesDirectory::decrement_records_count(size_t page_number) {
  size_t offset = 0;
  Page& p = get_directory_page_with_data_about(page_number, &offset);
  --(*(((size_t *) p.get_data(false)) + offset));
  p.unpin();
}

Page& PagesDirectory::create_new_heap_page() {
  Page& first_page = BufferManager::get_instance().get_page(0, directory_file_name_);
  ++(*((size_t *) first_page.get_data(false)));
  first_page.unpin();
  ++pages_count_;
  size_t offset = 0;
  Page& dir_page = get_directory_page_with_data_about(pages_count_ - 1, &offset);
  *(((size_t *) dir_page.get_data(false)) + offset) = 0;
  dir_page.unpin();
  return BufferManager::get_instance().get_page(pages_count_ - 1, heap_file_name_);
}

Page& PagesDirectory::get_page_for_insert() {
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
        return BufferManager::get_instance().get_page(heap_file_page_number, heap_file_name_);
      }
      ++current_offset;
      ++heap_file_page_number;
    }
    dir_page.unpin();
  }
  return create_new_heap_page();
}

std::string PagesDirectory::get_directory_file_name(std::string const & heap_file_name) {
  return heap_file_name + DIRECTORY_SUFFIX;
}

void PagesDirectory::init_directory(std::string const & heap_file_name, size_t records_per_page) {
  Page& first_page = BufferManager::get_instance().get_page(0, get_directory_file_name(heap_file_name));
  size_t * data = (size_t *) first_page.get_data(false);
  *data = 0;
  *(data + 1) = records_per_page;
  first_page.unpin();
}
