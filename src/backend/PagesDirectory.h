#pragma once

#include <string>
#include "Page.h"

class PagesDirectory {
public:
  class NotEmptyPagesIterator {
  public:
    NotEmptyPagesIterator(PagesDirectory& directory);
    Page & operator*();
    Page * operator->();
    bool next();
  private:
    PagesDirectory& directory_;
    ssize_t current_page_number_;
    Page * current_page_;
  };
public:
  explicit PagesDirectory(std::string const & heap_file_name);
  void increment_records_count(size_t page_number);
  void decrement_records_count(size_t page_number);
  Page& get_page_for_insert();
  NotEmptyPagesIterator get_iterator();
public:
  static std::string get_directory_file_name(std::string const & heap_file_name);
  static void init_directory(std::string const & heap_file_name, size_t records_per_page);
private:
  std::string heap_file_name_;
  std::string directory_file_name_;
  size_t pages_count_;
  size_t records_per_page_;
private:
  Page& get_directory_page_with_data_about(size_t page_number, size_t *offset);
  Page& create_new_heap_page();
private:
  static const std::string DIRECTORY_SUFFIX;
};
