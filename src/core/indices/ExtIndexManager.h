#pragma once

#include <string>
#include "IndexOperationParams.h"
#include "../../backend/Page.h"
//in make -I option is used, so we don't need to go a dir upper
#include "TableMetadata.pb.h"

// Format of files: 
// ext_hash_<attr_names> - page with index context
//    each page is a bucket. Bucket number is a page offset + bucket size. Page starts with a hash depth int + record count
//    each record: <rid> (x2 ints) + indexed data
// ext_hash_<attr_names>_directory
//    0th metadata -- total bucket depth;
//    1st.. number of items on page + ints that points on buckets


class ExtIndexManager {
public:
  ExtIndexManager(std::string const & table_dir);
  static void create_index(std::string const & table_name, TableMetaData_IndexMetadata const & metadata);

  int look_up_value(IndexOperationParams * params);
  bool insert_value(IndexOperationParams const & params);
  bool delete_value(IndexOperationParams * params);

#ifndef TEST_EXT_IND
private: //methods
#endif
  static size_t compute_key_size(TableMetaData const & t_meta, TableMetaData_IndexMetadata const & i_meta);
  static void init_params_with_record(TableMetaData const & t_meta, TableMetaData_IndexMetadata const & i_meta, void * rec_data,IndexOperationParams * params);
  static void init_buckets(std::string const & index_file_name, unsigned from, unsigned till, unsigned depth);
  bool bucket_has_free_slot(Page * bucket_id, unsigned record_size);
  void split_bucket(unsigned bucket_number, Page * page, unsigned record_size);
  void double_buckets_count();
  unsigned compute_hash(IndexOperationParams const & params);
  unsigned get_bucket_id(unsigned bucket_ptr);
  void add_ptr_to_index_dir(unsigned bucket_id, unsigned total);

private: //classes
  class BucketPointersIterator {
  public:
    BucketPointersIterator(std::string dir_file_name, bool init_mode = false);
    ~BucketPointersIterator();

    bool next();
    unsigned get_current_hash();
    int & operator*();
    int * operator->();
  private:
    std::string dir_file_name_;
    unsigned records_count_;

    bool is_init_mode_;
    Page *current_page_;
    unsigned records_to_go_;
    unsigned offset_;

    unsigned current_hash_;
  };

private: //vars and consts
  static std::string const DIR_SUFFIX;
  static unsigned const INIT_BUCKET_DEPTH;
  static size_t const DIR_PAGE_AUX_DATA_SIZE;
  static size_t const DIR_REC_SIZE;

  static size_t const PAGE_AUX_DATA_SIZE;
  static size_t const RECORD_ID_SIZE;

  std::string index_path_;

};

#ifdef TEST_EXT_IND
#include <iostream>

void test_hash_computing() {
  IndexOperationParams params;
  params.value_size = 5;
  char value_t[5] = {1, 0, 0, 0, 16};
  params.value = value_t;
  ExtIndexManager eim("");

  eim.compute_hash(params);
}

int main(int, char **) {
  test_hash_computing();
}

#endif
