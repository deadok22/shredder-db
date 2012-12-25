#pragma once

#include <string>
#include "IndexOperationParams.h"
//in make -I option is used, so we don't need to go a dir upper
#include "TableMetadata.pb.h"

//File format
// 0th page -- meta data: root_ptr; key_size; pages_used

// node: 1st int -- stored entries type, 2nd -- number of items, 3rd - ptr to next data leaf
// for nodes ptr value ptr ... ptr value ... ptr
// for leafs: page_id:slot_id values...


class BTreeIndexManager {
public:

  BTreeIndexManager(std::string const & table_dir, std::string const & index_name);
  static void create_index(std::string const & table_name, TableMetaData_IndexMetadata const & ind_metadata);

  int look_up_value(IndexOperationParams * params);
  bool insert_value(IndexOperationParams const & params);
  bool delete_value(IndexOperationParams * params);

  unsigned get_key_size();
private:
  struct SplitNodeOpContext {
    SplitNodeOpContext(char * current_node_data_, char * child_entry_, unsigned ins_index_,
      unsigned entry_size_, unsigned entries_per_page_):
      current_node_data(current_node_data_), child_entry(child_entry_),
      ins_index(ins_index_), entry_size(entry_size_), entries_per_page(entries_per_page_) {}

    char * current_node_data;
    char * child_entry;
    unsigned ins_index;
    unsigned entry_size;
    unsigned entries_per_page;
  };
private:
  //TODO remove duplication
  static size_t compute_key_size(TableMetaData const & t_meta, TableMetaData_IndexMetadata const & i_meta);
  static void init_params_with_record(TableMetaData const & t_meta, TableMetaData_IndexMetadata const & i_meta, void * rec_data,IndexOperationParams * params);
//info retrievers
  unsigned get_records_count(char * data);
  void set_records_count(char * data, unsigned new_rec_cnt);
  unsigned get_root_node();
  unsigned get_new_page_id();
  unsigned get_int_value_by_offset(unsigned page_id, unsigned offset);

//aux helpers
  void change_root(char *entry);
  unsigned find_offset_for_storage(
      char * records_data, unsigned aux_record_data_size, IndexOperationParams const & params);
  char * split_node(SplitNodeOpContext &ctx, IndexOperationParams const & params);

  void insert_into_leaf(char * data, unsigned index, IndexOperationParams const & params);
  void insert_into_node(char * data, unsigned index, char * entry);

//core operations
  int tree_search(unsigned node_id, IndexOperationParams * params);
  void tree_insert(unsigned node_id, IndexOperationParams const & params, void *& child_entry);
private: //vars
  std::string index_file_name_;
private: //cosntants
  static size_t const DATA_PAGE_HEADER_SIZE;

  static size_t const NODE_PTR_SIZE;
  static size_t const RECORD_ID_SIZE;
  static unsigned const LEAF_TYPE;
  static unsigned const NODE_TYPE;
};