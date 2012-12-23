#pragma once
#include <vector>
#include <string>

#include "Page.h"
#include "../core/MetaDataProvider.h"

class Filter { //mock filter, don't forget copy ctr
public:
  bool isOk(TableMetaData const & table, void * data) const { return true; }
};

class HeapFileManager {
public://class
  class RecordsIterator {
  public:
    RecordsIterator(TableMetaData const & table, Filter const & filter);
    bool next();
    unsigned rec_page_id();
    unsigned rec_slot_id();
    void * rec_data();
  private:
    bool switch_page();
    TableMetaData const & t_meta_;
    Filter const & filter_;

    char * records_data_;
    char * page_data_;
    int current_slot_id_;

    PagesDirectory pd;
    PagesDirectory::NotEmptyPagesIterator page_itr_;

  };


public:
  bool process_insert_record(
    TableMetaData const & table,
    std::vector<std::string> const & column_names,
    std::vector<std::string> const & column_values);

  bool process_delete_record(/* TODO */);
  void print_all_records(TableMetaData const & table);

  //returns a pointer to data related to a given record. If there is no such recprd returns NULL
  void * get_record(TableMetaData const & table, unsigned page_id, unsigned slot_id);

  int get_int_attr(void * data, TableMetaData const & table, std::string const & attr_name);
  double get_double_attr(void * data, TableMetaData const & table, std::string const & attr_name);
  std::string get_vchar_attr(void * data, TableMetaData const & table, std::string const & attr_name);

  static HeapFileManager & get_instance();

private:
  static HeapFileManager * instance_;

  // Page & get_page_for_insert(TableMetaData const & table);
  void print_record(TableMetaData const & table, char * data);
  int take_free_slot(char * page_data);
  char * get_attr_value(void * data, TableMetaData const & table, std::string const & attr_name);

  static std::string get_heap_file_name(std::string const & table_name);
};

#ifdef TEST_HFM
#include <vector>
#include <assert.h>

void test_io() {
  HeapFileManager &hfm = HeapFileManager::getInstance();

  TableMetaData md;
  md.set_name("test_table");
  TableMetaData_AttributeDescription *attr;
  attr = md.add_attribute();
  attr->set_name("id");
  attr->set_type_name(2);
  attr->set_size(4);
  attr = md.add_attribute();
  attr->set_name("age");
  attr->set_type_name(0);
  attr->set_size(4);
  attr = md.add_attribute();
  attr->set_name("weight");
  attr->set_type_name(1);
  attr->set_size(8);

  std::vector<std::string> n;
  n.push_back("id");
  n.push_back("age");
  n.push_back("weight");
  std::vector<std::string> v;
  v.push_back("data");
  v.push_back("42");
  v.push_back("3.14");

  char storage[300];
  hfm.processInsertRecord(md, storage, n, v);
  hfm.print_record(md, storage);


  TableMetaData md1;
  md1.set_name("test_table");
  TableMetaData_AttributeDescription *attr1;
  attr1 = md1.add_attribute();
  attr1->set_name("id");
  attr1->set_type_name(2);
  attr1->set_size(4);
  attr1 = md1.add_attribute();
  attr1->set_name("age");
  attr1->set_type_name(0);
  attr1->set_size(4);
  attr1 = md1.add_attribute();
  attr1->set_name("weight");
  attr1->set_type_name(1);
  attr1->set_size(8);

  n.clear();
  n.push_back("id");
  n.push_back("weight");
  v.clear();
  v.push_back("long long data");
  v.push_back("3.14");

  hfm.processInsertRecord(md, storage, n, v);
  hfm.print_record(md, storage);
}

void test_free_pick() {
  HeapFileManager &hfm = HeapFileManager::getInstance();

  char meta = 0;
  assert(hfm.take_free_slot(&meta) == 0);
  meta = 0xF0;
  assert(hfm.take_free_slot(&meta) == 4);
  meta = 0x80;
  assert(hfm.take_free_slot(&meta) == 1);
  meta = 0xFE;
  assert(hfm.take_free_slot(&meta) == 7);
  
  char metas[1];
  metas[0] = 0xFF;
  metas[1] = 0xF0;
  assert(hfm.take_free_slot(metas) == 12);
}

int main(int, char **) {
  test_io();
  test_free_pick();

  return 0;
}

#endif

