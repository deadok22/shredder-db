#pragma once
#include <vector>
#include <string>

#include "Page.h"
#include "../core/MetaDataProvider.h"

class HeapFileManager {
public:
  bool processInsertRecord(
    TableMetaData const & table,
    std::vector<std::string> const & column_names,
    std::vector<std::string> const & column_values);

  bool processDeleteRecord(/* TODO */);
  void print_all_records(TableMetaData const & table);

  static HeapFileManager & getInstance();

private:
  static HeapFileManager * instance_;

  // Page & get_page_for_insert(TableMetaData const & table);
  void print_record(TableMetaData const & table, char * data);
  int take_free_slot(char * page_data);

  std::string get_heap_file_name(std::string const & table_name);
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

