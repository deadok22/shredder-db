#pragma once

#include <string>
#include <vector>
#include <unordered_map>

#include "TableMetadata.pb.h"

class MetaDataProvider {
public:
  static TableMetaData * get_meta_data(std::string const & table_name);
  static bool save_meta_data(TableMetaData * meta_data);
  static bool add_index_info(std::string const & table_name, TableMetaData_IndexMetadata const & ind_metadata);
  static MetaDataProvider * get_instance();
private:
  static MetaDataProvider * instance_;
  static std::unordered_map<std::string, TableMetaData *> * cache_;
  static unsigned MAX_CACHE_SIZE;
};

#ifdef TEST_MDP
#include <iostream>

int main() {
  DBInfo di;
  di.root_path = "";
  InfoPool::get_instance().set_db_info(di);

  TableMetaData md;
  md.set_name("test_table");
  TableMetaData_AttributeDescription *attr;
  attr = md.add_attribute();
  
  attr->set_name("id");
  attr->set_type_name("vchar");

  MetaDataProvider::get_instance()->save_meta_data(md);  

  TableMetaData *md1;
  md1 = MetaDataProvider::get_instance()->get_meta_data("test_table");    
  cout << md1->attribute(0).name() << endl;
}

#endif
