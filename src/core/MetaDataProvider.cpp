#include <iostream>
#include <fstream>

#include "../common/InfoPool.h"
#include "../common/Utils.h"
#include "MetaDataProvider.h"
#include "TableMetadata.pb.h"

using namespace std;


MetaDataProvider * MetaDataProvider::instance_ = new MetaDataProvider();

MetaDataProvider * MetaDataProvider::get_instance() {
  return MetaDataProvider::instance_;
}

TableMetaData * MetaDataProvider::get_meta_data(string struct_name) {
  DBInfo * db_info = InfoPool::get_instance()->get_db_info();
  //move to consts
  string meta_data_path = db_info->root_path + struct_name + "/metadata";
  if (!Utils::check_existence(meta_data_path, false)) { return NULL; }

  fstream input(meta_data_path.c_str(), ios::in | ios::binary);
  TableMetaData * md = new TableMetaData();
  md->ParseFromIstream(&input);
  input.close();
  return md;
}

bool MetaDataProvider::save_meta_data(TableMetaData const & meta_data) {
  DBInfo * db_info = InfoPool::get_instance()->get_db_info();
  //move to consts
  string meta_data_path = db_info->root_path + meta_data.name() + "/metadata";
  fstream output(meta_data_path.c_str(), ios::out | ios::trunc | ios::binary);
  cout << meta_data_path << endl;
  meta_data.SerializeToOstream(&output);
  output.close();
}