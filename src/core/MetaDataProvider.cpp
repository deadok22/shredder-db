#include <iostream>
#include <fstream>
#include <sys/stat.h>

#include "../backend/Page.h"
#include "../common/InfoPool.h"
#include "../common/Utils.h"
#include "MetaDataProvider.h"
#include "TableMetadata.pb.h"

using namespace std;


MetaDataProvider * MetaDataProvider::instance_ = new MetaDataProvider();

MetaDataProvider * MetaDataProvider::get_instance() {
  return MetaDataProvider::instance_;
}

TableMetaData * MetaDataProvider::get_meta_data(string const & struct_name) {
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

bool MetaDataProvider::save_meta_data(TableMetaData * meta_data) {
  DBInfo * db_info = InfoPool::get_instance()->get_db_info();
  //move to consts
  string table_folder = db_info->root_path + meta_data->name();
  string meta_data_path = table_folder + "/metadata";

  if (!Utils::check_existence(table_folder, true)) {
    Utils::info("[MetaDataProvider] Creating missed " + table_folder);
    mkdir(table_folder.c_str(), 0777);
  }
  fstream output(meta_data_path.c_str(), ios::out | ios::trunc | ios::binary);
  if (output == NULL || !output.is_open()) {
    Utils::error("[MetaDataProvider] Unable to open file " + meta_data_path);
    return false;
  }

  unsigned total_rec_size = 0;
  for (int attr_ind = 0; attr_ind < meta_data->attribute_size(); ++attr_ind) {
    total_rec_size += meta_data->attribute(attr_ind).size();
  }

  meta_data->set_record_size(total_rec_size);
  unsigned records_per_page = Page::PAGE_SIZE / total_rec_size; 
  unsigned space_for_bit_mask = Page::PAGE_SIZE - records_per_page * total_rec_size;
  while (8 * space_for_bit_mask < records_per_page) {
    --records_per_page;
    space_for_bit_mask += total_rec_size;
  }
  meta_data->set_space_for_bit_mask(space_for_bit_mask);
  meta_data->set_records_per_page(records_per_page);

  Utils::info("[MetaDataProvider] Store metadata under " + meta_data_path);
  meta_data->SerializeToOstream(&output);
  output.close();
  
  return true;
}