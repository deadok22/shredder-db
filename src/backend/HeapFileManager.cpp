#include <map>
#include <iostream>

#include "../common/DataType.h"
#include "HeapFileManager.h"

HeapFileManager * HeapFileManager::instance_ = new HeapFileManager();

HeapFileManager & HeapFileManager::getInstance() {
  return *HeapFileManager::instance_;
}

bool HeapFileManager::processInsertRecord(
  TableMetaData const & table,
  std::vector<std::string> const & column_names,
  std::vector<std::string> const & column_values) {

  std::map<std::string, std::string> name_to_value;
  for (unsigned i = 0; i < column_names.size(); ++i) {
    name_to_value[column_names[i]] = column_values[i];
  }

  //determine free place for storage
  //Page page = get_page_for_insert(table);
  //char * page_data = page.get_data();
  char * page_data = dest;
  //TODO mult on record size
  int offset = 0;//take_free_slot(page_data);
  
  //save values in order
  for (int attr_ind = 0; attr_ind < table.attribute_size(); ++attr_ind) {
    std::string attr_name = table.attribute(attr_ind).name();
    int attr_size = table.attribute(attr_ind).size();

    switch ((TypeCode)table.attribute(attr_ind).type_name()) {
      case INT: {
          std::string value_to_store = name_to_value.count(attr_name) == 0 ? "0" : name_to_value[attr_name];
          int value = std::stoi(value_to_store);
          *((int *)(page_data + offset)) = value;
        }
        break;
      case DOUBLE: {
          std::string value_to_store = name_to_value.count(attr_name) == 0 ? "0" : name_to_value[attr_name];
          double value = std::stod(name_to_value[attr_name]);
          *((double *)(page_data + offset)) = value;
        }
        break;
      case VARCHAR:
        std::string value_to_store = name_to_value.count(attr_name) == 0 ? std::string(attr_size, '\0') : name_to_value[attr_name];
        memcpy(page_data + offset, value_to_store.c_str(), attr_size);
        break;
    }

    
    offset += attr_size;
  }
 // page.set_dirty();

  //relese page
  //page.unpin();

  return true;
}

/*
Page & HeapFileManager::get_page_for_insert(TableMetaData const & table) {
  //TODO
  return NULL;
}
*/

int HeapFileManager::take_free_slot(char * page_data) {
  //NB: do we need to somehow check if page really contain free slot? Current impl assumes that this has already been checked
  int slot_offset = 0;
  while (true) {
    char data_to_test = *(page_data + slot_offset);
    if (data_to_test != 0xFF) {
      for (int mask_offset = 0; mask_offset < 8; ++mask_offset) {
        if (((1 << (7 - mask_offset)) & data_to_test) == 0) {
          *(page_data + slot_offset) += 1 << (7 - mask_offset); //mark as taken
          return 8 * slot_offset + mask_offset;
        }
      }
    }
    ++slot_offset;
  }
}

bool HeapFileManager::processDeleteRecord(/* TODO */) { return false;}

void HeapFileManager::print_record(TableMetaData const & table, char * page_data) {
  int offset = 0;
  for (int attr_ind = 0; attr_ind < table.attribute_size(); ++attr_ind) {
    std::cout << table.attribute(attr_ind).name() << ": ";
    int attr_size = table.attribute(attr_ind).size();

    char char_attr_value[attr_size + 1];
    memcpy(char_attr_value, page_data + offset, attr_size);
    switch ((TypeCode)table.attribute(attr_ind).type_name()) {
      case INT: {
          int value = *((int *)char_attr_value);
          std::cout << value << "; ";
        }
        break;
      case DOUBLE: {
          double value = *((double *)char_attr_value);
          std::cout << value << "; ";
        }
        break;
      case VARCHAR: {
          char_attr_value[attr_size] = '\0';
          std::cout << "\"" << char_attr_value << "\"; ";
        }
        break;
    }
    offset += attr_size;
  }
  std::cout << std::endl;
}

//TODO temporary method for testing, remove it!
void printAllGetRecords(/* TODO */) {}