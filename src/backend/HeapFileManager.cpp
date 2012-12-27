#include <map>
#include <iostream>

#include "../common/DataType.h"
#include "../common/InfoPool.h"
#include "../common/Utils.h"
#include "PagesDirectory.h"
#include "BufferManager.h"
#include "HeapFileManager.h"

HeapFileManager * HeapFileManager::instance_ = new HeapFileManager();

HeapFileManager & HeapFileManager::get_instance() {
  return *HeapFileManager::instance_;
}

bool HeapFileManager::process_insert_record(
  TableMetaData const & table,
  std::vector<std::string> const & column_names,
  std::vector<std::string> const & column_values,
  HeapFMOperationResult *result_ctx) {

  std::map<std::string, std::string> name_to_value;
  for (unsigned i = 0; i < column_names.size(); ++i) {
    name_to_value[column_names[i]] = column_values[i];
  }

  //determine free place for storage
  PagesDirectory pd(get_heap_file_name(table.name()));
  Page & page = pd.get_page_for_insert();
  char * page_data = page.get_data();

  int slot_number = take_free_slot(page_data);
#ifdef HFM_DBG
  Utils::info("[HeapFileManager] Record slot for insert is " + std::to_string(slot_number) +
     ". Table: " + table.name() + "; PageId: " + std::to_string(page.get_pid()));
#endif
  page_data += slot_number * table.record_size() + table.space_for_bit_mask();
  char * new_record_ptr = page_data;
  //save values in order
  int offset = 0;
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
        memset(page_data + offset, '\0', attr_size); //must mem set since value is used in hash counting
        memcpy(page_data + offset, value_to_store.c_str(), 1 + value_to_store.size());
        *(page_data + offset + attr_size) = '\0';
        break;
    }

    offset += attr_size;
  }

  if (result_ctx != NULL) {
    memcmp(result_ctx->record_data, new_record_ptr, table.record_size());
    result_ctx->record_page_id = page.get_pid();
    result_ctx->record_slot_id = slot_number;
  }

  page.set_dirty();
  page.unpin();   //relese ASAP
#ifdef HFM_DBG
  Utils::info("[HeapFileManager] Insertion finished. Unpin page #" + std::to_string(page.get_pid()));
#endif
  pd.increment_records_count(page.get_pid());

  return true;
}

std::string HeapFileManager::get_heap_file_name(std::string const & table_name) {
  DBInfo & db_info = InfoPool::get_instance().get_db_info();
  return db_info.root_path + table_name + "/data";
}

int HeapFileManager::take_free_slot(char * page_data) {
  //NB: do we need to somehow check if page really contain free slot? Current impl assumes that this has already been checked
  int slot_offset = 0;
  while (true) {
    char data_to_test = *(page_data + slot_offset);
    //RW: magic constant
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

bool HeapFileManager::process_delete_record(TableMetaData const & table, unsigned page_id, unsigned slot_number) {
  PagesDirectory pd(get_heap_file_name(table.name()));  
  BufferManager &bm = BufferManager::get_instance();
  Page &req_page = bm.get_page(page_id, get_heap_file_name(table.name()));
  char * data = req_page.get_data(false);
// ???? check
  *(data + slot_number / 8)  &= (0xFF - (1 << (7-slot_number % 8) ) ); 
  
  req_page.unpin();
#ifdef HFM_DBG
  Utils::info("[HeapFileManager] Delete record finished. Unpin page #" + std::to_string(req_page.get_pid()));
#endif  
  pd.decrement_records_count(req_page.get_pid());
  return true;
}

bool HeapFileManager::process_update_record(
  TableMetaData const & table,
  unsigned page_id, unsigned slot_number,
  std::vector<std::string> const & column_names,
  std::vector<std::string> const & column_values) {

  std::map<std::string, std::string> name_to_value;
  for (unsigned i = 0; i < column_names.size(); ++i) {
    name_to_value[column_names[i]] = column_values[i];
  }

  BufferManager &bm = BufferManager::get_instance();
  Page & page =  bm.get_page(page_id, get_heap_file_name(table.name()));
  char * page_data = page.get_data(false);

#ifdef HFM_DBG
  Utils::info("[HeapFileManager] Record slot for update is " + std::to_string(slot_number) +
     ". Table: " + table.name() + "; PageId: " + std::to_string(page_id));
#endif
  page_data += slot_number * table.record_size() + table.space_for_bit_mask();
  
  //update values in order
  int offset = 0;
  for (int attr_ind = 0; attr_ind < table.attribute_size(); ++attr_ind) {
    std::string attr_name = table.attribute(attr_ind).name();
    int attr_size = table.attribute(attr_ind).size();

    switch ((TypeCode)table.attribute(attr_ind).type_name()) {
      case INT: {
          if( name_to_value.count(attr_name) ) {
            *((int *)(page_data + offset)) = std::stoi(name_to_value[attr_name]);
          }
        }
        break;
      case DOUBLE: {
          if( name_to_value.count(attr_name) ) {
            *((double *)(page_data + offset)) = std::stod(name_to_value[attr_name]);
          }
        }
        break;
      case VARCHAR: {
          if( name_to_value.count(attr_name) ) {
            memset(page_data + offset, '\0', attr_size);
            memcpy(page_data + offset, name_to_value[attr_name].c_str(), 1 + name_to_value[attr_name].size());
            *(page_data + offset + attr_size) = '\0';
          }
        }
        break;
    }

    offset += attr_size;
  }

  page.unpin();   //relese ASAP
#ifdef HFM_DBG
  Utils::info("[HeapFileManager] Update finished. Unpin page #" + std::to_string(page.get_pid()));
#endif
  
  return true;
}

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

void * HeapFileManager::get_record(TableMetaData const & table, unsigned page_id, unsigned slot_number) {
  BufferManager &bm = BufferManager::get_instance();
  Page &req_page = bm.get_page(page_id, get_heap_file_name(table.name()));

  //NB check if something is recorded?
  char * data = new char[table.record_size()]();
  memcpy(data, (char *)req_page.get_data() + slot_number * table.record_size() + table.space_for_bit_mask(), table.record_size());

  req_page.unpin();
  return data;
}

char * HeapFileManager::get_attr_value(void * data, TableMetaData const & table, std::string const & attr_name) {
  unsigned offset = 0;
  for (int attr_ind = 0; attr_ind < table.attribute_size(); ++attr_ind) {
    unsigned attr_size = table.attribute(attr_ind).size();
    if (table.attribute(attr_ind).name().compare(attr_name) == 0) {
      return (char *)data + offset;
    }
    offset += attr_size;
  }
  Utils::warning("[HFM] Unable to get data for " + attr_name + " attribute");
  return NULL;
}

//TODO remove code duplication
int HeapFileManager::get_int_attr(void * data, TableMetaData const & table, std::string const & attr_name) {
  char * attr_data = get_attr_value(data, table, attr_name);
  return attr_data ? *((int *) attr_data) : 0;
}

double HeapFileManager::get_double_attr(void * data, TableMetaData const & table, std::string const & attr_name) {
  char * attr_data = get_attr_value(data, table, attr_name);
  return attr_data ? *((double *) attr_data) : 0;
}

std::string HeapFileManager::get_vchar_attr(void * data, TableMetaData const & table, std::string const & attr_name) {
  char * attr_data = get_attr_value(data, table, attr_name);
  return attr_data ? attr_data : "";
}

void HeapFileManager::print_all_records(TableMetaData const & table) {
  HeapRecordsIterator records_itr(table.name());
  unsigned total = 0;
  while (records_itr.next()) {
    print_record(table, (char *)*records_itr);
    ++total;
  }
  std::cout << "Select finished. Total " << total << " records" << std::endl;
}

//-----------------------------------------------------------------------------
// Records iterator

HeapFileManager::HeapRecordsIterator::HeapRecordsIterator(std::string const & table_name):
  t_meta_(NULL), records_data_(NULL), page_data_(NULL), current_slot_id_(0),
  pd(PagesDirectory(get_heap_file_name(table_name))), page_itr_(pd.get_iterator()) {

  t_meta_ = MetaDataProvider::get_instance()->get_meta_data(table_name);
}

HeapFileManager::HeapRecordsIterator::~HeapRecordsIterator() { }


bool HeapFileManager::HeapRecordsIterator::switch_page() {
  if (!page_itr_.next()) {
#ifdef HFM_DBG
    Utils::info("  [HFM][RecordsItr] Current page was last. Stop iteration");
#endif
    return false;
  }
  
  page_data_ = page_itr_->get_data();
  records_data_ = page_data_ + t_meta_->space_for_bit_mask();
  current_slot_id_ = 0;
#ifdef HFM_DBG
  Utils::info("  [HFM][RecordsItr] Page with records was switched. New page has id " + std::to_string(page_itr_->get_pid()));
#endif
  return true;
}

bool HeapFileManager::HeapRecordsIterator::next() {
  if (current_slot_id_ == t_meta_->records_per_page() && page_data_ == NULL) {
    return false;
  }

  while (true) {
    ++current_slot_id_;
    records_data_ += t_meta_->record_size();
    if (current_slot_id_ == t_meta_->records_per_page()) { page_data_ = NULL; }
    if (page_data_ == NULL && !switch_page()) {
      return false;
    }

    unsigned bm_byte = current_slot_id_ / 8;
    unsigned bm_bit = 1 << (7 - current_slot_id_ % 8);
    if ((*(page_data_ + bm_byte) & bm_bit) > 0) {
#ifdef HFM_DBG
      Utils::info("  [HFM][RecordsItr] Record in slot " + std::to_string(current_slot_id_) + " was found");
#endif
      return true;
    }
  }

}

unsigned HeapFileManager::HeapRecordsIterator::record_page_id() { return page_itr_->get_pid(); }
unsigned HeapFileManager::HeapRecordsIterator::record_slot_id() { return current_slot_id_; }
void * HeapFileManager::HeapRecordsIterator::operator*() { return records_data_; }
