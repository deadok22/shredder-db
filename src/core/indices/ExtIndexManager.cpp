#include <fstream>

#include "ExtIndexManager.h"
#include "../../backend/BufferManager.h"
#include "../../common/Utils.h"
#include "TableMetadata.pb.h"
#include "../MetaDataProvider.h"
#include "../../backend/HeapFileManager.h"
#include "../../backend/Page.h"

std::string const ExtIndexManager::DIR_SUFFIX("_directory");
unsigned const ExtIndexManager::INIT_BUCKET_DEPTH = 3;

size_t const ExtIndexManager::DIR_PAGE_AUX_DATA_SIZE = sizeof(int);
size_t const ExtIndexManager::DIR_REC_SIZE = sizeof(int);

//One int int for depth, another is for number items in bucket
size_t const ExtIndexManager::PAGE_AUX_DATA_SIZE = 2 * sizeof(int);
//related to heapfile. Id is <page_ind, slot_ind>
size_t const ExtIndexManager::RECORD_ID_SIZE = 2 * sizeof(int);

ExtIndexManager::BucketPointersIterator::BucketPointersIterator(std::string dir_file_name, bool init_mode): 
  dir_file_name_(dir_file_name), is_init_mode_(init_mode), current_hash_(-1) {

  BufferManager &bm = BufferManager::get_instance();
  current_page_= &(bm.get_page(0, dir_file_name_));
  records_count_ = 1 << *((int *)current_page_->get_data());
  records_to_go_ = 0;
}

ExtIndexManager::BucketPointersIterator::~BucketPointersIterator() {
  if (current_page_ != NULL) { current_page_->unpin(); }
}

bool ExtIndexManager::BucketPointersIterator::next() {
  if (current_hash_ == records_count_) { 
    if (current_page_ && current_page_->get_pid() != 0) {
#ifdef EIM_DBG
      Utils::info("[EIM][BPtr Iterator] Set bucket size to " + std::to_string(Page::PAGE_SIZE / sizeof(int) - 1 - records_to_go_) +
        " for bucket #" + std::to_string(current_page_->get_pid()));
#endif
      *((int *)current_page_->get_data(false)) = (Page::PAGE_SIZE / sizeof(int)) - 1 - records_to_go_;
      current_page_->unpin();
    }
    return false;
  }
  
  if (records_to_go_ > 0) {
    --records_to_go_;
    ++offset_;
  } else {
#ifdef EIM_DBG
    Utils::info("[EIM][BPtr Iterator] Switch to next page ");
#endif
    if (is_init_mode_ && current_page_->get_pid() != 0) {
#ifdef EIM_DBG
      Utils::info("[EIM][BPtr Iterator] Set bucket size to " + std::to_string(Page::PAGE_SIZE / sizeof(int) - 1) + " for bucket #" + std::to_string(current_page_->get_pid()));
#endif
      *((int *)current_page_->get_data(false)) = (Page::PAGE_SIZE / sizeof(int)) - 1;
    }
    current_page_->unpin();
    BufferManager &bm = BufferManager::get_instance();
    current_page_ = &(bm.get_page(current_page_->get_pid() + 1, dir_file_name_));
    records_to_go_ = is_init_mode_ ? (Page::PAGE_SIZE / sizeof(int)) - 1 : *((int *)current_page_->get_data());
    offset_ = 1;   
  }
  ++current_hash_;
  return true;
}

unsigned ExtIndexManager::BucketPointersIterator::get_current_hash() { return current_hash_; }

int & ExtIndexManager::BucketPointersIterator::operator*() {
  return *((int *)current_page_->get_data(false) + offset_);
}

int * ExtIndexManager::BucketPointersIterator::operator->() {
  return ((int *)current_page_->get_data(false) + offset_);
}

//-----------------------------------------------------------------------------
// ExtIndetManager implementation

ExtIndexManager::ExtIndexManager(std::string const & table_name, std::string const & index_name):
 index_path_(Utils::get_table_dir(table_name) + "/ext_hash_" + index_name) {}

void ExtIndexManager::create_index(std::string const & table_name, TableMetaData_IndexMetadata const & ind_metadata) {
  std::string path = Utils::get_table_dir(table_name);
  std::string index_file_name = path + "/ext_hash_" + ind_metadata.name();
  std::string directory_file_name = index_file_name + ExtIndexManager::DIR_SUFFIX;
  
  //creates two files
  std::fstream directory_file(directory_file_name.c_str(), ios::in | ios::binary);
  directory_file.close();
  std::fstream index_file(index_file_name.c_str(), ios::in | ios::binary);
  index_file.close();

  TableMetaData * t_metadata = MetaDataProvider::get_instance()->get_meta_data(table_name);
  unsigned key_size = IndexManager::compute_key_size(*t_metadata, ind_metadata);

  BufferManager &bm = BufferManager::get_instance();
  Page &fst_dir_page = bm.get_page(0, directory_file_name);
  //markup directory
  *((unsigned *)fst_dir_page.get_data(false)) = ExtIndexManager::INIT_BUCKET_DEPTH;
  *((unsigned *)fst_dir_page.get_data(false) + 1) = key_size;
  fst_dir_page.unpin();

  BucketPointersIterator bpi(directory_file_name, true);
  while (bpi.next()) { *bpi = bpi.get_current_hash(); }
  init_buckets(index_file_name, 0, 1 << INIT_BUCKET_DEPTH, INIT_BUCKET_DEPTH);

  //insert records
  
  IndexOperationParams params;
  params.value_size = key_size;
  params.value = new char[params.value_size]();
#ifdef EIM_DBG
  Utils::info("[EIM][Create index] Key size was determined to be " + std::to_string(params.value_size));
  Utils::info("[EIM][Create index] Insert values into index... ");
#endif

  HeapFileManager::HeapRecordsIterator rec_itr(t_metadata->name());
  ExtIndexManager mock_manager(table_name, ind_metadata.name());
  while (rec_itr.next()) {
    params.page_id = rec_itr.record_page_id();
    params.slot_id = rec_itr.record_slot_id();
    IndexManager::init_params_with_record(*t_metadata, ind_metadata, *rec_itr, &params);
    mock_manager.insert_value(params);
  }

  delete [] (char *)params.value;
}

int ExtIndexManager::look_up_value(IndexOperationParams * params) {
  unsigned bucket_id = get_bucket_id(compute_hash(*params));

  Page &page = BufferManager::get_instance().get_page(bucket_id, index_path_);
  unsigned occupied = get_records_count(page.get_data());
  if (occupied == 0) {
    page.unpin();
    return -1;
  }

  char * data = page.get_data() + PAGE_AUX_DATA_SIZE;
  unsigned bucket_record_index = 0;
  while (bucket_record_index < occupied) {
    char *key_data = data + RECORD_ID_SIZE;
    //TODO replace with comparator
    if (memcmp(key_data, params->value, params->value_size) == 0) {
      //BINGO
      params->page_id = *((int *)data);
      params->slot_id = *((int *)data + 1);
#ifdef EIM_DBG
      Utils::info("  [EIM][Look up] Found record with id " + std::to_string(params->page_id) + ":" + std::to_string(params->slot_id));
#endif
      break;
    }

    //2 ints is a record id
    data += (RECORD_ID_SIZE + params->value_size);
    ++bucket_record_index;
  }
  page.unpin();
  return bucket_record_index == occupied ? -1 : bucket_record_index;
}

bool ExtIndexManager::insert_value(IndexOperationParams const & params) {
  unsigned bucket_id = get_bucket_id(compute_hash(params));
#ifdef EIM_DBG
  Utils::info("[EIM][Insert value] Insert record with heap id " + std::to_string(params.page_id) +
    ":" + std::to_string(params.slot_id) + " into bucket with id " + std::to_string(bucket_id));
#endif

  Page &page = BufferManager::get_instance().get_page(bucket_id, index_path_);
  //TODO lookfor duplicates?

  if (!bucket_has_free_slot(&page, params.value_size + RECORD_ID_SIZE)) {
    split_bucket(bucket_id, &page);
    if (!bucket_has_free_slot(&page, params.value_size + RECORD_ID_SIZE)) {
      std::cout << "Data is skrewed for external hashing. Some entries will be missed. " << std::endl;
      return false;
    }
  }

  //skip depth and occupied
  unsigned occupied = get_records_count(page.get_data());
  char * data = page.get_data(false) + PAGE_AUX_DATA_SIZE + occupied * (RECORD_ID_SIZE + params.value_size);

  *((int *)data) = params.page_id;
  *((int *)data + 1) = params.slot_id;
  memcpy(data + RECORD_ID_SIZE, (char *)params.value, params.value_size);

  set_records_count(page.get_data(false), occupied + 1);

  page.unpin();
  return false;
}

//TODO finish
bool ExtIndexManager::delete_value(IndexOperationParams * params) {
  int offset = look_up_value(params);
  if (offset == -1) { return false; }

  unsigned bucket_id = get_bucket_id(compute_hash(*params));
  Page &page = BufferManager::get_instance().get_page(bucket_id, index_path_);
  unsigned &occupied = *((unsigned *)page.get_data(false) + 1);
  char * page_data = (char *)((unsigned *)page.get_data(false) + 2);
  memcpy(page_data + offset * params->value_size, page_data + (occupied - 1) * params->value_size, params->value_size);
  --occupied;
  page.unpin();

  return true;
}

void ExtIndexManager::split_bucket(unsigned bucket_number, Page * bucket_page) {
#ifdef EIM_DBG
  Utils::info("[ExtInd] Split bucket " + std::to_string(bucket_number));
#endif
  unsigned bucket_depth = *((unsigned *)bucket_page->get_data());
  unsigned total = get_records_count(bucket_page->get_data());
  Page &meta_page = BufferManager::get_instance().get_page(0, index_path_ + DIR_SUFFIX);
  unsigned global_depth = *((unsigned *)meta_page.get_data());
  meta_page.unpin();

  if (global_depth == bucket_depth) {
    double_buckets_count();
  }
  ++bucket_depth;

  //clear current bucket and save it to tmp memory
  char *target_bucket_data = new char[Page::PAGE_SIZE]();
  memcpy(target_bucket_data, bucket_page->get_data(), Page::PAGE_SIZE);
  *((int *)bucket_page->get_data(false)) = bucket_depth;
  set_records_count(bucket_page->get_data(false), 0);

  //init subl bucket
  unsigned subl_bucket = bucket_number | (1 << (bucket_depth - 1));
  Page &subl_bucket_page = BufferManager::get_instance().get_page(subl_bucket, index_path_);
  *((int *)subl_bucket_page.get_data(false)) = bucket_depth;
  set_records_count(subl_bucket_page.get_data(false), 0);
  subl_bucket_page.unpin();
#ifdef EIM_DBG
  Utils::info("[ExtInd] Switch pointer #" + std::to_string(subl_bucket) + " from " + std::to_string(bucket_number) + " to " + std::to_string(subl_bucket));
#endif
  //change subl ptr
  unsigned ptrs_per_page = Page::PAGE_SIZE / sizeof(int) - 1;
  unsigned dir_page_number = subl_bucket / ptrs_per_page + 1;
  unsigned dir_page_offset = subl_bucket % ptrs_per_page + 1;
  Page &subl_bucket_dir_page = BufferManager::get_instance().get_page(dir_page_number, index_path_+DIR_SUFFIX);
  *((int *)subl_bucket_dir_page.get_data(false) + dir_page_offset) = subl_bucket;
  subl_bucket_dir_page.unpin();

  //perform rehashing for problem bucket
  unsigned key_size = get_key_size();
  char *record = target_bucket_data + PAGE_AUX_DATA_SIZE;
  IndexOperationParams params;
  params.value_size = key_size;

  for (unsigned i = 0; i < total; ++i) {
    params.page_id = *((unsigned *)record);
    params.slot_id = *((unsigned *)record + 1);
    params.value = record + RECORD_ID_SIZE;
    insert_value(params);
    record += RECORD_ID_SIZE + key_size;
  }
  delete [] target_bucket_data;
}

void ExtIndexManager::double_buckets_count() {
  Page &meta_page = BufferManager::get_instance().get_page(0, index_path_ + DIR_SUFFIX);
  unsigned global_depth = *((int *)meta_page.get_data());
  ++*((int *)meta_page.get_data(false));
  meta_page.unpin();

#ifdef EIM_DBG
  Utils::info("[ExtInd] Increase buckets count from " + std::to_string(1 << global_depth) + " to " + std::to_string(1 << (global_depth + 1)));
#endif

  unsigned ptrs = 1 << global_depth;
  unsigned ind = 0;
  BucketPointersIterator bpi(index_path_ + DIR_SUFFIX);
  while (bpi.next() && ind < ptrs) {
    unsigned bucket_id = *bpi;
#ifdef EIM_DBG
    Utils::info("    [EIM] Duplicate ptr #" + std::to_string(ind) + " that points to " + std::to_string(bucket_id));
#endif
    //TODO inline for optimization
    add_ptr_to_index_dir(bucket_id, ind + ptrs);
    ++ind;
  }
}

void ExtIndexManager::add_ptr_to_index_dir(unsigned bucket_id, unsigned total) {
  unsigned ptrs_per_page = Page::PAGE_SIZE / sizeof(int) - 1;
  unsigned dir_page_number = total / ptrs_per_page + 1;
  unsigned dir_page_offset = total % ptrs_per_page + 1;
  Page &last_bucket_dir_page = BufferManager::get_instance().get_page(dir_page_number, index_path_ + DIR_SUFFIX);
  *((int *)last_bucket_dir_page.get_data(false) + dir_page_offset) = bucket_id;
  *((int *)last_bucket_dir_page.get_data(false)) += 1; //NOTE: this is direcory bucket
  last_bucket_dir_page.unpin();
}

bool ExtIndexManager::bucket_has_free_slot(Page * page, unsigned record_size) {
  unsigned occupied = get_records_count(page->get_data());
  //page size - sizeof depth value - occupied slots
  unsigned max_records = (Page::PAGE_SIZE - PAGE_AUX_DATA_SIZE) / record_size;
  return occupied < max_records;
}

unsigned ExtIndexManager::get_bucket_id(unsigned hash) {  
  BufferManager &bm = BufferManager::get_instance();
  Page &fst_dir_page = bm.get_page(0, index_path_ + DIR_SUFFIX);
  unsigned global_depth = *((int *)fst_dir_page.get_data());
  fst_dir_page.unpin();

  unsigned mask = (1 << global_depth) - 1;
  unsigned ptr_number = hash & mask;

  //One int is for ptr on page count
  unsigned ptrs_per_page = (Page::PAGE_SIZE - DIR_PAGE_AUX_DATA_SIZE) / DIR_REC_SIZE;
  unsigned ptr_page = ptr_number / ptrs_per_page + 1;
  unsigned ptr_offset = ptr_number % ptrs_per_page + DIR_PAGE_AUX_DATA_SIZE / sizeof(int);

#ifdef EIM_DBG
  Utils::info("  [EIM][Calc bucket id] Hash value: " + std::to_string(hash) + "; maks: " + std::to_string(mask) +
    "; pointer number: " + std::to_string(ptr_number));
  Utils::info("  [EIM][Calc bucket id] Ptr page: " + std::to_string(ptr_page) + "; ptr offset: " + std::to_string(ptr_offset));
#endif

  Page &req_page = BufferManager::get_instance().get_page(ptr_page, index_path_ + ExtIndexManager::DIR_SUFFIX);
  unsigned bucket_id = *((unsigned *)req_page.get_data() + ptr_offset);
  req_page.unpin();
#ifdef EIM_DBG
  Utils::info("  [EIM][Calc bucket id] Bucket id is " + std::to_string(bucket_id));
#endif
  return bucket_id;
}

void ExtIndexManager::init_buckets(std::string const & index_file_name, unsigned from, unsigned till, unsigned depth) {
  for (unsigned i = from; i < till; ++i) {
#ifdef EIM_DBG
    Utils::info("    [EIM] Bucket #" + std::to_string(i) + " has been intialized for file " + index_file_name);
#endif
    Page &page = BufferManager::get_instance().get_page(i, index_file_name);
    int * data = (int *)page.get_data(false);
    data[0] = depth;
    data[1] = 0;
    page.unpin();
  }
}

unsigned ExtIndexManager::get_key_size() {
  Page &fst_dir_page = BufferManager::get_instance().get_page(0, index_path_ + DIR_SUFFIX);
  unsigned key_size = *((unsigned *)fst_dir_page.get_data() + 1);
  fst_dir_page.unpin();
  return key_size;
}

unsigned ExtIndexManager::compute_hash(IndexOperationParams const & params) {
  unsigned base = 0;

  char * data = (char *)params.value;
  unsigned full_ints = params.value_size / sizeof(int);
  for (unsigned i = 0; i < full_ints; ++i) {
    base ^= *((unsigned *)data + i);
  }

  for (unsigned i = 0; i < params.value_size % sizeof(int); ++i) {
    base ^= *(data + full_ints * sizeof(int) + i);
  }
#ifdef EIM_DBG
  Utils::info("[EIM] Computed hash code is " + std::to_string(base));
#endif
  return base;
}


unsigned ExtIndexManager::get_records_count(char * data) { return *((unsigned *)data + 1); }
void ExtIndexManager::set_records_count(char * data, unsigned new_count) { *((unsigned *)data + 1) = new_count; }

//-----------------------------------------------------------------------------
// ExtIndexIterator

ExtIndexManager::BucketIterator::BucketIterator(
  std::string const & table_name, std::string const & index_name, char * init_key):
  current_page_(NULL), t_metadata_(NULL), page_offset_(0), records_to_go_(0), init_key_(init_key), record_data_(NULL) {

  ExtIndexManager mgr(table_name, index_name);
  key_size_ = mgr.get_key_size();

  IndexOperationParams params;
  params.value = init_key;
  params.value_size = key_size_;

  unsigned bucket_ind = mgr.get_bucket_id(mgr.compute_hash(params));
  current_page_ = &BufferManager::get_instance().get_page(bucket_ind, mgr.index_path_);
  page_offset_ = 0;
  records_to_go_ = *((unsigned *)current_page_->get_data() + 1);
  t_metadata_ = MetaDataProvider::get_instance()->get_meta_data(table_name);
}

ExtIndexManager::BucketIterator::~BucketIterator() {
  if (current_page_ != NULL) { current_page_->unpin(); }
  delete [] init_key_;
  if (record_data_ != NULL) { delete [] (char *)record_data_; }
}

bool ExtIndexManager::BucketIterator::next() {
  if (record_data_ != NULL) {
    delete [] (char *)record_data_;
    record_data_ = NULL;
  }
  if (records_to_go_ == 0) { return false;}

  --records_to_go_;
  if (page_offset_ == 0) {
    page_offset_ = PAGE_AUX_DATA_SIZE;
  } else {
    page_offset_ += RECORD_ID_SIZE + key_size_;
  }
  return true;
}

void * ExtIndexManager::BucketIterator::operator*() {
  if (record_data_ == NULL) {
    record_data_ = (char *)HeapFileManager::get_instance().get_record(*t_metadata_, this->record_page_id(), this->record_slot_id());
  }
  return record_data_;
}

unsigned ExtIndexManager::BucketIterator::record_page_id() {
  return *((unsigned *)(current_page_->get_data() + page_offset_));
}

unsigned ExtIndexManager::BucketIterator::record_slot_id() {
  return *((unsigned *)(current_page_->get_data() + page_offset_ + sizeof(int)));
}

