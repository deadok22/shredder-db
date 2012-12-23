#include "ExtIndexManager.h"
#include "../../backend/BufferManager.h"
#include "../../common/Utils.h"

#include <fstream>

std::string const ExtIndexManager::DIR_SUFFIX("_directory");
unsigned const ExtIndexManager::INIT_BUCKET_DEPTH = 3;
size_t const ExtIndexManager::DIR_REC_SIZE_IN_INT = 1;

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

ExtIndexManager::ExtIndexManager(std::string const & index_path): index_path_(index_path) {}

void ExtIndexManager::create_index(std::string const & path, TableMetaData_IndexMetadata const & ind_metadata) {
  std::string attrs = ind_metadata.name() + "_";
  for (int i = 0; i < ind_metadata.keys_size(); ++i) {
    attrs += "_" + ind_metadata.keys(i).name();
  }
  std::string index_file_name = path + "/ext_hash_" + attrs;
  std::string directory_file_name = index_file_name + ExtIndexManager::DIR_SUFFIX;
  
  //creates two files
  std::fstream directory_file(directory_file_name.c_str(), ios::in | ios::binary);
  directory_file.close();
  std::fstream index_file(index_file_name.c_str(), ios::in | ios::binary);
  index_file.close();

  BufferManager &bm = BufferManager::get_instance();
  Page &fst_dir_page = bm.get_page(0, directory_file_name);
  //markup directory
  char * page_data = fst_dir_page.get_data(false);
  *((int *)page_data) = ExtIndexManager::INIT_BUCKET_DEPTH;
  fst_dir_page.unpin();

  BucketPointersIterator bpi(directory_file_name, true);
  while (bpi.next()) { *bpi = bpi.get_current_hash(); }

  init_buckets(index_file_name, 0, 1 << INIT_BUCKET_DEPTH, INIT_BUCKET_DEPTH);

  //insert records
  Utils::warning("[ExtIndexManager] Filling with records is not implemented");
}


int ExtIndexManager::look_up_value(HashOperationParams * params) {
  unsigned bucket_id = get_bucket_id(compute_hash(*params));

  Page &page = BufferManager::get_instance().get_page(bucket_id, index_path_);
  unsigned occupied = *((unsigned *)page.get_data() + 1);
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
      break;
    }

    //2 ints is a record id
    data += (RECORD_ID_SIZE + params->value_size);
    ++bucket_record_index;
  }

  return bucket_record_index == occupied ? -1 : bucket_record_index;
}

bool ExtIndexManager::insert_value(HashOperationParams * params) {
  unsigned bucket_id = get_bucket_id(compute_hash(*params));
  Page &page = BufferManager::get_instance().get_page(bucket_id, index_path_);

  if (!bucket_has_free_slot(page, params->value_size + RECORD_ID_SIZE)) {
    page.unpin();
    Utils::error("[ExtInd] Bucket extension not implemented yet. Ignore for now");
    return false;
    //split bucket
    //update current
  }

  //skip depth and occupied
  unsigned &occupied = *((unsigned *)page.get_data(false) + 1);
  char * data = page.get_data(false) + PAGE_AUX_DATA_SIZE +
    occupied * (RECORD_ID_SIZE + params->value_size);

  *((int *)data) = params->page_id;
  *((int *)data + 1) = params->slot_id;
  memcpy(data + 2*sizeof(int), (char *)params->value, params->value_size);

  ++occupied;
  page.unpin();
  return false;
}

//TODO finish
bool ExtIndexManager::delete_value(HashOperationParams * params) {
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

void ExtIndexManager::split_buckets(unsigned bucket_number) {
  //double bucket records in directory

  //perform rehashinh for problem bucket
}

void ExtIndexManager::double_buckets_count() {
  //perform copying on directory

  //

}

bool ExtIndexManager::bucket_has_free_slot(Page & page, unsigned record_size) {
  unsigned occupied = *((unsigned *)page.get_data() + 1);
  //page size - sizeof depth value - occupied slots
  unsigned max_records = (Page::PAGE_SIZE - PAGE_AUX_DATA_SIZE) / record_size;
  return occupied < max_records;
}

unsigned ExtIndexManager::get_bucket_id(unsigned hash) {  
  std::string directory_file_name = index_path_ + ExtIndexManager::DIR_SUFFIX;
  BufferManager &bm = BufferManager::get_instance();
  Page &fst_dir_page = bm.get_page(0, index_path_);

  unsigned mask = (1 << *((int *)fst_dir_page.get_data() + 1)) - 1;
  //0th page is for metadata
  unsigned ptr_number = (hash & mask) + 1;

  //One int is for ptr on page count
  unsigned ptrs_per_page = (Page::PAGE_SIZE - sizeof(int)) / (DIR_REC_SIZE_IN_INT * sizeof(int));
  unsigned ptr_page = ptr_number / ptrs_per_page;
  unsigned ptr_offset = ptr_number % ptrs_per_page + 1;

  Page &req_page = BufferManager::get_instance().get_page(ptr_page, directory_file_name);
  return *((unsigned *)req_page.get_data() + ptr_offset);
}

void ExtIndexManager::init_buckets(std::string const & index_file_name, unsigned from, unsigned till, unsigned depth) {
  for (unsigned i = from; i < till; ++i) {
#ifdef EIM_DBG
    Utils::info("    [EIM] Bucket #" + std::to_string(i) + " has been intialized for file " + index_file_name);
#endif
    Page &page = BufferManager::get_instance().get_page(i, index_file_name);
    int * data = (int *)page.get_data(true);
    data[0] = depth;
    data[1] = 0;
    page.unpin();
  }
}

unsigned ExtIndexManager::compute_hash(HashOperationParams const & params) {
  unsigned base = 0;

  char * data = (char *)params.value;
  unsigned full_ints = params.value_size / sizeof(int);
  for (unsigned i = 0; i < full_ints; ++i) {
    base ^= *((unsigned *)data + i);
  }

  for (unsigned i = 0; i < params.value_size % sizeof(int); ++i) {
    base ^= *((char *)(data + full_ints * sizeof(int) + i));
  }
#ifdef EIM_DBG
  Utils::info("[EIM] Computed hash code is " + std::to_string(base));
#endif
  return base;
}