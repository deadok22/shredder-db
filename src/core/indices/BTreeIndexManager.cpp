#include <fstream>
#include <string>

#include "BTreeIndexManager.h"
#include "../../backend/BufferManager.h"
#include "../../common/Utils.h"
#include "TableMetadata.pb.h"
#include "../MetaDataProvider.h"
#include "../../backend/HeapFileManager.h"
#include "../../backend/Page.h"

size_t const BTreeIndexManager::RECORD_ID_SIZE = 2 * sizeof(int);

size_t const BTreeIndexManager::DATA_PAGE_HEADER_SIZE = 3 * sizeof(int);
size_t const BTreeIndexManager::NODE_PTR_SIZE = sizeof(int);
unsigned const BTreeIndexManager::LEAF_TYPE = 0xEFBE;
unsigned const BTreeIndexManager::NODE_TYPE = 0xDEDE;

BTreeIndexManager::BTreeIndexManager(std::string const & table_name, std::string const & index_name):
  IndexManager(table_name, index_name), index_file_name_(Utils::get_table_dir(table_name) + "/btree_" + index_name) {}

void BTreeIndexManager::create_index(
  std::string const & table_name,
  TableMetaData_IndexMetadata const & ind_metadata) {

#ifdef BTREE_DBG
  Utils::info("[BTree][Create index] Start creating BTree index named " + ind_metadata.name());
#endif
  std::string table_path = InfoPool::get_instance().get_db_info().root_path + table_name;
  std::string index_file_name = table_path + "/btree_" + ind_metadata.name();
  
  //creates two files
  std::fstream index_file(index_file_name.c_str(), ios::in | ios::binary);
  index_file.close();

  BufferManager &bm = BufferManager::get_instance();
  Page &meta_page = bm.get_page(0, index_file_name);

  TableMetaData * t_metadata = MetaDataProvider::get_instance()->get_meta_data(table_name);

  size_t key_size = IndexManager::compute_key_size(*t_metadata, ind_metadata);
  char * page_data = meta_page.get_data(false);
  *((unsigned *)page_data) = 1; //root ptr
  *((unsigned *)page_data + 1) = key_size; //key size
  *((unsigned *)page_data + 2) = 1; //pages used, 1 is for root
  meta_page.unpin();

  Page &root_page = bm.get_page(1, index_file_name);
  *((unsigned *)root_page.get_data(false)) = LEAF_TYPE;
  *((unsigned *)root_page.get_data(false) + 1) = 0; //number of items
  *((unsigned *)root_page.get_data(false) + 2) = 0; //ptr to next data leaf
  root_page.unpin();

  //insert records
  IndexOperationParams params;
  params.value_size = key_size;
  params.value = new char[params.value_size]();
#ifdef BTREE_DBG
  Utils::info("[BTree][Create index] Key size was determined to be " + std::to_string(params.value_size));
  Utils::info("[BTree][Create index] Insert values into index... ");
#endif

  HeapFileManager::HeapRecordsIterator rec_itr(t_metadata->name());
  BTreeIndexManager mock_manager(t_metadata->name(), ind_metadata.name());
  while (rec_itr.next()) {
    params.page_id = rec_itr.record_page_id();
    params.slot_id = rec_itr.record_slot_id();
    IndexManager::init_params_with_record(*t_metadata, ind_metadata, *rec_itr, &params);
    mock_manager.insert_value(params);
  }

  delete [] (char *)params.value;
}

int BTreeIndexManager::look_up_value(IndexOperationParams * params) {
  int leaf_index = tree_search(get_root_node(), params);
#ifdef BTREE_DBG
  Utils::info("  [BTree][Look up] Analyze leaf on page #" + std::to_string(leaf_index));
#endif

  Page &leaf_page = BufferManager::get_instance().get_page(leaf_index, index_file_name_);
  char *data = leaf_page.get_data();
  unsigned entry_size = params->value_size + RECORD_ID_SIZE;
  
  unsigned ins_index = find_offset_for_storage(data, RECORD_ID_SIZE, *params);
  unsigned ret_value = -1;
  if (ins_index != 0) {
    char * record = data + ins_index * entry_size + DATA_PAGE_HEADER_SIZE; 
    if (0 == record_comparer_.compare(record + RECORD_ID_SIZE, params->value)) {
      params->page_id = *((unsigned *)record);
      params->slot_id = *((unsigned *)record + 1);
#ifdef BTREE_DBG
      Utils::info("  [BTree][Look up] Found Record with id " + std::to_string(params->page_id) + ":" + std::to_string(params->slot_id));
#endif
      ret_value = leaf_index;
    }
  } 

  leaf_page.unpin();
  return ret_value;
}

int BTreeIndexManager::tree_search(unsigned node_id, IndexOperationParams * params) {
  if (node_id == 0) {
    Utils::error("[BTree][Look up] There is a bug in BTREE. Given node id point ot META data page");
  }

  Page &node_page = BufferManager::get_instance().get_page(node_id, index_file_name_);
  char *data = node_page.get_data();
  if (*((unsigned *)data) == LEAF_TYPE) {
    node_page.unpin();
    return node_id;
  }

#ifdef BTREE_DBG
  Utils::info("   [BTree][Look up] Examine node with id " + std::to_string(node_id));
#endif
  //where are at node. continue search...
  unsigned record_index = find_offset_for_storage(data, NODE_PTR_SIZE, *params);
  data += DATA_PAGE_HEADER_SIZE + record_index * (params->value_size + NODE_PTR_SIZE);
  unsigned node_to_examine = *((unsigned *) data);
  node_page.unpin();

  return tree_search(node_to_examine, params);
}

bool BTreeIndexManager::insert_value(IndexOperationParams const & params) {
#ifdef BTREE_DBG
  Utils::info("[BTree][Insert] New insert started for record with id " + std::to_string(params.page_id) + ":" + std::to_string(params.slot_id));
#endif

  void * new_child = NULL;
  tree_insert(get_root_node(), params, new_child);

  if (new_child != NULL) { //look like this is first split
    if (get_root_node() != 1) {
      Utils::error("[BTree][Insert] First split ever is performed, but root_id is not 1");
      return false;
    }
    //Emmmmm here is unnatural logic. But this case can orruce only once.
    change_root((char *) new_child);
    delete [] (char *) new_child;
    new_child = NULL;
#ifdef BTREE_DBG
    Utils::info("[BTree][Insert] First split. Create a new root. New root id is " + std::to_string(get_root_node()));
#endif
  }

#ifdef BTREE_DBG
  Utils::info("[BTree][Insert] Insert finished");
#endif
  return true;
}


void BTreeIndexManager::tree_insert(unsigned node_id, IndexOperationParams const & params, void *& child_entry){
  if (node_id == 0) {
    Utils::error("[BTree][Insert] There is a bug in BTREE. Given node id point ot META data page");
  }

  Page &node_page = BufferManager::get_instance().get_page(node_id, index_file_name_);
  char *data = node_page.get_data(false);
  unsigned number_of_items = get_records_count(data);

  if (*((unsigned *)data) == LEAF_TYPE) {
#ifdef BTREE_DBG
    Utils::info("[BTree][Insert][Top Down] Process LEAF node with id " + std::to_string(node_id));
#endif
    size_t entry_size = params.value_size + RECORD_ID_SIZE;
    unsigned entries_per_page = (Page::PAGE_SIZE - DATA_PAGE_HEADER_SIZE) / entry_size; 

    unsigned ins_index = find_offset_for_storage(data, RECORD_ID_SIZE, params);

    if (number_of_items < entries_per_page) {
#ifdef BTREE_DBG
    Utils::info("[BTree][Insert][Top Down] Perform 'NON-FULL' Insert");
#endif
      insert_into_leaf(data, ins_index, params);
      child_entry = NULL;
    } else {
      SplitNodeOpContext ctx(data, NULL, ins_index, entry_size, entries_per_page);
      char * key_to_push = split_node(ctx, params);
#ifdef BTREE_DBG
      unsigned new_node_id = *((unsigned *)(key_to_push + get_key_size() + NODE_PTR_SIZE));
      Utils::info("[BTree][Insert][Top Down] Perform 'LEAF_SPLIT'. Created node id is " + std::to_string(new_node_id) + ". Push child with left-ref to " + std::to_string(node_id));
#endif

      *((unsigned *)key_to_push) = node_id;
      child_entry = key_to_push;
    }
    node_page.unpin();
  } else { //handle NODE_TYPE
#ifdef BTREE_DBG
    Utils::info("[BTree][Insert][Top Down] Process NODE node with id " + std::to_string(node_id));
#endif
    unsigned record_index = find_offset_for_storage(data, NODE_PTR_SIZE, params);

    data += DATA_PAGE_HEADER_SIZE + record_index * (params.value_size + NODE_PTR_SIZE);
    node_page.unpin(); //don't pin too much
#ifdef BTREE_DBG
    Utils::info("[BTree][Insert][Top Down] Execute insert for node " + std::to_string(*((unsigned *) data)));
#endif
    tree_insert(*((unsigned *) data), params, child_entry);

    if (child_entry == NULL) { //no entries for update
#ifdef BTREE_DBG
      Utils::info("[BTree][Insert][Bottom Up] Finish NODE node processing with id " + std::to_string(node_id));
#endif
      return;
    }

    //reload page
    Page &node_page = BufferManager::get_instance().get_page(node_id, index_file_name_);
    char *data = node_page.get_data(false);
    unsigned number_of_items = get_records_count(data);
      
    size_t entry_size = params.value_size + NODE_PTR_SIZE;
    //also count last entra ptr
    unsigned entries_per_page = (Page::PAGE_SIZE - DATA_PAGE_HEADER_SIZE - NODE_PTR_SIZE) / entry_size; 
    
    IndexOperationParams search_params;
    search_params.value = (char *)child_entry + NODE_PTR_SIZE;
    search_params.value_size = get_key_size();
    unsigned ins_index = find_offset_for_storage(data, NODE_PTR_SIZE, search_params);
    if (number_of_items < entries_per_page) { //just insert into node
#ifdef BTREE_DBG
      Utils::info("[BTree][Insert][Bottom Up] Perform 'NON-FULL' insert for child (curr node id is" + std::to_string(node_id) + ")");
#endif
      insert_into_node(data, ins_index, (char *) child_entry);
      delete [] (char *) child_entry;
      child_entry = NULL;
    } else { //split node
      BTreeIndexManager::SplitNodeOpContext ctx(data, (char *)child_entry, ins_index, entry_size, entries_per_page);
      char * key_to_push = split_node(ctx, params);
      delete [] (char *) child_entry;
      child_entry = NULL;

      *((unsigned *)key_to_push) = node_id;
      child_entry = key_to_push;
#ifdef BTREE_DBG
      unsigned new_node_id = *((unsigned *)(key_to_push + get_key_size() + NODE_PTR_SIZE));
      Utils::info("[BTree][Insert][Bottom Up] Performed 'NODE_SPLIT'. Push child with left-ref to " + std::to_string(node_id) + " and right-ref to " + std::to_string(new_node_id));
#endif
      if (node_id == get_root_node()) {
        change_root((char *)child_entry);
        delete [] (char *) child_entry;
        child_entry = NULL;
#ifdef BTREE_DBG
        Utils::info("[BTree][Insert][Bottom Up] Create a new root. New root id is " + std::to_string(get_root_node()));
#endif
      }
    }
    node_page.unpin();
#ifdef BTREE_DBG
    Utils::info("[BTree][Insert][Bottom Up] Finish insert exec for node " + std::to_string(node_id));
#endif
  }
  return;
}

char * BTreeIndexManager::split_node(SplitNodeOpContext &ctx, IndexOperationParams const & params) {

  bool node_is_leaf = ctx.child_entry == NULL;
  unsigned items_to_move = get_records_count(ctx.current_node_data) - ctx.entries_per_page / 2;
      
  //create a new leaf
  unsigned new_node_id = get_new_page_id();
  Page &new_node = BufferManager::get_instance().get_page(new_node_id, index_file_name_);

  char *new_data = new_node.get_data(false);
  *((unsigned *) new_data) = node_is_leaf ? LEAF_TYPE : NODE_TYPE;
  set_records_count(new_data, items_to_move);
  set_records_count(ctx.current_node_data, get_records_count(ctx.current_node_data) - items_to_move);

  if (node_is_leaf) { //add into leaves' linked list
    *((unsigned *) new_data + 2) = *((unsigned *) ctx.current_node_data + 2);
    *((unsigned *) ctx.current_node_data + 2) = new_node_id;
  }

  unsigned items_in_left_node = get_records_count(ctx.current_node_data);
#ifdef BTREE_DBG
  Utils::info("[BTree][Split] Items in left node: " + std::to_string(items_in_left_node) + "; items in right node: " + std::to_string(items_to_move));
#endif

  char * start_copy_ptr = ctx.current_node_data + DATA_PAGE_HEADER_SIZE + items_in_left_node * ctx.entry_size;
  memcpy(new_data + DATA_PAGE_HEADER_SIZE, start_copy_ptr, items_to_move * ctx.entry_size + (node_is_leaf ? 0 : NODE_PTR_SIZE));
  
  char * insertion_data = ctx.current_node_data;
  if (items_in_left_node <= ctx.ins_index) { //entry should go to 'right' node
    insertion_data = new_data;
    ctx.ins_index -= items_in_left_node;
  }

  if (node_is_leaf) {
    insert_into_leaf(insertion_data, ctx.ins_index, params);
  } else {
    insert_into_node(insertion_data, ctx.ins_index, ctx.child_entry);
  }

  char *key_to_push = new char[params.value_size + 2 * NODE_PTR_SIZE];
  unsigned aux_data_size = node_is_leaf ? RECORD_ID_SIZE : NODE_PTR_SIZE;
  memcpy(key_to_push + NODE_PTR_SIZE, new_data + DATA_PAGE_HEADER_SIZE + aux_data_size, params.value_size);
  *((unsigned *)(key_to_push + NODE_PTR_SIZE + params.value_size)) = new_node_id;

  new_node.unpin();

  return key_to_push;
}

void BTreeIndexManager::change_root(char *entry) {
  unsigned new_root_id = get_new_page_id();

  Page &new_root_node = BufferManager::get_instance().get_page(new_root_id, index_file_name_);
  char * root_data = new_root_node.get_data(false);
  *((unsigned *)root_data) = NODE_TYPE;
  set_records_count(root_data, 1);
  
  memcpy(root_data + DATA_PAGE_HEADER_SIZE , entry, 2 * NODE_PTR_SIZE + get_key_size());
  new_root_node.unpin();

  //update root node
  BufferManager &bm = BufferManager::get_instance();
  Page &meta_page = bm.get_page(0, index_file_name_);
  *((unsigned *)meta_page.get_data(false)) = new_root_id;
  meta_page.unpin();
}

void BTreeIndexManager::insert_into_leaf(char * data, unsigned index, IndexOperationParams const & params) {
  unsigned items_count = get_records_count(data);
  set_records_count(data, items_count + 1);
  
  unsigned record_size = params.value_size + RECORD_ID_SIZE;
  data += DATA_PAGE_HEADER_SIZE;

  memmove(data + (index + 1) * record_size , data + index * record_size, record_size * (items_count - index));

  //init new entry
  char * new_entry = data + index * record_size;
  *((unsigned *)new_entry) = params.page_id;
  *((unsigned *)new_entry + 1) = params.slot_id;
  memcpy(new_entry + RECORD_ID_SIZE, params.value, params.value_size);
}

void BTreeIndexManager::insert_into_node(char * data, unsigned index, char * entry) {
  unsigned items_count = get_records_count(data);
  set_records_count(data, items_count + 1);

  unsigned record_size = NODE_PTR_SIZE + get_key_size();
  data += DATA_PAGE_HEADER_SIZE;

  memmove(data + (index + 1) * record_size , data + index * record_size, record_size * (items_count - index) + NODE_PTR_SIZE);
  char * new_entry = data + index * record_size;
  memcpy(new_entry, entry, record_size + 2 * NODE_PTR_SIZE);
}

unsigned BTreeIndexManager::find_offset_for_storage(char * data, unsigned aux_record_data_size, IndexOperationParams const & params) {
  unsigned records_count = get_records_count(data);
  data += DATA_PAGE_HEADER_SIZE;

  unsigned record_index = 0;
  while (record_index < records_count) {
    char *key_data = data + aux_record_data_size;
    //NB can be enhanced with binary search
    if (record_comparer_.compare(key_data, params.value) >= 0) { break; }
    data += aux_record_data_size + params.value_size;
    ++record_index;
  }
  return record_index;
}


bool BTreeIndexManager::delete_value(IndexOperationParams * params) {
  Utils::warning("[BTREE] Delete is unimplemented");
  return false;
}

unsigned BTreeIndexManager::get_new_page_id() {
  BufferManager &bm = BufferManager::get_instance();
  Page &meta_page = bm.get_page(0, index_file_name_);
  unsigned pages_used = ++(*((unsigned *)meta_page.get_data(false) + 2));
  meta_page.unpin();
  return pages_used;  
}
unsigned BTreeIndexManager::get_root_node() const { return get_int_value_by_offset(0, 0); }
unsigned BTreeIndexManager::get_key_size() const { return get_int_value_by_offset(0, 1); }
unsigned BTreeIndexManager::get_records_count(char * data) const { return *((unsigned *)data + 1); }
void BTreeIndexManager::set_records_count(char * data, unsigned new_rec_cnt) { *((unsigned *)data + 1) = new_rec_cnt;}

unsigned BTreeIndexManager::get_int_value_by_offset (unsigned page_id, unsigned offset) const {
  BufferManager &bm = BufferManager::get_instance();
  Page &meta_page = bm.get_page(page_id, index_file_name_);
  unsigned data = *((unsigned *)meta_page.get_data() + offset);
  meta_page.unpin();
  return data;
}

unsigned BTreeIndexManager::get_left_most_leaf() const {
  unsigned left_most_id = get_root_node();
  bool found = false;
  while (!found) {
    Page &condidate = BufferManager::get_instance().get_page(left_most_id, index_file_name_);
    char *data = condidate.get_data();
    if (*((unsigned *)data) == LEAF_TYPE) {
      found = true;
    } else { 
      //NB do we need to handle empty node??
      left_most_id = *((unsigned *)(data + DATA_PAGE_HEADER_SIZE));
    }
    condidate.unpin();
  }

  return left_most_id;
}

std::string BTreeIndexManager::get_index_file_name() const { return index_file_name_; }

//-----------------------------------------------------------------------------
// Sorted iterator

BTreeIndexManager::SortedIterator::SortedIterator(std::string const & table_name, std::string const & index_name, char * init_key):
  btm_(new BTreeIndexManager(table_name, index_name)), t_metadata(NULL), current_page_(NULL),
  page_offset_(0), records_to_go_(0), key_size_(0), record_data_(NULL), init_key_(init_key) {

  t_metadata = MetaDataProvider::get_instance()->get_meta_data(table_name);
  key_size_ = btm_->get_key_size();
}

BTreeIndexManager::SortedIterator::~SortedIterator() {
  if (current_page_ != NULL) { current_page_->unpin(); }
  if (record_data_ != NULL) { delete [] (char *)record_data_; }
  if (init_key_ != NULL) { delete [] init_key_; }
}

bool BTreeIndexManager::SortedIterator::switch_page() {
  unsigned page_id = 0;

  if (current_page_ == NULL) {
    if (init_key_ == NULL) {
      page_id = btm_->get_left_most_leaf();
    } else {
      IndexOperationParams params;
      params.value = init_key_;
      params.value_size = key_size_;
      page_id = btm_->tree_search(btm_->get_root_node(), &params);
    }
  } else {
    page_id = *((unsigned *)current_page_->get_data() + 2);
    current_page_->unpin();
  }

#ifdef BTREE_SI_DBG
  Utils::info("[BTree][SortIter] Next leaf page id is " + std::to_string(page_id));
#endif
  if (page_id == 0) { 
#ifdef BTREE_SI_DBG
    Utils::info("[BTree][SortIter] Switch failed");
#endif
    return false;
  }

  current_page_ = &BufferManager::get_instance().get_page(page_id, btm_->get_index_file_name());  
  page_offset_ = DATA_PAGE_HEADER_SIZE;
  records_to_go_ = btm_->get_records_count(current_page_->get_data()) - 1;
#ifdef BTREE_SI_DBG
  Utils::info("[BTree][SortIter] Switch successfull. Number records to process is " + std::to_string(records_to_go_));
#endif
  return true;
}

bool BTreeIndexManager::SortedIterator::next() {
  if (record_data_ != NULL) {
    delete [] (char *)record_data_;
    record_data_ = NULL;
  }
  if (records_to_go_ > 0) {
    --records_to_go_;
    page_offset_ += RECORD_ID_SIZE + key_size_;
    return true;
  }

  return switch_page();
}

void * BTreeIndexManager::SortedIterator::operator*() {
  if (record_data_ == NULL) {
    record_data_ = HeapFileManager::get_instance().get_record(*t_metadata, this->record_page_id(), this->record_slot_id());
  }
  return record_data_;  
}

unsigned BTreeIndexManager::SortedIterator::record_page_id() {
  return *((unsigned *)(current_page_->get_data() + page_offset_));
}
unsigned BTreeIndexManager::SortedIterator::record_slot_id() {
  return *(((unsigned *)(current_page_->get_data() + page_offset_)) + 1);
}

