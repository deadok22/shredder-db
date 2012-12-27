#include "BufferManager.h"


BufferManager & BufferManager::get_instance() {
  static BufferManager bm;
  return bm;
}


BufferManager::BufferManager() : max_size_(InfoPool::get_instance().get_db_info().max_page_cnt){ 
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] create object buffmanager");
#endif
}


BufferManager::~BufferManager() {
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] call destructor in buffer, save and free page");
#endif
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i){
    disk_mng_.write_page(*i);
    delete *i;
  }
}

void BufferManager::print_pinned_page() {
  std::cout << "[BufferManager] print pinned page:" << std::endl;
  for(vector<Page*>::iterator i = buffer_.begin(), e = buffer_.end(); i != e; ++i){
    if( !(*i)->is_unpinned() ){      
      std::cout << "page_id: "<< std::to_string((*i)->get_pid()) << "; table_name: " << Utils::get_table_name((*i)->get_fname()) << std::endl;
    }
  } 
}

void BufferManager::purge() {
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] purge buffer");
#endif
  for(vector<Page*>::iterator i = buffer_.begin(), e = buffer_.end(); i != e; ++i){
    disk_mng_.write_page(*i);
    delete (*i);
  }
  buffer_.clear();
  dict_page_.clear();
}


void BufferManager::force() {
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] force pages");
#endif
  for(vector<Page*>::iterator i = buffer_.begin(), e = buffer_.end(); i != e;++i){
    disk_mng_.write_page(*i);
  }
}

void BufferManager::force(string const & table_name) {
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] force pages of dir_name: " + table_name);
#endif
  size_t st_dir = InfoPool::get_instance().get_db_info().root_path.size();

  for(vector<Page*>::iterator i = buffer_.begin(), e = buffer_.end(); i != e;++i){
    string fname = (*i)->get_fname();   
    if( fname.substr(st_dir, table_name.size()).compare(table_name) == 0){    
      if( !(*i)->is_unpinned()) {
        Utils::log("[BufferManager] page is not unpinned",ERROR);
      }
      disk_mng_.write_page(*i);
    }
  }
}

Page& BufferManager::get_page(size_t page_id,string const& fname) {
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] get page with page id: "+std::to_string(page_id) );
#endif
  Page* p = 0;
  if( !( p = find_page(page_id,fname)) ){
    p = new Page(page_id, fname); // pin default    
    if( buffer_.size() < max_size_){
      // append in back
      append(p);
    }else {
      replace(p);
    }
  }
  p->pin();
  return *p;   
}

void BufferManager::append(Page *p) {
  if(disk_mng_.read_page(p) ){
    buffer_.push_back(p);
    dict_page_[hash_pair(p->get_pid(), p->get_fname())] = buffer_.size() - 1;
#ifdef IO_BUFF_M 
    Utils::log("[BufferManager] page appended in buffer, current size buffer: "+ std::to_string(buffer_.size()) );
#endif
  } else {
    delete p;
    Utils::critical_error();       
  }
}

Page* BufferManager::find_page(size_t page_id,string const& fname) {
  size_t key = hash_pair(page_id, fname);
  if( dict_page_.count(key) ) {
    size_t idx = dict_page_[key];
#ifdef IO_BUFF_M
    Utils::log("[BufferManager] found page in buffer");
#endif
    return buffer_[idx];
  }
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] not found page in buffer");
#endif
  return 0;  
}

unsigned BufferManager::get_pinned_page_count() {
  unsigned pinned_pages_cnt = 0;
  for(vector<Page*>::iterator i = buffer_.begin(),  e = buffer_.end(); i != e;++i){
    if(!(*i)->is_unpinned()){ ++pinned_pages_cnt; }
  }
  return pinned_pages_cnt;
}

vector<Page*>::iterator BufferManager::find_unpinned_page()
{
  vector<Page*>::iterator res = buffer_.end();
  time_t min = LONG_MAX;
  for( vector<Page*>::iterator i = buffer_.begin(),  e = buffer_.end(); i != e; ++i) {
    if( (*i)->is_unpinned() && min > (*i)->get_tstamp() ) {
      min = (*i)->get_tstamp();
      res = i;
    }
  }

  if ( res != buffer_.end()){
#ifdef IO_BUFF_M 
      Utils::log("[BufferManager] found unpinned page in buffer: "+ std::to_string( (*res)->get_pid() ) );
#endif
    return res;
  } else {
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] not found unpinned page in buffer");
#endif
    return res;
  }
}

void BufferManager::replace(Page * p) {
  vector<Page*>::iterator i;
  if( (i = find_unpinned_page()) != buffer_.end()){
#ifdef IO_BUFF_M 
    Utils::log("[BufferManager] replace page and delete unpinned page");
#endif
    if( disk_mng_.write_page(*i) ){
      size_t key = hash_pair((*i)->get_pid(), (*i)->get_fname());
      dict_page_.erase( key ); 
      delete *i;
      *i = p;
      key = hash_pair((*i)->get_pid(), (*i)->get_fname());
      dict_page_[key] = i - buffer_.begin();
      if( disk_mng_.read_page(p) ) { return; }
    }
  }

  Utils::log("[BufferManager] method replace is failed", ERROR);
  
  delete p;
  Utils::critical_error();
}

size_t BufferManager::hash_pair(size_t page_id, string const& fname) {
  return hash_fn(fname) ^ page_id;
}

