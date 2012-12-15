#include "BufferManager.h"

BufferManager & BufferManager::get_instance() {
  static BufferManager bm;
  return bm;
}

BufferManager::BufferManager()
 : max_size_(InfoPool::get_instance()->get_db_info()->max_page_cnt)
{  
  Utils::log("[buffer_manager] create object buffmanager");
}

BufferManager::~BufferManager()
{
  Utils::log("[buffer_manager] call destructor in buffer, free page");        
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i){
    disk_mng_.write_page(*i);
    delete *i;      
  }  
}

Page& BufferManager::get_page(size_t page_id,string const& fname)
{
  Utils::log("[buffer_manager] get page with page id: "+std::to_string(page_id) );        
  Page* p = 0;
  if( !( p = find_page(page_id,fname)) ){
    p = new Page(page_id,fname); // pin default    
    if( buffer_.size() < max_size_){
      // append in back
      if( disk_mng_.read_page(p) ){
        buffer_.push_back(p);
        Utils::log("[buffer_manager] page appended in buffer, current size buffer: "+ std::to_string(buffer_.size()) );
      } else {
        delete p;
        exit(EXIT_FAILURE);        
      }         
    }else {
      if( !replace(p) ){
        delete p;
        exit(EXIT_FAILURE);
      }
    }    
  } 
  p->pin();
  return *p;   
}


Page* BufferManager::find_page(size_t page_id,string const& fname)
{
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i)
    if( (*i)->get_pid() == page_id && (*i)->get_fname() == fname ){
      Utils::log("[buffer_manager] found page in buffer");      
      return *i;
    }
  Utils::log("[buffer_manager] not found page in buffer");
  return 0;  
}




vector<Page*>::iterator BufferManager::find_unpinned_page()
{
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i)
    if( (*i)->is_unpinned()){
      Utils::log("[buffer_manager] found unpinned page in buffer: "+ std::to_string( (*i)->get_pid() ) ) ;      
      return i;
    }
  Utils::log("[buffer_manager] not found unpinned page in buffer");
  return buffer_.end();  
}


bool BufferManager::replace(Page * p)
{
  vector<Page*>::iterator i;
  if( (i = find_unpinned_page()) != buffer_.end()){
    Utils::log("[buffer_manager] replace page and delete unpinned page");    
    if( disk_mng_.write_page(*i) ){
      delete *i;
      *i = p;
      if( disk_mng_.read_page(p) )
        return true;
    }
  }
  Utils::log("[buffer_manager] method replace is failed", ERROR);
  return false;
}







