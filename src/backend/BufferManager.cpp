#include "BufferManager.h"

BufferManager & BufferManager::get_instance() {
  static BufferManager bm;
  return bm;
}

BufferManager::BufferManager()
 : max_size_(InfoPool::get_instance()->get_db_info()->max_page_cnt)
{ 
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] create object buffmanager");
#endif
}

BufferManager::~BufferManager()
{
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] call destructor in buffer, save and free page");
#endif
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i){
    disk_mng_.write_page(*i);
    delete *i;
  }
}

void BufferManager::purge()
{
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] purge buffer");
#endif
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i){
    disk_mng_.write_page(*i);
    delete (*i);
  }
  buffer_.clear();
}

void BufferManager::force()
{
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] force pages");
#endif
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i)
    disk_mng_.write_page(*i);
}

Page& BufferManager::get_page(size_t page_id,string const& fname)
{
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] get page with page id: "+std::to_string(page_id) );
#endif
  Page* p = 0;
  if( !( p = find_page(page_id,fname)) ){
    p = new Page(page_id,fname); // pin default    
    if( buffer_.size() < max_size_){
      // append in back
      if( disk_mng_.read_page(p) ){
        buffer_.push_back(p);
#ifdef IO_BUFF_M 
        Utils::log("[BufferManager] page appended in buffer, current size buffer: "+ std::to_string(buffer_.size()) );
#endif
      } else {
        delete p;
        Utils::critical_error();       
      }         
    }else {
      if( !replace(p) ){
        delete p;
        Utils::critical_error();
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
#ifdef IO_BUFF_M 
      Utils::log("[BufferManager] found page in buffer");
#endif
      return *i;
    }
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] not found page in buffer");
#endif
  return 0;  
}




vector<Page*>::iterator BufferManager::find_unpinned_page()
{
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i)
    if( (*i)->is_unpinned()){
#ifdef IO_BUFF_M 
      Utils::log("[BufferManager] found unpinned page in buffer: "+ std::to_string( (*i)->get_pid() ) );
#endif
      return i;
    }
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] not found unpinned page in buffer");
#endif
  return buffer_.end();  
}


bool BufferManager::replace(Page * p)
{
  vector<Page*>::iterator i;
  if( (i = find_unpinned_page()) != buffer_.end()){
#ifdef IO_BUFF_M 
    Utils::log("[BufferManager] replace page and delete unpinned page");
#endif
    if( disk_mng_.write_page(*i) ){
      delete *i;
      *i = p;
      if( disk_mng_.read_page(p) )
        return true;
    }
  }
#ifdef IO_BUFF_M 
  Utils::log("[BufferManager] method replace is failed", ERROR);
#endif
  return false;
}







