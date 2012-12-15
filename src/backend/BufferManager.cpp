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
  Page* p = 0;
  if( !( p = find_page(page_id,fname)) ){
    p = new Page(page_id,fname); // pin default    
    if( buffer_.size() < max_size_){
      // append in back
      if( disk_mng_.read_page(p) ){
        Utils::log("[buffer_manager] page appended in buffer");        
        buffer_.push_back(p);
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
    if( (*i)->isUnpinned()){
      Utils::log("[buffer_manager] found unpinned page in buffer");      
      return i;
    }
  Utils::log("[buffer_manager] not found unpinned page in buffer");
  return buffer_.end();  
}


bool BufferManager::replace(Page * p)
{
  vector<Page*>::iterator i;
  if( (i = find_unpinned_page()) != buffer_.end()){
    
    if( disk_mng_.write_page(*i) ){
      delete *i;
      *i = p;
      Utils::log("[buffer_manager] replace page and delete unpinned page");
      if( disk_mng_.read_page(p) )
        return true;
    }
  }
  Utils::log("[buffer_manager] method replace is failed", ERROR);
  return false;
}


//TEST_CODE
#ifdef TEST_BUFF_MNG
#include <iostream>
using namespace std;
int main() {
  DBInfo di;
  di.root_path = "./";
  
  di.max_page_cnt = 1;
  InfoPool::get_instance()->set_db_info(di);
  BufferManager bf;

  Page& p = bf.get_page(0,"file");  
  char * data = p.get_data();
  data[0]='!';
  p.set_dirty();
  p.unpin();

  bf.get_page(0,"file");   

  return 0;
}
#endif





