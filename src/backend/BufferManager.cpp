#include "BufferManager.h"
#include "DiskManager.h"
#include "Page.h"
#include "../common/Utils.h"
#include "../common/InfoPool.h"


BufferManager::BufferManager()
 : max_size_(InfoPool::get_instance()->get_db_info()->count_page)
{  
  Utils::log("[buffer_manager] create object buffmanager");
}

BufferManager::~BufferManager()
{
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i){
    if( (*i)->isDirty() ) 
      save_page(*i);
    delete *i;      
  }  
}

Page* BufferManager::get_page(size_t page_id)
{
  Page* p = 0;
  if( !( p = find_page(page_id)) ){
    if( buffer_.size() < max_size_){
      // append in back
      if(load_page(page_id,&p)){
        Utils::log("[buffer_manager] page appended in buffer");        
        buffer_.push_back(p);
      }          
    }else {
      replace(page_id);
    }    
  }   
  return p;   
}


Page* BufferManager::find_page(size_t page_id)
{
  for(vector<Page*>::iterator i = buffer_.begin(),
  e = buffer_.end(); i != e;++i)
    if( (*i)->get_pid() == page_id){
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


bool BufferManager::replace(size_t page_id)
{
  vector<Page*>::iterator i;
  if( (i = find_unpinned_page())!= buffer_.end()){
    
    if( (*i)->isDirty() )  {
      if(save_page(*i)){
        delete *i;
        if( load_page(page_id,&(*i)) )
          return true;  
      }
    } else {
      delete *i;
      if( load_page(page_id,&(*i)) )
        return true;       
    }
  }
  Utils::log("[buffer_manager] method replace is failed");
  return false;
}

bool BufferManager::save_page(Page * page)
{
  return disk_mng_.write_page(page->get_pid(),page->get_data());
}


bool BufferManager::load_page(size_t page_id,Page** page)
{
  *page = new Page(page_id);
  return disk_mng_.read_page(page_id, (*page)->get_data());
}





//TEST_CODE
#ifdef TEST_BUFF_MNG
#include <iostream>
using namespace std;
int main() {
  DBInfo di;
  di.root_path = "./";
  di.page_size = 4096;
  di.cur_file = "file";
  di.count_page = 10;
  InfoPool::get_instance()->set_db_info(di);
  {  
    DiskManager dm("file");

    char * buf = new char[InfoPool::get_instance()->get_db_info()->page_size];
    buf[0] = '1';
    for(int i = 0; i < 11;++i)
      dm.write_page(i,buf);
  }
  BufferManager bf;

  for(int i = 0; i < 11;++i)
    bf.get_page(i);
  
    

   
  return 0;
}
#endif

