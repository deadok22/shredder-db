#pragma once
#include <vector>
#include <cstdlib>
#include <string>
#include "DiskManager.h"
#include "Page.h"
#include "../common/Utils.h"
#include "../common/InfoPool.h"

class Page;
class DiskManager;
using std::vector;

class BufferManager
{
public:

  static BufferManager & get_instance();

  Page& get_page(size_t page_id,string const& fname);

private:

  BufferManager();
  ~BufferManager();

  Page* find_page(size_t page_id, string const& fname);
  vector<Page*>::iterator find_unpinned_page();
  bool replace(Page *p);
  

  
  DiskManager disk_mng_;
  vector<Page*> buffer_;
  size_t max_size_;
  
// TO-DO map

// closed  
  BufferManager(BufferManager const&);
  BufferManager& operator=(BufferManager const&);

};




//TEST_CODE
#ifdef TEST_BUFF_MNG
#include <iostream>
using namespace std;
int main() {
  DBInfo di;
  di.root_path = "./";
  
  di.max_page_cnt = 1;
  InfoPool::get_instance()->set_db_info(di);
  
  BufferManager & bf = BufferManager::get_instance();

  Page& p = bf.get_page(0,"file");  
  char * data = p.get_data();
  data[0]='!';
  p.set_dirty();
  p.unpin();

  Page& p1 = bf.get_page(1,"file");   

  

  Page& p2 = bf.get_page(1,"file");
  p1.set_dirty();    

  bf.get_page(2,"file");

  return 0;
}
#endif

