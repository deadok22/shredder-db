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

  void force();
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
void test_replace()
{
  BufferManager & bf = BufferManager::get_instance();
  for( size_t i = 0; i != 11; ++i)
    bf.get_page(i, "file");
    
  
}

int main() {
  DBInfo di;
  di.root_path = "./";
  
  di.max_page_cnt = 10;
  InfoPool::get_instance()->set_db_info(di);
  
  
  test_replace();

  return 0;
}
#endif

