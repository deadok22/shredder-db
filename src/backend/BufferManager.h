#pragma once
#include <vector>
#include <cstdlib>
#include <string>
#include "DiskManager.h"
#include "Page.h"
#include "../common/Utils.h"
#include "../common/InfoPool.h"

using std::vector;
using std::string;

class BufferManager
{
public:
  static BufferManager & get_instance();

  Page& get_page(size_t page_id,string const& fname);
  void purge();
  void force();
  void force(string const & table_name);

  //for debug
  void print_pinned_page();
  unsigned get_pinned_page_count();
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

private:
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
  for( size_t i = 0; i != 10; ++i)
    bf.get_page(i, "file");
    
  bf.print_pinned_page();
}

int main() {
  DBInfo di;
   
  
  di.max_page_cnt = 10;
  InfoPool::get_instance().set_db_info(di);
  
  
  test_replace();

  return 0;
}
#endif

