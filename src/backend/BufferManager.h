#pragma once
#include <vector>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>
#include <functional>
#include <algorithm>
#include <climits>
#include "DiskManager.h"
#include "Page.h"
#include "../common/Utils.h"
#include "../common/InfoPool.h"

using std::unordered_map;
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
  void replace(Page *p);
  void append(Page *p);

  
  DiskManager disk_mng_;
  vector<Page*> buffer_;
  size_t max_size_;
    
  size_t hash_pair(size_t page_id, string const& fname);
  std::hash<string> hash_fn;  
  unordered_map< size_t, size_t> dict_page_; 
  
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

  for( size_t i = 0; i != 9; ++i)
    bf.get_page(i, "file");

  Page & p = bf.get_page(9, "file");;
  p.unpin();
  bf.get_page(11, "file");
  bf.get_page(8, "file");
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

