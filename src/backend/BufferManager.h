#pragma once
#include <vector>
#include <cstdlib>
#include "DiskManager.h"

class Page;
class DiskManager;
using std::vector;

class BufferManager
{
public:
  Page* get_page(size_t page_id);
  BufferManager();
  ~BufferManager();

private:
// closed  
  BufferManager(BufferManager const&);
  BufferManager& operator=(BufferManager const&);

  Page* find_page(size_t page_id);
  vector<Page*>::iterator find_unpinned_page();
  bool replace(size_t page_id);
  bool save_page(Page* page);
 
// return arg: page     
  bool load_page(size_t page_id,Page** page);

  
  DiskManager disk_mng_;
  vector<Page*> buffer_;
  size_t max_size_;
  
// TO-DO map
  //map<page_id, index>
  //map<count_pin, index>
};
