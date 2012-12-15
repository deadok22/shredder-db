#pragma once
#include <vector>
#include <cstdlib>
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
  Page& get_page(size_t page_id,string const& fname);
  BufferManager();
  ~BufferManager();

private:

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





