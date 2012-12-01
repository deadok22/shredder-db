#pragma once
#include <vector>
#include "DiskManager.h"
#include "Page.h"

using std::vector;

class BufferManager
{
public:
  Page* get_page(size_t page_id);
  BufferManager();
  ~BufferManager();

private:
  BufferManager(BufferManager const&);
  BufferManager& operator=(BufferManager const&);

  Page* find_page(size_t page_id);
  bool replace(size_t page_id);
  bool save_page(size_t page_id);
  bool load_page(size_t page_id);


  DiskManager& disk_mng_;
  vector<Page*> buffer_;
  //map<page_id, index>
  //map<count_pin, index>
};
