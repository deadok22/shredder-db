#pragma once
#include <cstdio>
#include <cstdlib>
#include <algorithm> 
#include <string>
#include "Page.h"
#include "../common/Utils.h"
#include "../common/InfoPool.h"


using std::string;

class DiskManager
{
public:
  bool read_page(Page * page);
  bool write_page(Page * page);

  DiskManager();

private:
 
  bool is_allocated(size_t page_id);

  bool update_context(string const & fname);
  bool init_file();// change handler of file

  string fname_;
  FILE* file_;

// closed
  DiskManager(DiskManager const&);
  DiskManager& operator=(DiskManager const&);  
};


