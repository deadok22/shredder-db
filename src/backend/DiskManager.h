#pragma once
#include <cstdio>
#include <cstdlib>
#include <algorithm> 
#include <string>
#include "Page.h"
#include "../common/Utils.h"



using std::string;

class DiskManager
{
public:
  bool read_page(Page * page);
  bool write_page(Page * page);

  DiskManager();
  ~DiskManager();
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


//TEST_CODE
#ifdef TEST_DISKMAN
#include <iostream>
using namespace std;
int main() {

  
  DiskManager dm;


  Page *p = new Page(0,"file1");

  char* buf = p->get_data(0);

  buf[0] = '1';
  buf[1] = '\0';  

  dm.write_page(p);
  buf[0] = '0';
  buf[1] = '\0';
  cout << buf << endl;   
  dm.read_page(p); 
  cout << buf << endl;

  return 0;
}
#endif

