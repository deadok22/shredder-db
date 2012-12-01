#pragma once
#include <cstdio>
#include <cstdlib>
#include <string>


using std::string;

class DiskManager
{
public:
  bool read_page(size_t page_id, char *buf);
  bool write_page(size_t page_id, char *buf);

  size_t allocate();
  bool deallocate(size_t page_id);

  DiskManager();

  DiskManager(string const& name);
  bool init_context(string const &fname);// change handler of file
private:

   
  FILE* file_;

};


