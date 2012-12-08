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
  bool update_context();
  bool init_context();// change handler of file
private:
  DiskManager(DiskManager const&);
  DiskManager& operator=(DiskManager const&);
 
  string fname_;
  FILE* file_;

};


