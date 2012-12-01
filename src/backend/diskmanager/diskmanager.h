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
  bool dealloce(size_t page_id);

  DiskManager();

  DiskManager(string const& name);
  bool initContext(string const &file);
private:

  bool file_exists(string const& fname);
  FILE* file_;



};


