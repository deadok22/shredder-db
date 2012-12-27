#pragma once
#include <string>
#include <algorithm>
#include <ctime>

 
using std::string;

class Page
{
public:
  static const size_t PAGE_SIZE = 4096;  
  
  Page(size_t pid, string const& fname, size_t pin_c = 0 , bool dirty = false);
  char* get_data(bool is_read_only = true);
  size_t get_pid() const;
  string const & get_fname() const;
 
  void set_dirty();
  void reset_dirty();
  void pin(); // +1 
  void unpin(); // -1 if (pin > 0)
  
  bool is_dirty();
  bool is_unpinned();

  time_t get_tstamp();
  ~Page();
private:
   
  string fname_;
  size_t page_id_;
  size_t pin_count_;
  bool dirty_;
  char* data_;
  time_t tstamp_;

private:
  Page(Page const &);
  Page & operator=(Page const &);
};
