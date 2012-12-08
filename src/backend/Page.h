#pragma once
#include "../common/InfoPool.h" 
 

class Page
{
public:
  Page(size_t pid,char* data = 0,size_t pin_c = 1, bool dirty = false)
   :page_id_(pid),pin_count_(pin_c),dirty_(dirty),data_( data? data : new char[InfoPool::get_instance()->get_db_info()->page_size] )
  {
  }
   
  char* get_data();
  size_t get_pid() const;
  

  void set_dirty();
  void set_pin(); // +1 
  void set_unpin(); // -1 if (pin > 0)
  
  bool isDirty();
  bool isUnpinned();
  ~Page();
private:
   
  size_t page_id_;
  size_t pin_count_;
  bool dirty_;
  char* data_;
};
