#include "Page.h"

Page::Page(size_t pid, string const& fname, size_t pin_c = 0, bool dirty = false)
 :fname_(fname) , page_id_(pid),pin_count_(pin_c),dirty_(dirty),data_( new char[PAGE_SIZE] )
{
  std::fill(data,data+PAGE_SIZE,0);
}

Page::~Page()
{
  delete data_;
}

char* Page::get_data()
{
  return data_;
}

size_t Page::get_pid()  const
{
  return page_id_;  
}

string const & Page::get_fname() const
{
  return fname_;
}

bool Page::isUnpinned()
{
  return pin_count_ == 0;
}

bool Page::isDirty()
{
  return dirty_;
}

void pin()
{
  ++pin_count_ ;
}

void unpin()
{
  pin_count_ = std::max(pin_count_-1,0) ;
}

