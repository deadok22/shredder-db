#include "Page.h"

Page::Page(size_t pid, string const& fname, size_t pin_c , bool dirty )
 :fname_(fname) , page_id_(pid),pin_count_(pin_c),dirty_(dirty),data_( new char[PAGE_SIZE] )
{
  std::fill(data_,data_+PAGE_SIZE,0);
}

Page::~Page()
{
  delete data_;
}

char* Page::get_data(bool is_read_only)
{
  dirty_ = !dirty_? !is_read_only : dirty_ ;
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

bool Page::is_unpinned()
{
  return pin_count_ == 0;
}

bool Page::is_dirty()
{
  return dirty_;
}

void Page::pin()
{
  ++pin_count_ ;
}

void Page::unpin()
{
  pin_count_ = std::max((int)pin_count_-1,0) ;
}

void Page::set_dirty()
{
  dirty_ = true;
}

void Page::reset_dirty()
{
  dirty_ = false;
}


