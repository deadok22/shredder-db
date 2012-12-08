#include "Page.h"




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

bool Page::isUnpinned()
{
  return pin_count_ == 0;
}

bool Page::isDirty()
{
  return dirty_;
}
