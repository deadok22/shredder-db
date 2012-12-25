#pragma once
#include <string>

struct IndexOperationParams {
  //indexed data
  void * value;
  unsigned page_id;
  int slot_id;
  size_t value_size;
};