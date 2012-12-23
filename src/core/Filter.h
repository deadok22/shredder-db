#pragma once

#include "TableMetaData.pb.h"

class Filter { //mock filter, don't forget copy ctr
public:
  virtual bool isOk(TableMetaData const & table, void * data) const;
  static Filter const ANY;
};