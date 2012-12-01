#pragma once

#include <string>
#include "../common/DataType.h"

enum SqlStatementType {
  CREATE_TABLE = 1,
  CREATE_INDEX = 2,
  INSERT = 3,
  UPDATE = 4,
  DELETE = 5
};

class SqlStatement {
  public:
   
  private:
    SqlStatementType type_;
    
};
