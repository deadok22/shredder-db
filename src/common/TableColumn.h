#pragma once

#include <string>
#include "DataType.h"

class TableColumn {
  public:
    TableColumn(std::string name, DataType type) : name_(name), type_(type) {}
    
    std::string const & get_name() const {
      return name_;
    }
    
    DataType get_data_type() const {
      return type_;
    }

  private:
    std::string name_;
    DataType type_;
};
