#pragma once

#include <string>

enum TypeCode {
  INT = 0,
  DOUBLE = 1,
  VARCHAR = 2
};

class DataType {
  public:
    DataType(TypeCode type, size_t type_length = 0) : type_(type) {
      switch(type) {
        case INT : {
          type_length_ = sizeof(int);
          break;
        }
        case DOUBLE : {
          type_length_ = sizeof(double);
          break;
        }
        case VARCHAR : {
          type_length_ = type_length * sizeof(char);
          break;
        }
      }
    }
    TypeCode get_type_code() {
      return type_;
    }
    size_t get_size() {
      return type_length_;
    }
    static DataType get_int() {
      return DataType(INT);
    }
    static DataType get_double() {
      return DataType(DOUBLE);
    }
    static DataType get_varchar(size_t size) {
      return DataType(VARCHAR, size);
    }
    static std::string describe(TypeCode type, size_t type_length = 0) {
      switch(type) {
        case INT : {
          return "INT";
          break;
        }
        case DOUBLE : {
          return "DOUBLE";
          break;
        }
        case VARCHAR : {
          return std::string("VARCHAR(") + std::to_string(type_length) + ")"; 
          break;
        }
      }
    }
  private:
    TypeCode type_;
    size_t type_length_;
};

