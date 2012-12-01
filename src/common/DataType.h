#pragma once

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
          type_length_ = type_length;
          break;
        }
      }
    }
    TypeCode get_type_code() {
      return type_;
    }
  private:
    TypeCode type_;
    size_t type_length_;
};
