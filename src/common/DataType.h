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
      DataType int_type_(INT);
      return int_type_;
    }
    static DataType get_double() {
      DataType int_type_(double_type_);
      return double_type_;
    }
    static DataType get_varchar(size_t size) {
      return DataType(VARCHAR, size);
    }
  private:
    static DataType const int_type_;
    static DataType const double_type_;
    TypeCode type_;
    size_t type_length_;
};

