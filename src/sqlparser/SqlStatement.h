#pragma once

#include <string>
#include <vector>
#include "../common/TableColumn.h"

enum SqlStatementType {
  UNKNOWN = 0,
  CREATE_TABLE = 1,
  CREATE_INDEX = 2,
  INSERT = 3,
  UPDATE = 4,
  DELETE = 5,
  SELECT = 6
};

class SqlStatement {
  public:
    SqlStatement() : type_(UNKNOWN) {}
    
    SqlStatementType get_type() const {
      return type_;
    }

  protected:
    SqlStatement(SqlStatementType type) : type_(type) {}

  private:
    SqlStatementType type_;
};

class UnknownStatement : public SqlStatement {
  public:
    UnknownStatement() : SqlStatement(UNKNOWN) {}
};

class TableTargetedStatement : public SqlStatement {
  protected:
    TableTargetedStatement(SqlStatementType type, std::string table_name)
      : SqlStatement(type), table_name_(table_name) {}
  public:
    std::string const & get_table_name() const {
      return table_name_;
    }
  private:
    std::string table_name_;
};

class CreateTableStatement : public TableTargetedStatement {
  public:
    CreateTableStatement(std::string table_name, std::vector<TableColumn> columns)
      : TableTargetedStatement(CREATE_TABLE, table_name), columns_(columns) {}
    
    std::vector<TableColumn> const & get_columns() const {
      return columns_;
    }
    
  private:
    std::vector<TableColumn> columns_;
};

//TODO add select from join statement
//TODO remove inheritance from TableTargetedStatement when you add join support
class SelectStatement : public TableTargetedStatement {
  public:
    SelectStatement(std::string table_name, std::vector<std::string> column_names)
      : TableTargetedStatement(SELECT, table_name), column_names_(column_names) {}
    
    std::vector<std::string> const & get_column_names() const {
      return column_names_;
    }
    
    bool is_select_asterisk() const {
      return column_names_.empty();
    }
    
  private:
    std::vector<std::string> column_names_;
};

class InsertStatement : public TableTargetedStatement {
  public:
    InsertStatement(std::string table_name, std::vector<std::string> column_names, std::vector<std::string> values)
      : TableTargetedStatement(INSERT, table_name), column_names_(column_names), values_(values) {}
      
    std::vector<std::string> const & get_column_names() const {
      return column_names_;
    }
    
    std::vector<std::string> const & get_values() const {
      return values_;
    }
    
  private:
    std::vector<std::string> column_names_;
    std::vector<std::string> values_;
};
