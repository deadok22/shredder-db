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
  SELECT = 6,
  DROP   = 7
};

class SqlStatement {
  public:
    SqlStatement() : type_(UNKNOWN) {}
    
    SqlStatementType get_type() const {
      return type_;
    }

    virtual ~SqlStatement() {}

  protected:
    SqlStatement(SqlStatementType type) : type_(type) {}

  private:
    SqlStatementType type_;
};

class UnknownStatement : public SqlStatement {
  public:
    UnknownStatement() : SqlStatement(UNKNOWN) {}
    
    virtual ~UnknownStatement() {}
    
};

class TableTargetedStatement : public SqlStatement {
  protected:
    TableTargetedStatement(SqlStatementType type, std::string const & table_name)
      : SqlStatement(type), table_name_(table_name) {}
  public:
    std::string const & get_table_name() const {
      return table_name_;
    }
    
    virtual ~TableTargetedStatement() {}
    
  private:
    std::string table_name_;
};

class CreateTableStatement : public TableTargetedStatement {
  public:
    CreateTableStatement(std::string const & table_name, std::vector<TableColumn> columns)
      : TableTargetedStatement(CREATE_TABLE, table_name), columns_(columns) {}
    
    std::vector<TableColumn> const & get_columns() const {
      return columns_;
    }
    
    virtual ~CreateTableStatement() {}
    
  private:
    std::vector<TableColumn> columns_;
};

class CreateIndexStatement : public TableTargetedStatement {
  public:
    struct Column {
        std::string name;
        bool is_descending;
        Column(std::string const & column_name, bool is_desc = false)
          : name(column_name), is_descending(is_desc) {}
    };
  public:
    CreateIndexStatement(std::string const & index_name,
                         std::string const & table_name,
                         std::vector<CreateIndexStatement::Column> const & columns,
                         bool is_btree = false,
                         bool is_unique = false) :
        TableTargetedStatement(CREATE_INDEX, table_name),
        index_name_(index_name),
        columns_(columns),
        is_unique_(is_unique),
        is_btree_(is_btree) {}
    
    std::string const & get_index_name() const {
      return index_name_;
    }
    
    std::vector<CreateIndexStatement::Column> const & get_columns() const {
      return columns_;
    }
    
    bool is_unique() const {
      return is_unique_;
    }
    
    bool is_btree() const {
      return is_btree_;
    }
    
    virtual ~CreateIndexStatement() {}
    
  private:
    std::string index_name_;
    std::vector<CreateIndexStatement::Column> columns_;
    bool is_unique_;
    bool is_btree_;
};

class WhereClause {
public:
  enum PredicateType {EQ, NEQ, LT, LTOE, GT, GTOE, UNKNOWN};
  struct Predicate {
    WhereClause::PredicateType type;
    std::string column;
    std::string value;
    Predicate(WhereClause::PredicateType pt, std::string const & col, std::string const & val) :
      type(pt), column(col), value(val) {}
  };
public:
  WhereClause() {}
  
  WhereClause(std::vector<WhereClause::Predicate> const & predicates) : predicates_(predicates) {}
  
  bool is_empty() const {
    return predicates_.empty();
  }
  
  std::vector<WhereClause::Predicate> const & get_predicates() const {
    return predicates_;
  }
  
private:
  std::vector<WhereClause::Predicate> predicates_;
};

//TODO create a supertype with where clause.

class DeleteStatement : public TableTargetedStatement {
  public:
    DeleteStatement(std::string const & table_name, WhereClause const & where = WhereClause())
      : TableTargetedStatement(DELETE, table_name),
        where_clause_(where) {}
    
    WhereClause const & get_where_clause() const {
      return where_clause_;
    }
    
    bool has_where_clause() const {
      return !where_clause_.is_empty();
    }
    
    virtual ~DeleteStatement() {}
    
  private:
    WhereClause where_clause_;
};

//TODO remove inheritance from TableTargetedStatement when you add join support
class SelectStatement : public TableTargetedStatement {
  public:
    SelectStatement(std::string const & table_name, std::vector<std::string> const & column_names, WhereClause const & where = WhereClause()):
      TableTargetedStatement(SELECT, table_name),
      column_names_(column_names),
      where_clause_(where) {}
    
    std::vector<std::string> const & get_column_names() const {
      return column_names_;
    }
    
    WhereClause const & get_where_clause() const {
      return where_clause_;
    }
    
    bool is_select_asterisk() const {
      return column_names_.empty();
    }
    
    bool has_where_clause() const {
      return !where_clause_.is_empty();
    }
    
    virtual ~SelectStatement() {}
    
  private:
    std::vector<std::string> column_names_;
    WhereClause where_clause_;
};

class InsertStatement : public TableTargetedStatement {
  public:
    InsertStatement(std::string const & table_name, std::vector<std::string> const & column_names, std::vector<std::string> const & values)
      : TableTargetedStatement(INSERT, table_name), column_names_(column_names), values_(values) {}
      
    std::vector<std::string> const & get_column_names() const {
      return column_names_;
    }
    
    std::vector<std::string> const & get_values() const {
      return values_;
    }
    
    virtual ~InsertStatement() {}
    
  private:
    std::vector<std::string> column_names_;
    std::vector<std::string> values_;
};
//TODO
class UpdateStatement : public TableTargetedStatement {


};
//TODO
class DropStatement : public TableTargetedStatement {


};
