#include "SqlParser.h"
#include <boost/regex.hpp>
#include <string>

/*
 * SQL statement start regexps. Used to get type of sql statements.
 */
static const std::string SELECT_START_REGEX_TEXT("^\\s*SELECT\\s+.*$");
static const boost::regex SELECT_START_REGEX(SELECT_START_REGEX_TEXT, boost::regex_constants::icase);

static const std::string CREATE_INDEX_START_REGEX_TEXT("^\\s*CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+.*$");
static const boost::regex CREATE_INDEX_START_REGEX(CREATE_INDEX_START_REGEX_TEXT, boost::regex_constants::icase);

static const std::string CREATE_TABLE_START_REGEX_TEXT("^\\s*CREATE\\s+TABLE\\s+.*$");
static const boost::regex CREATE_TABLE_START_REGEX(CREATE_TABLE_START_REGEX_TEXT, boost::regex_constants::icase);

static const std::string INSERT_START_REGEX_TEXT("^\\s*INSERT\\s+INTO\\s+.*$");
static const boost::regex INSERT_START_REGEX(INSERT_START_REGEX_TEXT, boost::regex_constants::icase);

/*
 * Utility constants
 */
static const std::string CSV_REGEX_TEXT = "(?:(?:\\s*(?:\"(?:(?:\"\")|[^\"])*\")|(?:[^\\),][^,]*)\\s*,\\s*))*(?:(?:\\s*(?:\"(?:(?:\"\")|[^\"])*\")|(?:[^\\),][^,]*)))";
static const std::string COLUMN_NAME_AND_TYPE_REGEX_TEXT
      = "(?:\\s*(?'NAME'\\w+)\\s+(?:(?'TYPE'INT)|(?'TYPE'DOUBLE)|(?:(?'TYPE'VARCHAR)\\s*\\((?'SIZE'\\d+)\\)))\\s*)";
static const std::string NAMES_AND_TYPES_REGEX_TEXT
      = "(?:(?:\\s*"
        + COLUMN_NAME_AND_TYPE_REGEX_TEXT
        + "\\s*,)*\\s*"
        + COLUMN_NAME_AND_TYPE_REGEX_TEXT
        +")";
static const std::string PREDICATE_REGEX_TEXT = "\\s*(?'COLUMN'\\w+)\\s*(?'PREDICATE'(?:<=?)|(?:>=?)|=|(?:!=))\\s*(?:(?:\"(?'VALUE'(?:(?:\"\")|[^\"])*)\")|(?'VALUE'[^,\\s]+))";
static const std::string WHERE_CLAUSE_REGEX_TEXT
      = "WHERE\\s+(?'WHERE'"
        + PREDICATE_REGEX_TEXT
        + "(?:\\s+AND\\s+"
        + PREDICATE_REGEX_TEXT
        + "\\s*)*)";

SqlStatement const * SqlParser::parse(std::string const & statement_text) const {
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] entered sql statement parsing");
  Utils::info("[SqlParser] the statement is " + statement_text);
#endif
  SqlStatement const * parsedStatement = 0;
  SqlStatementType type = get_sql_statement_type(statement_text);
  switch(type) {
    case SELECT : {
      parsedStatement = parse_select_statement(statement_text);
      break;
    }
    case CREATE_TABLE : {
      parsedStatement = parse_create_table_statement(statement_text);
      break;
    }
    case INSERT : {
      parsedStatement = parse_insert_statement(statement_text);
      break;
    }
    case CREATE_INDEX : {
      parsedStatement = parse_create_index_statement(statement_text);
      break;
    }
    case UPDATE : {
      //TODO implement
      Utils::error("[SqlParser] UPDATE parser is not implemented yet");
      break;
    }
    case DELETE : {
      //TODO implement
      Utils::error("[SqlParser] DELETE parser is not implemented yet");
      break;
    }
    case UNKNOWN : {
      parsedStatement = new UnknownStatement();
      break;
    }
    default : {
      Utils::error("[SqlParser] unexpected SqlStatementType enum value");
      break;
    }
  }
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] leaving sql statement parsing");
#endif
  return parsedStatement;
}

SqlStatementType SqlParser::get_sql_statement_type(std::string const & statement_text) const {
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] recognizing sql statement type");
#endif
  if (boost::regex_match(statement_text, INSERT_START_REGEX)) {
#ifdef SQLPARSE_DBG
    Utils::info("[SqlParser] the statement is INSERT");
#endif
    return INSERT;
  }
  if (boost::regex_match(statement_text, SELECT_START_REGEX)) {
#ifdef SQLPARSE_DBG
    Utils::info("[SqlParser] the statement is SELECT");
#endif
    return SELECT;
  }
  if (boost::regex_match(statement_text, CREATE_TABLE_START_REGEX)) {
#ifdef SQLPARSE_DBG
    Utils::info("[SqlParser] the statement is CREATE TABLE");
#endif
    return CREATE_TABLE;
  }
  if (boost::regex_match(statement_text, CREATE_INDEX_START_REGEX)) {
#ifdef SQLPARSE_DBG
    Utils::info("[SqlParser] the statement is CREATE [UNIQUE] INDEX");
#endif
    return CREATE_INDEX;
  }
  //TODO add more statement types
  Utils::warning("[SqlParser] the statement is not recognized");
  return UNKNOWN;
}

SqlStatement const * SqlParser::parse_create_table_statement(std::string const & statement_text) const {
  static const std::string CREATE_TABLE_REGEX_TEXT
        = "^\\s*CREATE\\s+TABLE\\s+(?'TABLE'\\w+)\\s*\\((?'COLUMNS'" + NAMES_AND_TYPES_REGEX_TEXT + ")\\)$";
  static const boost::regex CREATE_TABLE_REGEX(CREATE_TABLE_REGEX_TEXT, boost::regex_constants::icase);
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] parsing CREATE TABLE statement");
#endif
  boost::smatch match_results;
  if (!boost::regex_match(statement_text, match_results, CREATE_TABLE_REGEX)) {
    Utils::warning("[SqlParser] invalid CREATE TABLE statement syntax");
    return new UnknownStatement();
  }
  
  std::string table = match_results["TABLE"].str();
  std::vector<TableColumn> table_columns = parse_table_columns(match_results["COLUMNS"].str());
  if (0 == table_columns.size()) {
    Utils::warning("[SqlParser] invalid CREATE TABLE syntax: passed column definitions are empty or they contain errors");
    return new UnknownStatement();
  }
  
  return new CreateTableStatement(table, table_columns);
}

SqlStatement const * SqlParser::parse_create_index_statement(std::string const & statement_text) const {
  static const std::string CREATE_INDEX_REGEX_TEXT
        = "^\\s*CREATE\\s+(?:(?'UNIQUE'UNIQUE)\\s+)?INDEX\\s+(?'INDEX'\\w+)\\s+ON\\s+(?'TABLE'\\w+)\\s*\\((?'COLUMNS'"
          + CSV_REGEX_TEXT
          + ")\\)\\s+USING\\s+(?'TYPE'(?:BTREE)|(?:HASH))";
  static const boost::regex CREATE_INDEX_REGEX(CREATE_INDEX_REGEX_TEXT, boost::regex_constants::icase);
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] parsing CREATE INDEX statement");
#endif
  boost::smatch match_results;
  if (!boost::regex_match(statement_text, match_results, CREATE_INDEX_REGEX)) {
    Utils::warning("[SqlParser] invalid CREATE INDEX statement syntax");
    return new UnknownStatement();
  }
  
  bool is_unique = 0 != match_results["UNIQUE"].str().size();
  std::string table = match_results["TABLE"].str();
  std::string index_name = match_results["INDEX"].str();
  std::string index_type = match_results["TYPE"].str();
  bool is_btree = 0 != index_type.size() && ('b' == index_type[0] || 'B' == index_type[0]);
  std::vector<CreateIndexStatement::Column> columns = parse_create_index_columns(match_results["COLUMNS"].str());
  if (0 == columns.size()) {
    Utils::warning("[SqlParser] invalid CREATE INDEX syntax: no columns specified");
    return new UnknownStatement();
  }
  
  return new CreateIndexStatement(index_name, table, columns, is_btree, is_unique);
}

SqlStatement const * SqlParser::parse_insert_statement(std::string const & statement_text) const {
  static const std::string INSERT_REGEX_TEXT
        = "^\\s*INSERT\\s+INTO\\s+(?'TABLE'\\w+)\\s*\\((?'COLUMNS'"
          + CSV_REGEX_TEXT
          + ")\\)\\s*VALUES\\s*\\((?'VALUES'" 
          + CSV_REGEX_TEXT
          + ")\\)$";
  static const boost::regex INSERT_REGEX(INSERT_REGEX_TEXT, boost::regex_constants::icase);
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] parsing INSERT statement");
#endif
  boost::smatch match_results;
  if (!boost::regex_match(statement_text, match_results, INSERT_REGEX)) {
    Utils::warning("[SqlParser] invalid INSERT statement syntax");
    return new UnknownStatement();
  }
  
  std::string table = match_results["TABLE"].str();
  std::vector<std::string> column_names = parse_comma_separated_values(match_results["COLUMNS"].str());
  std::vector<std::string> values = parse_comma_separated_values(match_results["VALUES"].str());
  
  if (column_names.size() != values.size()) {
    Utils::warning("[SqlParser] invalid INSERT statement syntax: columns count is not equal to values count");
    return new UnknownStatement();
  }
  
  return new InsertStatement(table, column_names, values);
}

SqlStatement const * SqlParser::parse_select_statement(std::string const & statement_text) const {
  static const std::string SELECT_REGEX_TEXT
        = "^\\s*SELECT\\s+(?'WHAT'(?:\\*|"
          + CSV_REGEX_TEXT
          + "))\\s+FROM\\s+(?'TABLE'\\w+)(?:\\s+"
          + WHERE_CLAUSE_REGEX_TEXT
          + ")?\\s*$";
  static const boost::regex SELECT_REGEX(SELECT_REGEX_TEXT, boost::regex_constants::icase);
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] parsing SELECT statement");
#endif
  boost::smatch match_results;
  if (!boost::regex_match(statement_text, match_results, SELECT_REGEX)) {
    Utils::warning("[SqlParser] invalid SELECT statement syntax");
    return new UnknownStatement();
  }
  
  boost::ssub_match what_match = match_results["WHAT"];
  boost::ssub_match table_match = match_results["TABLE"];
  WhereClause where_clause;
  if ("" != match_results["WHERE"].str()) {
    where_clause = parse_where_clause(match_results["WHERE"].str());
    if (where_clause.is_empty()) {
      Utils::warning("[SqlParser] invalid SELECT statement syntax: bad WHERE clause");
      return new UnknownStatement();
    }
  }
  
  if ("*" == what_match.str()) {
    return new SelectStatement(table_match.str(), std::vector<std::string>(), where_clause);
  }
  
  return new SelectStatement(table_match.str(), parse_comma_separated_values(what_match.str()), where_clause);
}

std::vector<std::string> SqlParser::parse_comma_separated_values(std::string const & values_string) const {
  static const std::string COMMA_SEPARATED_VALUE_REGEX_TEXT
        = "^\\s*(?:(?:\"(?'VALUE'(?:(?:\"\")|[^\"])*)\")|(?'VALUE'[^,]+))\\s*(?:$|,)";
  static const boost::regex COMMA_SEPARATED_VALUE_REGEX(COMMA_SEPARATED_VALUE_REGEX_TEXT, boost::regex_constants::icase);
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] [parseCSV] parsing comma separated values");
#endif
  std::string::const_iterator start = values_string.begin();
  std::string::const_iterator end = values_string.end();
  std::vector<std::string> values;
  boost::smatch match_results;
  while (boost::regex_search(start, end, match_results, COMMA_SEPARATED_VALUE_REGEX)) {
#ifdef SQLPARSE_DBG
    Utils::info("[SqlParser] [parseCSV] parsed value: " + match_results["VALUE"].str());
#endif
    values.push_back(match_results["VALUE"].str());
    start = match_results[0].second;
  }
  
  if (0 == values.size()) {
    Utils::warning("[SqlParser] [parseCSV] no values parsed");
  }
  
  return values;
}

/**
 * Returns an empty vector if any syntax error are found
 */
std::vector<CreateIndexStatement::Column> SqlParser::parse_create_index_columns(std::string const & columns_string) const {
  static const std::string PARSE_ONE_COLUMN_NAME_AND_DESC_ASC_REGEX_TEXT
        = "^\\s*(?'NAME'\\w+)\\s*(?'ORDER'(?:ASC)|(?:DESC))?\\s*(?:,|$)";
  static const boost::regex COLUMN_REGEX(PARSE_ONE_COLUMN_NAME_AND_DESC_ASC_REGEX_TEXT, boost::regex_constants::icase);
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] [parseIC] parsing index columns");
#endif
  std::string::const_iterator start = columns_string.begin();
  std::string::const_iterator end = columns_string.end();
  std::vector<CreateIndexStatement::Column> columns;
  boost::smatch match_results;
  while(boost::regex_search(start, end, match_results, COLUMN_REGEX)) {
    std::string name = match_results["NAME"].str();
    std::string order = match_results["ORDER"].str();
    bool is_descending = 0 != order.size() && ('d' == order[0] || 'D' == order[0]);
    
    Utils::info("[SqlParser] [parseIC] parsed index column: " + name + (is_descending ? " DESCENDING" : " ASCENDING"));
    
    columns.push_back(CreateIndexStatement::Column(name, is_descending));
    
    start = match_results[0].second;
  }
  
  if (0 == columns.size()) {
    Utils::warning("[SqlParser] [parseIC] no columns parsed");
  }
  
  return columns;
}

/**
 * Returns an empty vector if any syntax errors are found
 */
std::vector<TableColumn> SqlParser::parse_table_columns(std::string const & columns_string) const {
  static const std::string PARSE_ONE_COLUMN_NAME_AND_TYPE_REGEX_TEXT
        = "^(?:(?:" 
          + COLUMN_NAME_AND_TYPE_REGEX_TEXT 
          + "\\s*,)|" 
          + COLUMN_NAME_AND_TYPE_REGEX_TEXT 
          + ")";
  static const boost::regex COLUMN_NAME_AND_TYPE_REGEX(PARSE_ONE_COLUMN_NAME_AND_TYPE_REGEX_TEXT, boost::regex_constants::icase);
#ifdef SQLPARSE_DBG
  Utils::info("[SqlParser] [parseTC] parsing table columns");
#endif
  std::string::const_iterator start = columns_string.begin();
  std::string::const_iterator end = columns_string.end();
  std::vector<TableColumn> columns;
  boost::smatch match_results;
  while(boost::regex_search(start, end, match_results, COLUMN_NAME_AND_TYPE_REGEX)) {
    std::string name = match_results["NAME"].str();
    std::string type = match_results["TYPE"].str();
#ifdef SQLPARSE_DBG
    Utils::info("[SqlParser] [parseTC] parsed column: " + name + " of type " + type);
#endif
    switch (type[0]) {
      //INT
      case 'i' : {}
      case 'I' : {
        columns.push_back(TableColumn(name, DataType::get_int()));
        break;
      }
      //DOUBLE
      case 'd' : {}
      case 'D' : {
        columns.push_back(TableColumn(name, DataType::get_double()));
        break;
      }
      //VARCHAR
      case 'v' : {}
      case 'V' : {
        int size = std::stoi(match_results["SIZE"].str());
        if (size < 1) {
          Utils::warning("[SqlParser] [parseTC] VARCHAR size argument is not positive");
        }
        columns.push_back(TableColumn(name, DataType::get_varchar((size_t) size)));
        break;
      }
      //whatever else
      default : {
        Utils::warning("[SqlParser] [parseTC] unexpected column type: " + type);
        return std::vector<TableColumn>();
      }
    }
    start = match_results[0].second;
  }
  
  if (0 == columns.size()) {
    Utils::warning("[SqlParser] [parseTC] no columns parsed");
  }
  
  return columns;
}

WhereClause::PredicateType get_predicate_type(std::string const & predicate_text) {
  if ("=" == predicate_text) {
    return WhereClause::PredicateType::EQ;
  } else if ("!=" == predicate_text) {
    return WhereClause::PredicateType::NEQ;
  } else if ("<=" == predicate_text) {
    return WhereClause::PredicateType::LTOE;
  } else if (">=" == predicate_text) {
    return WhereClause::PredicateType::GTOE;
  } else if ("<" == predicate_text) {
    return WhereClause::PredicateType::LT;
  } else if (">" == predicate_text) {
    return WhereClause::PredicateType::GT;
  }
  return WhereClause::PredicateType::UNKNOWN;
}

WhereClause SqlParser::parse_where_clause(std::string const & predicates_string) const {
  static const std::string ONE_PREDICATE_REGEX_TEXT
        = "^" + PREDICATE_REGEX_TEXT + "(?:(?:\\s+AND\\s+)|(?:\\s*$))";
  static const boost::regex PREDICATE_REGEX(ONE_PREDICATE_REGEX_TEXT, boost::regex_constants::icase);
  
  Utils::info("[SqlParser] [parseWC] parsing WHERE clause");
  
  std::vector<WhereClause::Predicate> predicates;
  std::string::const_iterator start = predicates_string.begin();
  std::string::const_iterator end = predicates_string.end();
  boost::smatch match_results;
  while (boost::regex_search(start, end, match_results, PREDICATE_REGEX)) {
    std::string column = match_results["COLUMN"].str();
    std::string value = match_results["VALUE"].str();
    WhereClause::PredicateType pred_type = get_predicate_type(match_results["PREDICATE"].str());
    if (pred_type == WhereClause::PredicateType::UNKNOWN) {
      Utils::warning("[SqlParser] [parseWC] unknown predicate type");
      return WhereClause();
    }
    
    Utils::info("[SqlParser] [parseWC] parsed predicate " + match_results["PREDICATE"].str() + " on column " + column + " and value " + value);
    
    predicates.push_back(WhereClause::Predicate(pred_type, column, value));
    start = match_results[0].second;
  }
  
  if (predicates.empty()) {
    Utils::warning("[SqlParser] [parseWC] no predicates parsed");
    return WhereClause();
  }
  
  return WhereClause(predicates);
}
