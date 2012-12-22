#include "SqlParser.h"
#include <boost/regex.hpp>
#include <string>

/*
 * SQL statement start regexps. Used to get type of sql statements.
 */
static const std::string SELECT_START_REGEX_TEXT("^\\s*SELECT\\s+.*$");
static const boost::regex SELECT_START_REGEX(SELECT_START_REGEX_TEXT, boost::regex_constants::icase);

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

SqlStatement const * SqlParser::parse(std::string const & statement_text) const {
  Utils::info(" [SqlParser] entered sql statement parsing");
  Utils::info(" [SqlParser] the statement is " + statement_text);

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
      //TODO implement
      Utils::error(" [SqlParser] CREATE_INDEX parser is not implemented yet");
      break;
    }
    case UPDATE : {
      //TODO implement
      Utils::error(" [SqlParser] UPDATE parser is not implemented yet");
      break;
    }
    case DELETE : {
      //TODO implement
      Utils::error(" [SqlParser] DELETE parser is not implemented yet");
      break;
    }
    case UNKNOWN : {
      parsedStatement = new UnknownStatement();
      break;
    }
    default : {
      Utils::error(" [SqlParser] unexpected SqlStatementType enum value");
      break;
    }
  }
  Utils::info(" [SqlParser] leaving sql statement parsing");
  return parsedStatement;
}

SqlStatementType SqlParser::get_sql_statement_type(std::string const & statement_text) const {
  Utils::info(" [SqlParser] recognizing sql statement type");
  
  if (boost::regex_match(statement_text, SELECT_START_REGEX)) {
    Utils::info(" [SqlParser] the statement is SELECT");
    return SELECT;
  }
  if (boost::regex_match(statement_text, CREATE_TABLE_START_REGEX)) {
    Utils::info(" [SqlParser] the statement is CREATE TABLE");
    return CREATE_TABLE;
  }
  if (boost::regex_match(statement_text, INSERT_START_REGEX)) {
    Utils::info(" [SqlParser] the statement is INSERT");
    return INSERT;
  }
  //TODO add more statement types
  Utils::warning(" [SqlParser] the statement is not recognized");
  return UNKNOWN;
}

SqlStatement const * SqlParser::parse_create_table_statement(std::string const & statement_text) const {
  static const std::string CREATE_TABLE_REGEX_TEXT
        = "^\\s*CREATE\\s+TABLE\\s+(?'TABLE'\\w+)\\s*\\((?'COLUMNS'" + NAMES_AND_TYPES_REGEX_TEXT + ")\\)$";
  static const boost::regex CREATE_TABLE_REGEX(CREATE_TABLE_REGEX_TEXT, boost::regex_constants::icase);
  
  Utils::info(" [SqlParser] parsing CREATE TABLE statement");
  
  boost::smatch match_results;
  if (!boost::regex_match(statement_text, match_results, CREATE_TABLE_REGEX)) {
    Utils::warning(" [SqlParser] invalid CREATE TABLE statement syntax");
    return new UnknownStatement();
  }
  
  std::string table = match_results["TABLE"].str();
  std::vector<TableColumn> table_columns = parse_table_columns(match_results["COLUMNS"].str());
  if (0 == table_columns.size()) {
    Utils::warning(" [SqlParser] invalid CREATE TABLE syntax: passed column definitions are empty or they contain errors");
    return new UnknownStatement();
  }
  
  return new CreateTableStatement(table, table_columns);
}

SqlStatement const * SqlParser::parse_insert_statement(std::string const & statement_text) const {
  static const std::string INSERT_REGEX_TEXT
        = "^\\s*INSERT\\s+INTO\\s+(?'TABLE'\\w+)\\s*\\((?'COLUMNS'"
          + CSV_REGEX_TEXT
          + ")\\)\\s*VALUES\\s*\\((?'VALUES'" 
          + CSV_REGEX_TEXT
          + ")\\)$";
  static const boost::regex INSERT_REGEX(INSERT_REGEX_TEXT, boost::regex_constants::icase);
  
  Utils::info(" [SqlParser] parsing INSERT statement");
  
  boost::smatch match_results;
  if (!boost::regex_match(statement_text, match_results, INSERT_REGEX)) {
    Utils::warning(" [SqlParser] invalid INSERT statement syntax");
    return new UnknownStatement();
  }
  
  std::string table = match_results["TABLE"].str();
  std::vector<std::string> column_names = parse_comma_separated_values(match_results["COLUMNS"].str());
  std::vector<std::string> values = parse_comma_separated_values(match_results["VALUES"].str());
  
  if (column_names.size() != values.size()) {
    Utils::warning(" [SqlParser] invalid INSERT statement syntax: columns count is not equal to values count");
    return new UnknownStatement();
  }
  
  return new InsertStatement(table, column_names, values);
}

SqlStatement const * SqlParser::parse_select_statement(std::string const & statement_text) const {
  static const std::string SELECT_REGEX_TEXT
        = "^\\s*SELECT\\s+(?'WHAT'(?:\\*|" + CSV_REGEX_TEXT + "))\\s+FROM\\s+(?'TABLE'\\w+)\\s*$";
  static const boost::regex SELECT_REGEX(SELECT_REGEX_TEXT, boost::regex_constants::icase);
  
  Utils::info(" [SqlParser] parsing SELECT statement");
  
  boost::smatch match_results;
  if (!boost::regex_match(statement_text, match_results, SELECT_REGEX)) {
    Utils::warning(" [SqlParser] invalid SELECT statement syntax");
    return new UnknownStatement();
  }
  
  boost::ssub_match what_match = match_results["WHAT"];
  boost::ssub_match table_match = match_results["TABLE"];
  
  Utils::info(" [SqlParser] parsed: SELECT <WHAT: " + what_match.str() + "> FROM <TABLE:" + table_match.str() + ">");
  
  if ("*" == what_match.str()) {
    return new SelectStatement(table_match.str(), std::vector<std::string>());
  }
  
  return new SelectStatement(table_match.str(), parse_comma_separated_values(what_match.str()));
}

std::vector<std::string> SqlParser::parse_comma_separated_values(std::string const & values_string) const {
  static const std::string COMMA_SEPARATED_VALUE_REGEX_TEXT
        = "^\\s*(?:(?:\"(?'VALUE'(?:(?:\"\")|[^\"])*)\")|(?'VALUE'[^,]*))\\s*(?:$|,)";
        //"^\\s*(?:(?:(?:\"(?'VALUE'(?:\"\"|))\"|(?'VALUE' NO QUOTES))\\s*,)|(?'VALUE' LAST VALUE))\\s*";
  static const boost::regex COMMA_SEPARATED_VALUE_REGEX(COMMA_SEPARATED_VALUE_REGEX_TEXT, boost::regex_constants::icase);
  
  Utils::info(" [SqlParser] [parseCSV] parsing comma separated values");
  
  std::string::const_iterator start = values_string.begin();
  std::string::const_iterator end = values_string.end();
  std::vector<std::string> values;
  boost::smatch match_results;
  while (start != end && boost::regex_search(start, end, match_results, COMMA_SEPARATED_VALUE_REGEX)) {
    Utils::info(" [SqlParser] [parseCSV] parsed value: " + match_results["VALUE"].str());
    values.push_back(match_results["VALUE"].str());
    start = match_results[0].second;
  }
  
  if (0 == values.size()) {
    Utils::warning(" [SqlParser] [parseCSV] no values parsed");
  }
  
  return values;
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
  
  Utils::info(" [SqlParser] [parseTC] parsing table columns");
  
  std::string::const_iterator start = columns_string.begin();
  std::string::const_iterator end = columns_string.end();
  std::vector<TableColumn> columns;
  boost::smatch match_results;
  while(boost::regex_search(start, end, match_results, COLUMN_NAME_AND_TYPE_REGEX)) {
    std::string name = match_results["NAME"].str();
    std::string type = match_results["TYPE"].str();
    Utils::info(" [SqlParser] [parseTC] parsed column: " + name + " of type " + type);
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
          Utils::warning(" [SqlParser] [parseTC] VARCHAR size argument is not positive");
        }
        columns.push_back(TableColumn(name, DataType::get_varchar((size_t) size)));
        break;
      }
      //whatever else
      default : {
        Utils::warning(" [SqlParser] [parseTC] unexpected column type: " + type);
        return std::vector<TableColumn>();
      }
    }
//    if ("INT" == type) {
//      columns.push_back(TableColumn(name, DataType::get_int()));
//    } else if ("DOUBLE" == type) {
//      columns.push_back(TableColumn(name, DataType::get_double()));
//    } else if ("VARCHAR" == type) {
//      int size = std::stoi(match_results["SIZE"].str());
//      if (size < 1) {
//        Utils::warning(" [SqlParser] [parseTC] VARCHAR size argument is not positive");
//      }
//      columns.push_back(TableColumn(name, DataType::get_varchar((size_t) size)));
//    } else {
//      Utils::warning(" [SqlParser] [parseTC] unexpected column type: " + type);
//      return std::vector<TableColumn>();
//    }
    start = match_results[0].second;
  }
  
  if (0 == columns.size()) {
    Utils::warning(" [SqlParser] [parseTC] no columns parsed");
  }
  
  return columns;
}
