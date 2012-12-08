#include "SqlParser.h"
#include <boost/regex.hpp>

/*
 * SQL statement start regexps. Used to get type of sql statements.
 */
static const std::string SELECT_START_REGEX_TEXT("^\\s*SELECT\\s+.*$");
static const boost::regex SELECT_START_REGEX(SELECT_START_REGEX_TEXT, boost::regex_constants::icase);

static const std::string CREATE_TABLE_START_REGEX_TEXT("^\\s*CREATE\\s+TABLE\\s+.*$");
static const boost::regex CREATE_TABLE_START_REGEX(CREATE_TABLE_START_REGEX_TEXT, boost::regex_constants::icase);

static const std::string INSERT_START_REGEX_TEXT("^\\s*INSERT\\s+INTO\\s+.*$");
static const boost::regex INSERT_START_REGEX(INSERT_START_REGEX_TEXT, boost::regex_constants::icase);

SqlStatement const * SqlParser::parse(std::string const & statement_text) const {
  Utils::info(" [SqlParser] entered sql statement parsing");
  Utils::info(" [SqlParser] the statement is " + statement_text);

  SqlStatement const * parsedStatement = 0;
  SqlStatementType type = getSqlStatementType(statement_text);
  switch(type) {
    case SELECT : {
      parsedStatement = parseSelectStatement(statement_text);
      break;
    }
    case CREATE_TABLE : {
      parsedStatement = parseCreateTableStatement(statement_text);
      break;
    }
    case INSERT : {
      parsedStatement = parseInsertStatement(statement_text);
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

SqlStatementType SqlParser::getSqlStatementType(std::string const & statement_text) const {
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

SqlStatement const * SqlParser::parseCreateTableStatement(std::string const & statement_text) const {
  //TODO do actual parsing
  return new CreateTableStatement("NOT PARSED YET", std::vector<TableColumn>());
}

SqlStatement const * SqlParser::parseInsertStatement(std::string const & statement_text) const {
  //TODO do actual parsing
  return new InsertStatement("NOT PARSED YET", std::vector<std::string>(), std::vector<std::string>());
}

SqlStatement const * SqlParser::parseSelectStatement(std::string const & statement_text) const {
  static const std::string SELECT_REGEX_TEXT
        = "^\\s*SELECT\\s+(?'WHAT'(?:\\*|(?:(?:\\s*\\w+\\s*,)*\\s*\\w+)))\\s+FROM\\s+(?'TABLE'\\w+)\\s*$";
  static const boost::regex SELECT_REGEX(SELECT_REGEX_TEXT, boost::regex_constants::icase);
  
  Utils::info(" [SqlParser] parsing SELECT statement");
  
  boost::smatch match_results;
  if (!boost::regex_match(statement_text, match_results, SELECT_REGEX)) {
    Utils::info(" [SqlParser] invalid SELECT statement syntax");
    return new UnknownStatement();
  }
  
  boost::ssub_match what_match = match_results["WHAT"];
  boost::ssub_match table_match = match_results["TABLE"];
  
  Utils::info(" [SqlParser] parsed: SELECT <WHAT: " + what_match.str() + "> FROM <TABLE:" + table_match.str() + ">");
  
  if ("*" == what_match.str()) {
    return new SelectStatement(table_match.str(), std::vector<std::string>());
  }
  
  return new SelectStatement(table_match.str(), parseCommaSeparatedValues(what_match.str()));
}

std::vector<std::string> SqlParser::parseCommaSeparatedValues(std::string const & values_string) const {
  static const std::string COMMA_SEPARATED_VALUE_REGEX_TEXT
        = "^\\s*(?:(?:(?'VALUE'\\w+)\\s*,)|(?'VALUE'\\w+))\\s*";
  static const boost::regex COMMA_SEPARATED_VALUE_REGEX(COMMA_SEPARATED_VALUE_REGEX_TEXT, boost::regex_constants::icase);
  
  Utils::info(" [SqlParser] [parseCSV] parsing comma separated values");
  
  std::string::const_iterator start = values_string.begin();
  std::string::const_iterator end = values_string.end();
  std::vector<std::string> values;
  boost::smatch match_results;
  while (boost::regex_search(start, end, match_results, COMMA_SEPARATED_VALUE_REGEX)) {
    Utils::info(" [SqlParser] [parseCSV] parsed value: " + match_results["VALUE"].str());
    values.push_back(match_results["VALUE"].str());
    start = match_results[0].second;
  }
  
  if (0 == values.size()) {
    Utils::warning(" [SqlParser] [parseCSV] no values parsed");
  }
  
  return values;
}
