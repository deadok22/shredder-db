#include "SqlParser.h"
#include <boost/regex.hpp>

using namespace boost;

SqlStatement SqlParser::parse(std::string const & statement_text) {
  Utils::info(" [SqlParser] entered sql statement parsing");
  //TODO write regexps for each type of queries
  //TODO deal with multiline strings
  regex rgx("^\\s*SELECT\\s+(\\*|(\\s*\\w+\\s*,\\s*)*\\s*\\w+)\\s+FROM\\s+\\w+\\s+$", regex_constants::icase);
  Utils::info(std::string(" [SqlParser] parsed successfully: ") + (regex_match(statement_text, rgx) ? "true" : "false"));
  Utils::info(" [SqlParser] leaving sql statement parsing");
  return CreateTableStatement("aaa", std::vector<TableColumn>());
}
