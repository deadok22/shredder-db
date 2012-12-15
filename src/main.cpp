#include <iostream>
#include <cstdlib>
#include <string>
#include <algorithm>

#include "common/InfoPool.h"
#include "common/Utils.h"
#include "core/DBFacade.h"
#include "core/MetaDataProvider.h"
#include "sqlparser/SqlParser.h"

std::string read_command() {
  std::string command;
  std::string command_chunck;

  unsigned empty_in_a_row = 0;

  while (true) {
    getline(std::cin, command_chunck);
    if (command_chunck.empty()) {
      if (++empty_in_a_row == 1) {
        return command;
      }
      continue;
    } else {
      empty_in_a_row = 0;
    }

    command += (command.size() > 0 ? " " : "") + command_chunck;
  }
}

void describe_table(string const & table_name) {
  TableMetaData * metadata = MetaDataProvider::get_instance()->get_meta_data(table_name);
  if (!metadata) {
    std::cout << "Error: Table with name " + table_name + " does not exist" << std::endl;
    return;
  }
  std::cout << "Table " + table_name + " has the following attributes: " << std::endl;
  for (int attr_ind = 0; attr_ind < metadata->attribute_size(); ++attr_ind) {
    std::cout << "  " + std::to_string(attr_ind + 1) + ". " << metadata->attribute(attr_ind).name() << " of type " ;
    std::cout << DataType::describe((TypeCode)metadata->attribute(attr_ind).type_name(), metadata->attribute(attr_ind).size());
  }
  std::cout << std::endl;
  //TODO show indeces
}

void capitalize(std::string * s) {
  for (unsigned i = 0; i < s->size(); ++i) {
    s->at(i) = std::toupper(s->at(i));
  }
}

void repl() {
  SqlParser parser;
  while (true) {
    std::string command = read_command();
    capitalize(&command);

    Utils::info("[REPL] execute \"" + command +"\"");
    if (command.compare("QUIT") == 0) {
      return;
    } if (command.find("ABOUT ") == 0) {
      std::string table_name = command.substr(6);
      Utils::info("[REPL] 'about' was called for " + table_name);
      describe_table(table_name);
    } else {
      SqlStatement const * stmt = parser.parse(command);

      DBFacade::get_instance()->execude_statement(stmt);
      delete stmt;
    }
  }
}

int main(int argc, char ** argv) {
  if (argc < 3) {
    std::cout << "Not enough arguments. Args: <path to dir> <max page number>" << std::endl;
    return 0;
  }

  InfoPool::get_instance()->get_db_info()->root_path = argv[1];
  InfoPool::get_instance()->get_db_info()->max_page_cnt = std::stoi(argv[2]); 
  Utils::info("[Common] Max page # is " + std::to_string(InfoPool::get_instance()->get_db_info()->max_page_cnt));

  std::string current_root_path = InfoPool::get_instance()->get_db_info()->root_path;
  Utils::info("[Common] DB root path is " + current_root_path);
  if (!Utils::check_existence(current_root_path, true)) {
    Utils::info("[Common] Creating missed DB directory " + current_root_path);
    mkdir(current_root_path.c_str(), 0777);
  }

  repl();
}
