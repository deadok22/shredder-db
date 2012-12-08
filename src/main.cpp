#include <iostream>
#include <cstdlib>
#include <string>

#include "common/InfoPool.h"
#include "common/Utils.h"
#include "core/DBFacade.h"
#include "sqlparser/SqlParser.h"

std::string read_command() {
  std::string command;
  std::string command_chunck;

  unsigned empty_in_a_row = 0;

  while (true) {
    getline(std::cin, command_chunck);
    if (command_chunck.empty()) {
      if (++empty_in_a_row == 2) {
        return command;
      }
      continue;
    } else {
      empty_in_a_row = 0;
    }

    command += command_chunck;
  }
}

void repl() {
  SqlParser parser;
  while (true) {
    std::string command = read_command();
    Utils::info("[REPL] execute \"" + command +"\"");
    if (command.compare("quit") == 0) {
      return;
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
  Utils::info("[Common] DB root path is " + InfoPool::get_instance()->get_db_info()->root_path);
  InfoPool::get_instance()->get_db_info()->max_page_cnt = std::stoi(argv[2]); 
  Utils::info("[Common] Max page # is " + std::to_string(InfoPool::get_instance()->get_db_info()->max_page_cnt));

  repl();
}
