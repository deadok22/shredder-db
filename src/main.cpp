#include <iostream>
#include <cstdlib>
#include <string>
#include <algorithm>

#include "backend/BufferManager.h"
#include "common/InfoPool.h"
#include "common/Utils.h"
#include "core/DBFacade.h"
#include "core/MetaDataProvider.h"
#include "sqlparser/SqlParser.h"

std::string read_command() {
  std::string command;
  std::string command_chunck;

  unsigned empty_in_a_row = 0;

  while (getline(std::cin, command_chunck)) {
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
  return command;
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

  std::cout << "Stat: Rec size is " << metadata->record_size() << "; Recs per page: " <<  metadata->records_per_page();
  std::cout << "; Bit mask size is " << metadata->space_for_bit_mask() << ";" << std::endl;
  if (metadata->indices_size() != 0) {
    std::cout << "Indices:" << std::endl;
    for (int index_i = 0; index_i < metadata->indices_size(); ++index_i) {
      TableMetaData_IndexMetadata index = metadata->indices(index_i);
      std::cout << "  " << index_i + 1 << ". " << index.name() << " on attributes ";
      for (int key_i = 0; key_i < index.keys_size(); ++key_i) {
        TableMetaData_IndexMetadata_KeyInfo keyInfo = index.keys(key_i);
        std::cout << keyInfo.name() << "(" << (keyInfo.asc() ? "ASC" : "DESC") << "); ";
      }
      std::cout << "based on " << (index.type() == 0 ? "HASH" : "BTREE") << std::endl;
    }
  }
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
    std::string::size_type first_space = command.find_first_of(" ");
    std::string fst_token = first_space == string::npos ? command : command.substr(0, first_space);
    capitalize(&fst_token);

#ifdef MAIN_DBG
    Utils::info("[REPL] execute \"" + command +"\"");
#endif
    if (fst_token.compare("QUIT") == 0 || fst_token.compare("EXIT") == 0) {
      return;
    } else if (fst_token.compare("PURGE") == 0) {
      BufferManager &bm = BufferManager::get_instance();
      bm.purge();
      std::cout << "Purge has been done" << std::endl;
    } else if (fst_token.compare("BMST") == 0) {
      std::cout << "BufferManager state:" << std::endl;
      std::cout << "  Pinned pages: " + std::to_string(BufferManager::get_instance().get_pinned_page_count()) << std::endl;
      BufferManager::get_instance().print_pinned_page();
    } else if (fst_token.compare("ABOUT") == 0) {
      std::string table_name = command.substr(6);
#ifdef MAIN_DBG
      Utils::info("[REPL] 'about' was called for " + table_name);
#endif
      describe_table(table_name);
    } else {
      SqlStatement const * stmt = parser.parse(command);

      DBFacade::get_instance()->execute_statement(stmt);
      delete stmt;
    }
  }
}

int main(int argc, char ** argv) {
  if (argc < 3) {
    std::cout << "Not enough arguments. Args: <path to dir> <max page number>" << std::endl;
    return 0;
  }

  std::cout << "Welcome to ShredderDB." << std::endl;

  InfoPool::get_instance().get_db_info().root_path = argv[1];
  InfoPool::get_instance().get_db_info().max_page_cnt = std::stoi(argv[2]);
#ifdef MAIN_DBG
  Utils::info("[Common] Max page # is " + std::to_string(InfoPool::get_instance().get_db_info().max_page_cnt));
#endif
  if (InfoPool::get_instance().get_db_info().max_page_cnt == 0) {
    Utils::warning("[Common] Max buffer page size is 0.");
  }

  std::string &current_root_path = InfoPool::get_instance().get_db_info().root_path;
#ifdef MAIN_DBG
  Utils::info("[Common] DB root path is " + current_root_path);
#endif
  if (!Utils::check_existence(current_root_path, true)) {
#ifdef MAIN_DBG
    Utils::info("[Common] Creating missed DB directory " + current_root_path);
#endif
    mkdir(current_root_path.c_str(), 0777);
  }
  if (current_root_path[current_root_path.size() - 1] != '/') {
    current_root_path += '/';
  }

  repl();
}
