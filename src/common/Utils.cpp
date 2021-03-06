#include "Utils.h"

#include "InfoPool.h"

using namespace std;

void Utils::log(string const & msg, LogLevel log_level, ostream & stream) {
#ifdef DEBUG  
  string str_log_lvl("\033[32m[INFO] ");
  if (log_level == ERROR) {
    str_log_lvl = "\033[31m[ERROR]   ";
  } else if (log_level == WARNING) {
    str_log_lvl = "\033[33m[WARNING] ";
  }
  cout << str_log_lvl << "\033[37m" << msg << "\033[00m" << endl; 
#endif
}

void Utils::critical_error() {
  cout << "\033[31m[CRITICAL_ERROR]   " << "tortoise won, but we'll be back\033[00m" << endl;
  exit(EXIT_FAILURE);
}

string Utils::get_table_name(string const & path) {
  size_t found;
  found = path.find_last_of("/\\");
  return path.substr(found+1);
}

bool Utils::check_existence(string const & path, bool is_dir) {
  struct stat root_path_info;
  bool some_entry_exists = stat(path.c_str(), &root_path_info) == 0;
  return some_entry_exists && (is_dir ? S_ISDIR(root_path_info.st_mode) : S_ISREG(root_path_info.st_mode));
}

void Utils::info(string const & msg, ostream & stream) {
#ifdef DEBUG  
  Utils::log(msg, INFO, stream);
#endif
}

void Utils::warning(string const & msg, ostream & stream) {
#ifdef DEBUG  
  Utils::log(msg, WARNING, stream);
#endif
}

void Utils::error(string const & msg, ostream & stream) {
#ifdef DEBUG  
  Utils::log(msg, ERROR, stream);
#endif
}

std::string Utils::get_table_dir(std::string const & table_name) {
  return InfoPool::get_instance().get_db_info().root_path + table_name;
}
