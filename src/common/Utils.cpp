#include "Utils.h"

using namespace std;

void Utils::log(string const & msg, LogLevel log_level, ostream & stream) {
#ifdef DEBUG  
  string str_log_lvl("\033[32m[INFO] ");
  if (log_level == ERROR) {
    str_log_lvl = "\033[31m[ERROR] ";
  } else if (log_level == WARNING) {
    str_log_lvl = "\033[33m[WARNING] ";
  }
  cout << str_log_lvl << "\033[37m" << msg << "\033[00m" << endl; 
#endif
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
