#pragma once
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <cstdlib>

using namespace std;

enum LogLevel{ INFO = 0x0, WARNING = 0x1, ERROR = 0x2 };

class Utils {
public:
  static void log(string const & msg, LogLevel log_level = INFO, ostream & stream = std::cout);
  //checks wether FS entry with given name exists. If is_dir is set checks that entry is directory otherwise entry must be file
  static bool check_existence(string const & path, bool is_dir);
  static string get_table_name(string const & path);

  static void critical_error();
  static void info(string const & msg, ostream & stream = std::cout);
  static void warning(string const & msg, ostream & stream = std::cout);
  static void error(string const & msg, ostream & stream = std::cout);
};

//TEST_CODE
#ifdef TEST_UTILS
int main() {
  Utils::log("LogInfo", INFO);
  Utils::log("LogError", ERROR);
  Utils::log("LogWarning", WARNING);
  cout << Utils::get_table_name("./country/country") << endl << Utils::get_table_name("c:\\country\\country") << endl;
  cout << Utils::check_existence("./makefile", false) << endl;
}
#endif

