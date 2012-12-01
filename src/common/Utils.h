#pragma once
#include <iostream>
#include <string>

using namespace std;

enum LogLevel{ INFO = 0x0, WARNING = 0x1, ERROR = 0x2 };

class Utils {
public:
  static void log(string const & msg, LogLevel log_level = INFO, ostream & stream = std::cout);
};

//TEST_CODE
#ifdef TEST
int main() {
  Utils::log("LogInfo", INFO);
  Utils::log("LogError", ERROR);
  Utils::log("LogWarning", WARNING);
}
#endif

