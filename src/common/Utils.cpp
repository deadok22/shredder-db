#include "Utils.h"

using namespace std;

void Utils::log(string const & msg, LogLevel log_level, ostream & stream) {
#ifdef DEBUG  
  string str_log_lvl("[INFO]");
  if (log_level == ERROR) {
    str_log_lvl = "[ERROR]";
  } else if (log_level == WARNING) {
    str_log_lvl = "[WARNING]";
  }
  cout << str_log_lvl << msg << endl; 
#endif
}