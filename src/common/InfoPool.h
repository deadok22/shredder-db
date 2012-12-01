#pragma once
#include <string>

using namespace std;

struct DBInfo {
  string root_path;
};

class InfoPool {
public:
  DBInfo * get_db_info();
  void set_db_info(DBInfo db_info);

  static InfoPool * get_instance();
private:
  DBInfo db_info_;
  static InfoPool * instance_; 
};

#ifdef TEST
#include <iostream>

int main() {
  DBInfo di;
  di.root_path = "test/";

  InfoPool::get_instance()->set_db_info(di);
  cout << InfoPool::get_instance()->get_db_info()->root_path << endl;
}

#endif

