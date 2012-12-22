#pragma once
#include <string>

using namespace std;

struct DBInfo {
  string root_path;
  size_t max_page_cnt;
};

class InfoPool {
public:
  DBInfo & get_db_info();
  void set_db_info(DBInfo const& db_info);

  static InfoPool & get_instance();
private:
  InfoPool();
  ~InfoPool();
  
  DBInfo db_info_; 
};

#ifdef TEST_INFOPOOL
#include <iostream>

int main() {
  DBInfo di;
  di.root_path = "test/";

  InfoPool::get_instance()->set_db_info(di);
  cout << InfoPool::get_instance()->get_db_info()->root_path << endl;
}

#endif

