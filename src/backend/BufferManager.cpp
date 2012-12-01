#include "BufferManager.h"























//TEST_CODE
#ifdef TEST_BUFF_MNG
#include <iostream>
using namespace std;
int main() {
  DBInfo di;
  di.root_path = "./";
  di.page_size = 4096;
  InfoPool::get_instance()->set_db_info(di);
  
   
  return 0;
}
#endif
