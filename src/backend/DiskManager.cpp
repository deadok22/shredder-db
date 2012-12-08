#include "DiskManager.h"
#include "../common/Utils.h"
#include "../common/InfoPool.h"
 

DiskManager::DiskManager()
 :file_(0)
{}

DiskManager::DiskManager(string const & name)
 :file_(0),fname_(name)
{
  init_context();
  Utils::log("[disk_manager] create object diskmanager");
}

 

bool DiskManager::init_context()
{
  string file = InfoPool::get_instance()->get_db_info()->root_path + fname_;
  Utils::log("[disk_manager] open/create file: "+file);
  string mode;

   
  if(Utils::check_existence(file, false))
    mode = "rb+";
  else
    mode = "wb+";

  Utils::log("[disk_manager] choose mode for file: "+mode);

   

  file_ = fopen(file.c_str(),mode.c_str());
  if(!file_){
    Utils::log("[disk_manager] can't open file: "+file,ERROR);
    return false;
  }
  return true;
}

bool DiskManager::update_context()
{
  string cur_file = InfoPool::get_instance()->get_db_info()->cur_file;
  if( cur_file != fname_){
    Utils::log("[disk_manager] update context(file):"+fname_+"->"+cur_file);
    fname_ = cur_file;
    return true;
  }
  return false;
}

bool DiskManager::read_page(size_t page_id, char *buf)
{
  if( update_context())
    if( !init_context())
      return false;
    
  size_t const PAGE_SIZE = InfoPool::get_instance()->get_db_info()->page_size;


  if(fseek(file_,page_id*PAGE_SIZE,SEEK_SET)){
    Utils::log("[disk_manager] can't change offset in file",ERROR);
    return false;
  }
  Utils::log("[disk_manager] read file");
  if( fread(buf,sizeof(char),PAGE_SIZE,file_) != PAGE_SIZE ){
    Utils::log("[disk_manager] can't read from file",ERROR);
    return false;
  }
  return true;
}

bool DiskManager::write_page(size_t page_id, char * buf)
{
  if( update_context())
    if( !init_context())
      return false;

  size_t const PAGE_SIZE = InfoPool::get_instance()->get_db_info()->page_size;

  if(fseek(file_,page_id*PAGE_SIZE,SEEK_SET)){
    Utils::log("[disk_manager] can't change offset in file",ERROR);
    return false;
  }
  Utils::log("[disk_manager] write file");
  if( fwrite(buf,sizeof(char),PAGE_SIZE,file_) != PAGE_SIZE ){
    Utils::log("[disk_manager] can't write in file",ERROR);
    return false;
  }
  return true;
}


size_t DiskManager::allocate()
{
	//stub
	//TO-DO bitmap for tracking free page
	Utils::log("[disk_manager] allocate (stub)");
  return 0;
}

bool DiskManager::deallocate(size_t page_id)
{
	//stub
  Utils::log("[disk_manager] de-allocate page (stub)");
  return true;
}

//TEST_CODE
#ifdef TEST_DISKMAN
#include <iostream>
using namespace std;
int main() {
  DBInfo di;
  di.root_path = "./";
  di.page_size = 4096;
  di.cur_file = "file";
  InfoPool::get_instance()->set_db_info(di);
  
  
  DiskManager dm("file1");

  char * buf = new char[InfoPool::get_instance()->get_db_info()->page_size];
  dm.read_page(10,buf);



  buf[0] = '1';
  cout<<buf<<endl;

  dm.write_page(0,buf);
  char * buf1 = new char[InfoPool::get_instance()->get_db_info()->page_size];;
  dm.read_page(0,buf1);
  cout<<buf1<<endl;
  return 0;
}
#endif


