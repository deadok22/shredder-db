#include <fstream>
#include "diskmanager.h"
#include "common/Utils.h"
#include "common/InfoPool.h"

DiskManager::DiskManager()
 :file_(0)
{}

DiskManager::DiskManager(string const & name)
 :file_(0)
{
  init_context(name);
  Utils::log("[disk_manager] init object");
}

bool DiskManager::file_exists(string const& fname)
{
  return  std::ifstream(fname.c_str()) != NULL;
}

bool DiskManager::init_context(string const& fname)
{
  string file = InfoPool::get_instance()->get_db_info()->root_path + fname;
  Utils::log("[disk_manager] open/create file: "+file);
  string mode;

  if(file_exists(file))
    mode = "rb+";
  else
    mode = "wb+";

  file_ = fopen(file.c_str(),mode.c_str());
  if(!file_){
    Utils::log("[disk_manager] can't open file: "+file,ERROR);
    return false;
  }
  return true;
}


bool DiskManager::read_page(size_t page_id, char *buf)
{
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

}

bool DiskManager::deallocate(size_t page_id)
{

}

//TEST_CODE
#ifdef TEST_DISKMANAGER
#include <iostream>
int main() {
  DBInfo di;
  di.root_path = "./";
  di.page_size = 4096;
  InfoPool::get_instance()->set_db_info(di);
  DiskManager dm("file");

  char * buf = new char[InfoPool::get_instance()->get_db_info()->page_size];
  dm.read_page(10,buf);



  buf[0] = '1';
  std::cout<<buf<<std::endl;

  dm.write_page(0,buf);
  char * buf1 = new char[InfoPool::get_instance()->get_db_info()->page_size];;
  dm.read_page(0,buf1);
  std::cout<<buf1<<std::endl;
  return 0;
}
#endif


