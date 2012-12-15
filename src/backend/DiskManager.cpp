#include "DiskManager.h"

DiskManager::DiskManager()
 :file_(0)
{}




bool DiskManager::init_file()
{
  string file = InfoPool::get_instance()->get_db_info()->root_path + fname_;
  Utils::log("[disk_manager] open/create file: "+fname_);

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

bool DiskManager::update_context(string const & fname)
{
  if( fname != fname_){
    Utils::log("[disk_manager] update context(file):"+fname_+"->"+fname);
    fname_ = fname;
    return init_file();
  }
  return true;
}

bool DiskManager::is_allocated(size_t page_id)
{
  fseek (file_, 0, SEEK_END);
  long int cur_page_cnt = ftell (file_) / Page::PAGE_SIZE;

  if( cur_page_cnt < page_id+1){
    Utils::log("[disk_manager] page is'not allocated, create virtual page befor first writing");
    return false;
  }
  return true;
}


bool DiskManager::read_page(Page * page)
{
  if( !update_context(page->get_fname()))
    return false;
    
  if( !is_allocated(page->get_pid()) )
    return true;
   
  if(fseek(file_ , page->get_pid()*Page::PAGE_SIZE , SEEK_SET)){
    Utils::log("[disk_manager] can't change offset in file",ERROR);
    return false;
  }
  Utils::log("[disk_manager] read file");
  if( fread(page->get_data(),sizeof(char),Page::PAGE_SIZE,file_) != Page::PAGE_SIZE ){
    Utils::log("[disk_manager] can't read from file",ERROR);
    return false;
  }
  return true;
}

bool DiskManager::write_page(Page * page)
{
  if( !page->is_dirty() ) return true;
  if( !update_context(page->get_fname()))
    return false;
  
  if(fseek(file_ , page->get_pid()*Page::PAGE_SIZE , SEEK_SET)){
    Utils::log("[disk_manager] can't change offset in file",ERROR);
    return false;
  }
  Utils::log("[disk_manager] write file");
  if( fwrite(page->get_data(),sizeof(char),Page::PAGE_SIZE,file_) != Page::PAGE_SIZE ){
    Utils::log("[disk_manager] can't write in file",ERROR);
    return false;
  }
  return true;
}



//TEST_CODE
#ifdef TEST_DISKMAN
#include <iostream>
using namespace std;
int main() {
/*  DBInfo di;
  di.root_path = "./";


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
*/
  return 0;
}
#endif


