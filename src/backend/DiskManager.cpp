#include "DiskManager.h"

DiskManager::DiskManager()
 :file_(0)
{}




bool DiskManager::init_file()
{
  Utils::log("[DiskManager] open/create file: "+fname_);

  string mode;   
  if(Utils::check_existence(fname_, false))
    mode = "rb+";
  else
    mode = "wb+";

  Utils::log("[DiskManager] choose mode for file: "+mode);

  file_ = fopen(fname_.c_str(),mode.c_str());
  if(!file_){
    Utils::log("[DiskManager] can't open file: "+fname_,ERROR);
    return false;
  }
  return true;
}

bool DiskManager::update_context(string const & fname)
{
  if( fname != fname_){
    Utils::log("[DiskManager] update context(file):"+fname_+"->"+fname);
    fname_ = fname;
    if( fclose(file_) != 0){
      Utils::log("[DiskManager] can't closed file: "+fname_,ERROR);
      Utils::critical_error();
    }
    return init_file();
  }
  return true;
}

bool DiskManager::is_allocated(size_t page_id)
{
  fseek (file_, 0, SEEK_END);
  long int cur_page_cnt = ftell (file_) / Page::PAGE_SIZE;

  if( cur_page_cnt < (int)page_id+1){
    Utils::log("[DiskManager] page is'not allocated, create virtual page befor first writing");
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
    Utils::log("[DiskManager] can't change offset in file",ERROR);
    return false;
  }
  Utils::log("[DiskManager] read in file page: "+ std::to_string(page->get_pid()) );
  if( fread(page->get_data(),sizeof(char),Page::PAGE_SIZE,file_) != Page::PAGE_SIZE ){
    Utils::log("[DiskManager] can't read from file",ERROR);
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
    Utils::log("[DiskManager] can't change offset in file",ERROR);
    return false;
  }
  Utils::log("[DiskManager] write in file page: "+ std::to_string(page->get_pid()));
  if( fwrite(page->get_data(),sizeof(char),Page::PAGE_SIZE,file_) != Page::PAGE_SIZE ){
    Utils::log("[DiskManager] can't write in file",ERROR);
    return false;
  }
  page->reset_dirty();
  return true;
}

DiskManager::~DiskManager()
{
  fclose(file_) != 0);
}


