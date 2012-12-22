#include "DiskManager.h"

DiskManager::DiskManager()
 :file_(0)
{}




bool DiskManager::init_file()
{
#ifdef IO_DISK_M
  Utils::log("[DiskManager] open/create file: "+fname_);
#endif
  string mode;   
  if(Utils::check_existence(fname_, false))
    mode = "rb+";
  else
    mode = "wb+";
#ifdef IO_DISK_M
  Utils::log("[DiskManager] choose mode for file: "+mode);
#endif
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
#ifdef IO_DISK_M
    Utils::log("[DiskManager] update context(file):"+fname_+"->"+fname);
#endif
    fname_ = fname;
    if(file_ != NULL && fclose(file_) != 0){
      Utils::log("[DiskManager] Unable to close file: "+fname_,ERROR);
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
#ifdef IO_DISK_M
    Utils::log("[DiskManager] page is'not allocated, create virtual page befor first writing");
#endif
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
#ifdef IO_DISK_M
  Utils::log("[DiskManager] read in file page: "+ std::to_string(page->get_pid()) );
#endif
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
#ifdef IO_DISK_M
  Utils::log("[DiskManager] write in file page: "+ std::to_string(page->get_pid()));
#endif
  if( fwrite(page->get_data(),sizeof(char),Page::PAGE_SIZE,file_) != Page::PAGE_SIZE ){
    Utils::log("[DiskManager] can't write in file",ERROR);
    return false;
  }
  page->reset_dirty();
  return true;
}

DiskManager::~DiskManager()
{
  fclose(file_);
}


