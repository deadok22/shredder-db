#include "InfoPool.h"

InfoPool::InfoPool() {}
InfoPool::~InfoPool() {}

InfoPool & InfoPool::get_instance() {
  static InfoPool ip;  
  return ip;
}

DBInfo & InfoPool::get_db_info() {
  return db_info_;
}

void InfoPool::set_db_info(DBInfo const & db_info) {
  db_info_ = db_info;
}

