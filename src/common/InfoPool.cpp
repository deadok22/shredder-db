#include "InfoPool.h"

InfoPool * InfoPool::instance_ = new InfoPool();

InfoPool * InfoPool::get_instance() {
  return InfoPool::instance_;
}

DBInfo * InfoPool::get_db_info() {
  return &db_info_;
}

void InfoPool::set_db_info(DBInfo db_info) {
  db_info_ = db_info;
}

