#pragma once

#include "IndexOperationParams.h"
#include "TableMetadata.pb.h"

class IndexManager {
public:
  enum IndexType { HASH = 0, BTREE = 1};
public:
  virtual int look_up_value(IndexOperationParams * params) = 0;
  virtual bool insert_value(IndexOperationParams const & params) = 0;
  virtual bool delete_value(IndexOperationParams * params) = 0;
  virtual ~IndexManager() {}

  static size_t compute_key_size(TableMetaData const & t_meta, TableMetaData_IndexMetadata const & i_meta) {
    size_t key_size = 0;
    for (int i = 0; i < i_meta.keys_size(); ++i) {
      for (int attr_i = 0; attr_i < t_meta.attribute_size(); ++attr_i) {
        if (i_meta.keys(i).name().compare(t_meta.attribute(attr_i).name()) == 0) {
          key_size += t_meta.attribute(attr_i).size();
          break;
        }
      }
    }
    return key_size;
  }
  
  static void init_params_with_record(TableMetaData const & t_meta, TableMetaData_IndexMetadata const & i_meta, void * rec_data, IndexOperationParams * params) {
    size_t rec_offset = 0;
    size_t key_offset = 0;
    for (int i = 0; i < i_meta.keys_size(); ++i) {
      for (int attr_i = 0; attr_i < t_meta.attribute_size(); ++attr_i) {
        if (i_meta.keys(i).name().compare(t_meta.attribute(attr_i).name()) == 0) {
          memcpy((char *)params->value + key_offset, (char *)rec_data + rec_offset, t_meta.attribute(attr_i).size());
          key_offset += t_meta.attribute(attr_i).size();
        }
        rec_offset += t_meta.attribute(attr_i).size();
      }
    }
  }
};