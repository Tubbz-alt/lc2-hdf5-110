#ifndef DSET_CREATION_HH
#define DSET_CREATION_HH

#include <vector>
#include <string>
#include "hdf5.h"

struct DsetCreation {
  std::string name;
  hid_t h5type;
  hid_t access;
  hid_t proplist;
  std::vector<hsize_t> start_dims;  

  DsetCreation() = default;
  DsetCreation(const DsetCreation &) = default;
  DsetCreation & operator=(const DsetCreation &) = default;

  DsetCreation(std::string _name, hid_t _h5type, const std::vector<hsize_t> & _start_dims, hsize_t chunk=0);
  static hid_t create_access_for_chunk_cache(const std::vector<hsize_t> &chunk_dims, size_t type_size_bytes);
  void close();
};


#endif
