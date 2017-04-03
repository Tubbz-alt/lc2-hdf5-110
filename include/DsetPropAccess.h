#ifndef DSET_PROP_ACCESS_HH
#define DSET_PROP_ACCESS_HH

#include <vector>
#include <string>
#include "hdf5.h"

struct DsetPropAccess {
  std::string name;
  hid_t h5type;
  hid_t access;
  hid_t proplist;
  std::vector<hsize_t> chunk_dims;

  DsetPropAccess() = default;
  DsetPropAccess(const DsetPropAccess &) = default;
  DsetPropAccess & operator=(const DsetPropAccess &) = default;

  DsetPropAccess(std::string _name, hid_t _h5type, const std::vector<hsize_t> & _chunk_dims);
  void close();

protected:
  hid_t create_access_for_chunk_cache();

};


#endif
