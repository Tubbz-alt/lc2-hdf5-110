#include "DsetPropAccess.h"
#include "check_macros.h"


DsetPropAccess::DsetPropAccess(std::string _name, hid_t _h5type, const std::vector<hsize_t> & _chunk_dims) :
  name(_name),
  h5type(_h5type),
  access(H5P_DEFAULT),
  proplist(H5P_DEFAULT),
  chunk_dims(_chunk_dims)
{
  int rank = int(chunk_dims.size());
  hid_t new_plist = NONNEG( H5Pcreate(H5P_DATASET_CREATE) );
  NONNEG( H5Pset_chunk(new_plist, rank, &chunk_dims.at(0)) );
  proplist = new_plist;
  access = create_access_for_chunk_cache();
}


hid_t DsetPropAccess::create_access_for_chunk_cache() {
  hid_t new_access = NONNEG( H5Pcreate(H5P_DATASET_ACCESS) );
  size_t type_size_bytes = NONNEG( H5Tget_size( h5type ) );

  hsize_t num_elements_in_a_chunk = 1;
  for (auto iter = chunk_dims.begin(); iter != chunk_dims.end(); ++iter) {
    num_elements_in_a_chunk *= *iter;
  }
  hsize_t num_bytes_in_a_chunk = type_size_bytes * num_elements_in_a_chunk;

  size_t rdcc_nslots = 101;
  size_t rdcc_nbytes = num_bytes_in_a_chunk * 3;
  double rdcc_w0 = 0.75;
  NONNEG(H5Pset_chunk_cache(new_access, rdcc_nslots, rdcc_nbytes, rdcc_w0) );
  return new_access;
}


void DsetPropAccess::close() {
  if (proplist != H5P_DEFAULT) {
    NONNEG( H5Pclose( proplist ) );
    proplist = H5P_DEFAULT;
  }
  if (access != H5P_DEFAULT) {
    NONNEG( H5Pclose( access ) );
    access = H5P_DEFAULT;
  }
}

