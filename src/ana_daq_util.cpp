#include <cstdio>
#include <stdexcept>
#include "hdf5.h"

#include "ana_daq_util.h"


void check_nonneg(long long int err, const char *msg, int lineno, const char *fname) {
  if (err < 0) {
    fprintf(::stderr, "ERROR: %s line=%d  file=%s\n", msg, lineno, fname);
    throw std::runtime_error("FATAL: check_nonneg");
  } 
}

void check_pos(long long int err, const char *msg, int lineno, const char *fname) {
  if (err <= 0) {
    fprintf(::stderr, "ERROR: %s line=%d  file=%s\n", msg, lineno, fname);
    throw std::runtime_error("FATAL: check_pos");
  } 
}


DsetInfo create_1d_dataset(hid_t parent, const char *dset, hid_t h5_type, hsize_t chunk_size, size_t type_size_bytes) {
  int rank = 1;
  static hsize_t current_dims = 0;
  static hsize_t maximum_dims = H5S_UNLIMITED;
  hid_t space_id = NONNEG( H5Screate_simple(rank, &current_dims, &maximum_dims) );
  hid_t plist_id = NONNEG( H5Pcreate(H5P_DATASET_CREATE) );
  int chunk_rank = 1;
  NONNEG(H5Pset_chunk(plist_id, chunk_rank, &chunk_size) );
  
  hid_t access_id = NONNEG( H5Pcreate(H5P_DATASET_ACCESS) );
  
  size_t rdcc_nslots = 101;
  size_t rdcc_nbytes = chunk_size * type_size_bytes * 3;
  double rdcc_w0 = 0.75;
  NONNEG(H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0) );

  hid_t h5_dset = NONNEG( H5Dcreate2(parent, dset, h5_type, space_id, H5P_DEFAULT, plist_id, access_id) );

  NONNEG( H5Sclose(space_id) );
  NONNEG( H5Pclose(plist_id) );
  NONNEG( H5Pclose(access_id) );
  
  return DsetInfo(h5_dset, 0L);
}

DsetInfo create_4d_short_dataset(hid_t parent, const char *dset,
                                 int dim1, int dim2, int dim3, 
                                 int chunk_size) {
  int rank = 4;
  static hsize_t current_dims[4] = {0, hsize_t(dim1), hsize_t(dim2), hsize_t(dim3)};
  static hsize_t maximum_dims[4] = {H5S_UNLIMITED, hsize_t(dim1), hsize_t(dim2), hsize_t(dim3)};
  static hsize_t chunk_dims[4] = {hsize_t(chunk_size),  hsize_t(dim1), hsize_t(dim2), hsize_t(dim3)};
  
  hid_t space_id = NONNEG( H5Screate_simple(rank, current_dims, maximum_dims) );
  hid_t plist_id = NONNEG( H5Pcreate(H5P_DATASET_CREATE) );
  NONNEG( H5Pset_chunk(plist_id, rank, chunk_dims) );
  
  //  hid_t access_id = NONNEG( H5Pcreate(H5P_DATASET_ACCESS) );
  
  //  size_t rdcc_nslots = 101;
  //  size_t rdcc_nbytes = chunk_size * type_size_bytes * 3;
  //  double rdcc_w0 = 0.75;
  //  NONNEG(H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0) );

  //  hid_t h5_dset = NONNEG( H5Dcreate2(parent, dset, h5_type, space_id, H5P_DEFAULT, plist_id, access_id) );
  hid_t h5_dset = NONNEG( H5Dcreate2(parent, dset,  H5T_NATIVE_SHORT, space_id, H5P_DEFAULT, plist_id, H5P_DEFAULT) );

  NONNEG(H5Sclose(space_id) );
  NONNEG(H5Pclose(plist_id) );
  //  NONNEG(H5Pclose(access_id) );
  
  return DsetInfo(h5_dset, 0);
}

hsize_t append_many_to_1d_dset(DsetInfo &dset_info, hsize_t count, long *data) {
  hsize_t start = dset_info.extent;
  dset_info.extent += count;
  NONNEG( H5Dset_extent(dset_info.dset_id, &dset_info.extent) );
  hid_t dspace_id = NONNEG( H5Dget_space(dset_info.dset_id) );
  NONNEG( H5Sselect_hyperslab(dspace_id, H5S_SELECT_SET,
                              &start, NULL, &count, NULL) );
  hid_t memory_dspace_id = NONNEG( H5Screate_simple(1, &count, &count) );
  NONNEG( H5Dwrite(dset_info.dset_id,
                   H5T_NATIVE_LONG,
                   memory_dspace_id,
                   dspace_id,
                   H5P_DEFAULT,
                   data) );
  NONNEG( H5Sclose(dspace_id) );    
  NONNEG( H5Sclose(memory_dspace_id) );    
  
  return start;
}

void append_to_1d_dset(DsetInfo &dset_info, long value) {
  hsize_t current_size = dset_info.extent;
  dset_info.extent += 1;
  static hsize_t count = 1; 
  NONNEG( H5Dset_extent(dset_info.dset_id, &dset_info.extent) );
  hid_t dspace_id = NONNEG( H5Dget_space(dset_info.dset_id) );
  NONNEG( H5Sselect_hyperslab(dspace_id, H5S_SELECT_SET,
                              &current_size, NULL, &count, NULL) );
  hid_t memory_dspace_id = NONNEG( H5Screate_simple(1, &count, &count) );
  NONNEG( H5Dwrite(dset_info.dset_id,
                         H5T_NATIVE_LONG,
                         memory_dspace_id,
                         dspace_id,
                         H5P_DEFAULT,
                         &value) );
  NONNEG( H5Sclose(dspace_id) );    
  NONNEG( H5Sclose(memory_dspace_id) );    
}

void append_to_4d_short_dset(DsetInfo &dset_info, int dim1, int dim2, int dim3, short *data) {
  hsize_t current = dset_info.extent;
  dset_info.extent += 1;
  hsize_t new_size[4] = {dset_info.extent, hsize_t(dim1), hsize_t(dim2), hsize_t(dim3)};
  NONNEG( H5Dset_extent(dset_info.dset_id, new_size) );
  hid_t dspace_id = NONNEG( H5Dget_space(dset_info.dset_id) );

  hsize_t offset[4] = {current, 0, 0, 0};
  hsize_t count[4] = {1, hsize_t(dim1), hsize_t(dim2), hsize_t(dim3)};
  NONNEG( H5Sselect_hyperslab(dspace_id,
                              H5S_SELECT_SET,
                              offset,
                              NULL,
                              count,
                              NULL) );
  hid_t memory_dspace_id = NONNEG( H5Screate_simple(4, count, NULL) );
  NONNEG( H5Dwrite(dset_info.dset_id,
                         H5T_NATIVE_SHORT,
                         memory_dspace_id,
                         dspace_id,
                         H5P_DEFAULT,
                         data) );
  NONNEG( H5Sclose(dspace_id) );    
  NONNEG( H5Sclose(memory_dspace_id) );
}

int foo() {
  return 3;
}

