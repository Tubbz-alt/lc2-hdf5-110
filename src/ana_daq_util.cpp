#include <cstdio>
#include <stdexcept>
#include <numeric>
#include <unistd.h>
#include "hdf5.h"
#include "hdf5_hl.h"

#include "check_macros.h"
#include "ana_daq_util.h"


namespace {
  
  DsetWriterInfo create_dataset(hid_t parent, 
                                const char *dset, 
                                hid_t h5_type, 
                                size_t type_size_bytes, 
                                std::vector<hsize_t> one_chunk_dim) {
    int rank = one_chunk_dim.size();
    std::vector<hsize_t> current_dims(one_chunk_dim);
    std::vector<hsize_t> maximum_dims(one_chunk_dim);
    current_dims.at(0)=0;
    maximum_dims.at(0) = H5S_UNLIMITED;

    hid_t space_id = NONNEG( H5Screate_simple(rank, &current_dims.at(0), &maximum_dims.at(0)) );
    hid_t plist_id = NONNEG( H5Pcreate(H5P_DATASET_CREATE) );
    NONNEG(H5Pset_chunk(plist_id, rank, &one_chunk_dim.at(0)) );
    
    hid_t access_id = NONNEG( H5Pcreate(H5P_DATASET_ACCESS) );
    
    hsize_t num_elements_in_a_chunk = 1;
    for (auto iter = one_chunk_dim.begin(); iter != one_chunk_dim.end(); ++iter) {
      num_elements_in_a_chunk *= *iter;
    }
    hsize_t num_bytes_in_a_chunk = type_size_bytes * num_elements_in_a_chunk;

    size_t rdcc_nslots = 101;
    size_t rdcc_nbytes = num_bytes_in_a_chunk * 3;
    double rdcc_w0 = 0.75;
    NONNEG(H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0) );
    
    hid_t h5_dset = NONNEG( H5Dcreate2(parent, dset, h5_type, space_id, H5P_DEFAULT, plist_id, access_id) );
    
    NONNEG( H5Sclose(space_id) );
    NONNEG( H5Pclose(plist_id) );
    NONNEG( H5Pclose(access_id) );
    
    return DsetWriterInfo(h5_dset, current_dims);
  }

  void append_many_to_dset(DsetWriterInfo &dset_info, size_t num_elem, 
                           hid_t memtype, const void *buffer) {
    hid_t dxpl_id = H5P_DEFAULT;
    unsigned index = 0;
    NONNEG( H5DOappend( dset_info.dset_id(), dxpl_id, index, num_elem, memtype, buffer ) );
  }
}


DsetWriterInfo create_1d_int64_dataset(hid_t parent, const char *dset, hsize_t chunk_size) {
  std::vector<hsize_t> chunk_dim(1, chunk_size);
  return create_dataset(parent, dset, H5T_NATIVE_INT64, 8, chunk_dim);
}


DsetWriterInfo create_4d_int16_dataset(hid_t parent, const char *dset,
                                       hsize_t dim1, hsize_t dim2, hsize_t dim3, 
                                       hsize_t chunk_size) {
  std::vector<hsize_t> chunk_dim(4);
  chunk_dim.at(0)=chunk_size;
  chunk_dim.at(1)=dim1;
  chunk_dim.at(2)=dim2;
  chunk_dim.at(3)=dim3;
  return create_dataset(parent, dset, H5T_NATIVE_INT16, 2, chunk_dim);
}


void  append_many_to_1d_int64_dset(DsetWriterInfo &dset_info, hsize_t count, int64_t *data) {
  append_many_to_dset(dset_info, count, H5T_NATIVE_INT64, data);
}


void append_to_1d_int64_dset(DsetWriterInfo &dset_info, int64_t value) {
  append_many_to_dset(dset_info, 1, H5T_NATIVE_INT64, &value);
}


void append_to_4d_int16_dset(DsetWriterInfo &dset_info, int16_t *data) {
  append_many_to_dset(dset_info, 1, H5T_NATIVE_INT16, data);
}



bool wait_for_dataset_to_grow(hid_t dset_id, hsize_t * dims, hsize_t len_to_grow_to, 
                              int microseconds_to_pause, int timeout_seconds) {
  auto t0 = std::chrono::system_clock::now();
  
  while (true) {
    if (dims[0] >= len_to_grow_to) {
      return true;
    }

    if (microseconds_to_pause > 0) {
      usleep(microseconds_to_pause);
    }

    if (timeout_seconds > 0) {
      auto t1 = std::chrono::system_clock::now();
      auto diff = t1-t0;
      auto seconds = std::chrono::duration_cast<std::chrono::seconds>(diff);
      if (seconds.count() > timeout_seconds) {
        return false;
      }
    }

    NONNEG( H5Drefresh(dset_id) );
    NONNEG( H5LDget_dset_dims(dset_id, dims) );

  }

  return false;
  
}

DsetReaderInfo createReaderDsetInfo(hid_t parent, const char *dset_path, 
                                    int num_events_in_dataset_chunk_cache) {
  
  // open and close the dataset in order to read the dimensions.
  std::vector<hsize_t> dim;
  size_t type_size_bytes=0;
  {
    hid_t dset = NONNEG( H5Dopen2( parent, dset_path, H5P_DEFAULT) );
    hid_t dspace_id = NONNEG( H5Dget_space( dset ) );
    int rank = NONNEG( H5Sget_simple_extent_ndims( dspace_id ) );
    dim.resize(rank);
    NONNEG( H5Sget_simple_extent_dims( dspace_id, &dim.at(0), NULL ) );

    hid_t type = NONNEG( H5Dget_type( dset ) );
    type_size_bytes = NONNEG( H5Tget_size( type ) );

    NONNEG( H5Sclose( dspace_id ) );
    NONNEG( H5Dclose( dset ) );
    NONNEG( H5Tclose( type ) );
  }

  // now open with a access based on dimensions
  hid_t access_id = NONNEG( H5Pcreate(H5P_DATASET_ACCESS) );
    
  hsize_t chunk_cache_bytes = num_events_in_dataset_chunk_cache * type_size_bytes;

  if (dim.size() > 1) {
    for (auto iter = dim.begin()+1; iter != dim.end(); ++iter) {
      chunk_cache_bytes *= *iter;
    }
  }

  size_t rdcc_nslots = 101;
  size_t rdcc_nbytes = chunk_cache_bytes;
  double rdcc_w0 = 0.75;
  NONNEG( H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0) );
    
  hid_t dset = NONNEG( H5Dopen2(parent, dset_path, access_id) );
  NONNEG( H5Pclose( access_id ) );

  DsetReaderInfo dsetInfo;
  dsetInfo.dset_id( dset );
  dsetInfo.dim( dim );

  std::vector<hsize_t> one(dim);
  one.at(0)=1;
  hid_t mem_space_one_event = NONNEG( H5Screate( H5S_SIMPLE ) );
  NONNEG( H5Sset_extent_simple(mem_space_one_event, one.size(), &one.at(0), NULL) );
  dsetInfo.mem_space_one_event( mem_space_one_event );
  
  return dsetInfo;
}


int64_t read_int64_from_1d(DsetReaderInfo &info, int64_t event_index) {
  int64_t result;
  info.file_space_id(event_index);
  NONNEG( H5Dread( info.dset_id(), H5T_NATIVE_INT64, info.mem_space_one_event(),
                   info.file_space_id(), H5P_DEFAULT, &result) );
  return result;
}

