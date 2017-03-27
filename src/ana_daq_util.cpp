#include <cstdio>
#include <stdexcept>
#include <numeric>
#include <unistd.h>
#include "hdf5.h"
#include "hdf5_hl.h"

#include "check_macros.h"
#include "ana_daq_util.h"
#include "DsetCreation.h"


namespace {
  
  Dset create_dataset(hid_t parent, 
                      const char *name, 
                      hid_t h5_type, 
                      std::vector<hsize_t> one_chunk_dim) {
    hsize_t chunk = one_chunk_dim.at(0);
    std::vector<hsize_t> start_dims(one_chunk_dim);
    start_dims.at(0)=0;
    DsetCreation dsetCreation( std::string(name), h5_type,  start_dims, chunk);

    std::vector<hsize_t> max_dims(one_chunk_dim);
    max_dims.at(0) = H5S_UNLIMITED;

    hid_t space_id = NONNEG( H5Screate_simple(rank, &start_dims.at(0), &max_dims.at(0)) );
    hid_t plist_id = dsetCreate.proplist;
    hid_t access_id = dsetCreate.access;
    hid_t h5_dset = NONNEG( H5Dcreate2(parent, name, h5_type, space_id, H5P_DEFAULT, plist_id, access_id) );
    
    NONNEG( H5Sclose(space_id) );
    NONNEG( H5Pclose(plist_id) );
    NONNEG( H5Pclose(access_id) );
    
    return Dset(h5_dset, start_dims);
  }

  void append_many_to_dset(Dset &dset, size_t num_elem, 
                           hid_t memtype, const void *buffer) {
    printf("append_many_to_dset - start dset=%ld\n", dset.dset_id());
    fflush(stdout);
    hid_t dxpl_id = H5P_DEFAULT;
    unsigned index = 0;

    NONNEG( H5DOappend( dset.dset_id(), dxpl_id, index, num_elem, memtype, buffer ) );
    printf("append_many_to_dset - appended %ld\n", num_elem);
    fflush(stdout);
  }

  size_t calc_bbox_bytes(const std::vector<hsize_t> bbox_start, 
                         const std::vector<hsize_t> bbox_end, size_t bytes_per_elem) {
    size_t total = bytes_per_elem;
    for (size_t dim = 0; dim < bbox_start.size(); ++dim) {
      size_t dim_len = bbox_end.at(dim) - bbox_start[dim] + 1;
      total *= dim_len;
    }
    return total;
  }

  void check_bounds(const std::vector<hsize_t> &index, const std::vector<hsize_t> &dims) {
    for (size_t rank=0; rank < dims.size(); ++rank) {
      if ((index.at(rank) < 0) or (index.at(rank) >= dims[rank])) {
        std::cerr << "check_bounds: rank=" << rank 
                  << " index=" << index.at(rank)
                  << " bound=" << dims[rank] << std::endl;
        throw std::runtime_error("check_bounds");
      } 
    }
  }
  
  void check_read( hid_t dset, hid_t h5type, hid_t mem_space, hid_t file_space, size_t buffer_len_bytes) {
    hid_t dset_space = NONNEG( H5Dget_space( dset ) );
    htri_t is_simple = NONNEG( H5Sis_simple( dset_space ) );
    if (0 == is_simple) {
      std::cerr << "check_read called on dset that is not simple, dset=" << dset << std::endl;
      NONNEG( H5Sclose( dset_space ) );
      return;
    }

    hid_t dset_type = NONNEG( H5Dget_type( dset ) );
    htri_t types_equal = NONNEG( H5Tequal( dset_type, h5type ) );
    NONNEG( H5Tclose( dset_type ) );
    if (0 == types_equal) {
      throw std::runtime_error("check_read - types not equal");
    }

    int dset_rank = NONNEG( H5Sget_simple_extent_ndims( dset_space ) );
    std::vector< hsize_t > dset_dims(dset_rank);
    NONNEG( H5Sget_simple_extent_dims( dset_space, &dset_dims.at(0), NULL ) );
    NONNEG( H5Sclose( dset_space ) );
    
    int mem_rank = NONNEG( H5Sget_simple_extent_ndims( mem_space ) );
    int file_rank = NONNEG( H5Sget_simple_extent_ndims( file_space ) );
    if (dset_rank != mem_rank) {
      throw std::runtime_error("check_read - dset_rank != mem_rank");
    }
    if (dset_rank != file_rank) {
      throw std::runtime_error("check_read - dset_rank != file_rank");
    }

    std::vector< hsize_t > mem_bbox_start(dset_rank), file_bbox_start(dset_rank);
    std::vector< hsize_t > mem_bbox_end(dset_rank), file_bbox_end(dset_rank);

    NONNEG( H5Sget_select_bounds( mem_space, &mem_bbox_start.at(0), &mem_bbox_end.at(0) ) );
    NONNEG( H5Sget_select_bounds( file_space, &file_bbox_start.at(0), &file_bbox_end.at(0) ) );

    for (size_t dim = 0; dim < mem_bbox_start.size(); ++dim) {
      if (0 != mem_bbox_start[dim]) {
        throw std::runtime_error("check_read - mem bbox does not start at 0");
      }
    }

    size_t type_size_bytes = H5Tget_size( h5type );
    size_t mem_bbox_bytes = calc_bbox_bytes( mem_bbox_start, mem_bbox_end, type_size_bytes );
    if (mem_bbox_bytes > buffer_len_bytes) {
      std::cerr << "mem_bbox_bytes=" << mem_bbox_bytes << " buffer_len=" << buffer_len_bytes << std::endl;
      throw std::runtime_error("check_read - mem bbox to large for buffer");
    }
    size_t file_bbox_bytes = calc_bbox_bytes( file_bbox_start, file_bbox_end, type_size_bytes );
    if (file_bbox_bytes != mem_bbox_bytes) {
      std::cerr << "mem_bbox_bytes=" << mem_bbox_bytes << " file_bbox_bytes=" << file_bbox_bytes << std::endl;
      throw std::runtime_error("file and mem bbox bytes not the same, OK if doing strided read from file");
    }

    check_bounds(file_bbox_start, dset_dims);
    check_bounds(file_bbox_end, dset_dims);
    
  }

} // local namespace


//----------------------------------------

Dset create_1d_int64_dataset(hid_t parent, const char *name, hsize_t chunk_size) {
  std::vector<hsize_t> chunk_dim(1, chunk_size);
  return create_dataset(parent, name, H5T_NATIVE_INT64, chunk_dim);
}


Dset create_4d_int16_dataset(hid_t parent, const char *name,
                             hsize_t dim1, hsize_t dim2, hsize_t dim3, 
                             hsize_t chunk_size) {
  std::vector<hsize_t> chunk_dim(4);
  chunk_dim.at(0)=chunk_size;
  chunk_dim.at(1)=dim1;
  chunk_dim.at(2)=dim2;
  chunk_dim.at(3)=dim3;
  return create_dataset(parent, name, H5T_NATIVE_INT16, 2, chunk_dim);
}


void  append_many_to_1d_int64_dset(Dset &dset, hsize_t count, int64_t *data) {
  append_many_to_dset(dset, count, H5T_NATIVE_INT64, data);
}


void append_to_1d_int64_dset(Dset &dset, int64_t value) {
  append_many_to_dset(dset, 1, H5T_NATIVE_INT64, &value);
}


void append_to_4d_int16_dset(Dset &dset, int16_t *data) {
  append_many_to_dset(dset, 1, H5T_NATIVE_INT16, data);
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
  
  return dsetInfo;
}


int64_t read_int64_from_1d(DsetReaderInfo &info, int64_t event_index) {
  int64_t result;
  info.file_space_select(event_index);

  check_read(info.dset_id(), H5T_NATIVE_INT64, 
             info.mem_space_id(), info.file_space_id(), sizeof(result));

  NONNEG( H5Dread( info.dset_id(), H5T_NATIVE_INT64, info.mem_space_id(),
                   info.file_space_id(), H5P_DEFAULT, &result) );
  printf("read\n");
  return result;
}

