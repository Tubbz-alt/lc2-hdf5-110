#include <iostream>
#include <unistd.h>
#include <chrono>

#include "hdf5_hl.h"

#include "DsetPropAccess.h"
#include "Dset.h"
#include "check_macros.h"


void print_h5_hyperslab(std::ostream &ostr, const char *header, hid_t space) {
  int ndims = H5Sget_simple_extent_ndims(space);

  ostr << header << ": ";

  std::vector<hsize_t> start(ndims), count(ndims), stride(ndims), block(ndims);
  H5Sget_regular_hyperslab(space, &start.at(0), &stride.at(0), &count.at(0), &block.at(0));
  ostr << " start=" << start << " count=" << count << " stride=" << stride << " block=" << block;
  ostr << std::endl;
  ostr.flush();
}

void print_h5_simple(std::ostream &ostr, const char *header, hid_t space) {
  ostr << header << ": ";
  int ndims = H5Sget_simple_extent_ndims(space);

  std::vector<hsize_t> dims(ndims);
  H5Sget_simple_extent_dims(space, &dims.at(0), NULL );
  ostr << " dims=" << dims;
  ostr << std::endl;
  ostr.flush();
}

Dset Dset::create(hid_t parent, const char *name, hid_t h5type, const std::vector<hsize_t> & chunk) {
  std::vector<hsize_t> start_dims(chunk);
  std::vector<hsize_t> max_dims(chunk);
  start_dims.at(0)=0;
  max_dims.at(0) = H5S_UNLIMITED;

  DsetPropAccess dsetCreate( std::string(name), h5type,  chunk);
  Dset dset;
  dset.m_type = h5type;
  hid_t space_id = NONNEG( H5Screate_simple(int(chunk.size()), &start_dims.at(0), &max_dims.at(0)) );
  dset.m_id = NONNEG( H5Dcreate2(parent, name, h5type, space_id, H5P_DEFAULT,
                                     dsetCreate.proplist, dsetCreate.access) );
  dset.m_dims = start_dims;

  dsetCreate.close();
  NONNEG( H5Sclose(space_id) );

  return dset;
}


std::vector<hsize_t> Dset::get_chunk(const std::string & fname, const std::string &dset) {
  hid_t parent = POS( H5Fopen(fname.c_str(), H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, H5P_DEFAULT) );
  std::vector<hsize_t> chunk = get_chunk(parent, dset);
  NONNEG( H5Fclose( parent ) );
  return chunk;
}

std::vector<hsize_t> Dset::get_chunk(hid_t parent, const std::string &dset) {
  hid_t dset_id = NONNEG(H5Dopen2(parent, dset.c_str(), H5P_DEFAULT));
  std::vector<hsize_t> chunk = get_chunk(parent, dset_id);
  NONNEG( H5Dclose( dset_id ) );
  return chunk;
}

std::vector<hsize_t> Dset::get_chunk(hid_t parent, hid_t dset) {
  std::vector<hsize_t> chunk;
  hid_t dspace_id = NONNEG(H5Dget_space(dset));
  int rank = NONNEG(H5Sget_simple_extent_ndims(dspace_id));
  chunk.resize(rank);

  hid_t proplist = NONNEG(H5Dget_create_plist(dset));
  H5D_layout_t layout = NONNEG( H5Pget_layout( proplist ) );

  if (layout == H5D_CHUNKED) {
    NONNEG(H5Pget_chunk(proplist, rank, &chunk.at(0)));
  } else if (layout == H5D_VIRTUAL) {
    size_t num_map;
    NONNEG( H5Pget_virtual_count( proplist, &num_map) );

    for (size_t ii = 0; ii < num_map; ++ii) {

      ssize_t filename_len = NONNEG( H5Pget_virtual_filename( proplist, ii, NULL, 0) );
      std::string filename; filename.resize(filename_len+1);
      NONNEG( H5Pget_virtual_filename( proplist, ii, &filename.at(0), filename_len+1) );

      ssize_t dset_len = NONNEG( H5Pget_virtual_dsetname( proplist, ii, NULL, 0) );
      std::string dsetname; dsetname.resize(dset_len+1);
      NONNEG( H5Pget_virtual_dsetname( proplist, ii, &dsetname.at(0), dset_len+1) );
      
      std::vector<hsize_t> chunk_ii = get_chunk(filename, dsetname);

      if (ii == 0) {
        chunk = chunk_ii;
      } else {
        if ( chunk != chunk_ii) {
          throw std::runtime_error("chunks across mappings in VDS not equal");
        }
      }
    }
    chunk.at(0) *= num_map;
  } else {
    throw std::runtime_error("neither VDS or chunked dataset");
  }

  NONNEG(H5Pclose(proplist));
  NONNEG(H5Sclose(dspace_id));

  return chunk;
}

Dset Dset::open(hid_t parent, const char *name) {
  // open and close the dataset in order to read the
  // dimensions, type and chunk size. We want a per
  // dataset cache of 3 types the chunk cache.
  // If it is a virtual dataset, we want to do sum
  // the chunk size among all the source files.

  std::vector<hsize_t> dims, chunk;
  hid_t h5type = -1;
  {
    hid_t dset = NONNEG(H5Dopen2(parent, name, H5P_DEFAULT));
    hid_t dspace_id = NONNEG(H5Dget_space(dset));
    int rank = NONNEG(H5Sget_simple_extent_ndims(dspace_id));
    dims.resize(rank);
    NONNEG(H5Sget_simple_extent_dims(dspace_id, &dims.at(0), NULL));

    hid_t type = NONNEG(H5Dget_type(dset));
    if (H5Tequal(type, H5T_NATIVE_INT64) >= 1) {
      h5type = H5T_NATIVE_INT64;
    } else if (H5Tequal(type, H5T_NATIVE_INT16) >= 1) {
      h5type = H5T_NATIVE_INT16;
    } else {
      throw std::runtime_error("this is not a int64 or int16 dataset");
    }

    chunk = get_chunk(parent, dset);

    NONNEG(H5Sclose(dspace_id));
    NONNEG(H5Dclose(dset));
    NONNEG(H5Tclose(type));
  }

  hsize_t chunk_cache_bytes = NONNEG(H5Tget_size(h5type));
  for (auto iter = chunk.begin(); iter != chunk.end(); ++iter) {
    chunk_cache_bytes *= *iter;
  }
  chunk_cache_bytes *= 2;

  // now open with a access based on dimensions
  hid_t access_id = NONNEG(H5Pcreate(H5P_DATASET_ACCESS));

  size_t rdcc_nslots = 101;
  size_t rdcc_nbytes = chunk_cache_bytes;
  double rdcc_w0 = 0.75;
  NONNEG(H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0));

  hid_t dset_id = NONNEG(H5Dopen2(parent, name, access_id));
  NONNEG(H5Pclose(access_id));

  Dset dset;
  dset.m_id = dset_id;
  dset.m_type = h5type;
  dset.m_dims = dims;

  return dset;
}

std::ostream & Dset::dbgInfo(std::ostream &o) {
  char name[512];
  NONNEG( H5Iget_name(id(), name, 512) );
  hid_t space = NONNEG(H5Dget_space(id()));
  int ndims = H5Sget_simple_extent_ndims(space);
  std::vector<hsize_t> dims(ndims);
  H5Sget_simple_extent_dims(space, &dims.at(0), NULL );
  NONNEG(H5Sclose(space));
  o << "dset=" << name << " &m_dims=" << &m_dims.at(0) << " m_dims=" << m_dims << " space_dims=" << dims;
  return o;
}


void Dset::check_read(hid_t type, hsize_t start, hsize_t count) {
  if ( H5Tequal(m_type, type) <= 0) {
    dbgInfo(std::cout) << "error: check_read, type=" << type << " start=" << start << " count=" << count << std::endl;
    throw std::runtime_error("dset::read, type mismatch");
  }
  if ((start < 0) or (count < 0)) {
    dbgInfo(std::cout) << "error: check_read, type=" << type << " start=" << start << " count=" << count << std::endl;
    throw std::runtime_error("dset::read - start or count is negative");
  }
  if (start+count > m_dims.at(0)) {
    dbgInfo(std::cout) << "error: check_read, type=" << type << " start=" << start << " count=" << count << std::endl;
    throw std::runtime_error("dset::read - start+count to big for dset");
  }
}

void Dset::check_append(hid_t type, hsize_t start, hsize_t count, size_t data_len) {
  if ( H5Tequal(m_type, type) <= 0) {
    dbgInfo(std::cout) << "error: check_append, type=" << type << " start=" << start << " count=" << count << " data_len=" << data_len << std::endl;
    throw std::runtime_error("dset::append, type mismatch");
  }

  if (size_t(start) + size_t(count) > data_len) {
    dbgInfo(std::cout) << "error: check_append, type=" << type << " start=" << start << " count=" << count << " data_len=" << data_len << std::endl;
    throw std::runtime_error("start+count is > data_len - append");
  }
  data_len -= size_t(start);

  hsize_t min_elem_needed_in_data = count;
  if (m_dims.size() > 1) {
    auto iter = m_dims.begin();
    ++iter;
    while (iter != m_dims.end()) {
      min_elem_needed_in_data *= *iter++;
    }
  }
  
  if (min_elem_needed_in_data > data_len) {
    throw std::runtime_error("Dset::append - data doesn't have enough elements");
  }
}


void Dset::generic_append(hsize_t count, const void *data) {
  hsize_t start = m_dims.at(0);
  m_dims.at(0) += count;
  NONNEG( H5Dset_extent(m_id, &m_dims[0]));

  hid_t filespace = NONNEG( H5Dget_space(m_id) );
  file_space_select(filespace, start, count);

  std::vector<hsize_t > mem_dims(m_dims);
  mem_dims.at(0)=count;

  hid_t memspace = NONNEG( H5Screate_simple(int(mem_dims.size()), &mem_dims.at(0), &mem_dims.at(0)));
  NONNEG( H5Sselect_all(memspace));
  // print_h5_hyperslab(std::cout, "filespace", filespace);
  // print_h5_simple(std::cout, "memspace", memspace);
  NONNEG( H5Dwrite(m_id, m_type, memspace, filespace, H5P_DEFAULT, data));

  NONNEG(H5Sclose(filespace));
  NONNEG(H5Sclose(memspace));
}


void Dset::append(hsize_t start, hsize_t count, const std::vector<int16_t> &data) {
  check_append(H5T_NATIVE_INT16, start, count, data.size());
  generic_append(count, &data.at(start));
}


void Dset::append(hsize_t start, hsize_t count, const std::vector<int64_t> &data) {
  check_append(H5T_NATIVE_INT64, start, count, data.size());
  generic_append(count, &data.at(start));
}

void Dset::generic_read(hsize_t start, hsize_t count, void *data) {
  hid_t filespace = NONNEG( H5Dget_space(m_id) );
  file_space_select(filespace, start, count);

  std::vector<hsize_t > mem_dims(m_dims);
  mem_dims.at(0)=count;

  hid_t memspace = NONNEG( H5Screate_simple(int(mem_dims.size()), &mem_dims.at(0), &mem_dims.at(0)));
  NONNEG( H5Sselect_all(memspace));

  // print_h5_hyperslab(std::cout, "filespace", filespace);
  // print_h5_simple(std::cout, "memspace", memspace);

  NONNEG( H5Dread(m_id, m_type, memspace, filespace, H5P_DEFAULT, data));

  NONNEG(H5Sclose(filespace));
  NONNEG(H5Sclose(memspace));

}

void Dset::read(hsize_t start, hsize_t count, std::vector<int64_t> &data) {
  check_read(H5T_NATIVE_INT64, start, count);
  size_t data_len = count;
  for (unsigned idx = 1; idx < m_dims.size(); ++idx)  data_len *= m_dims.at(idx);
  data.resize(data_len);
  generic_read(start, count, &data.at(0));
}


void Dset::read(hsize_t start, hsize_t count, std::vector<int16_t> &data) {
  check_read(H5T_NATIVE_INT16, start, count);
  size_t data_len = count;
  for (unsigned idx = 1; idx < m_dims.size(); ++idx)  data_len *= m_dims.at(idx);
  data.resize(data_len);
  generic_read(start, count, &data.at(0));
}


void Dset::file_space_select(hid_t file_space, hsize_t start, hsize_t count) {
  std::vector<hsize_t> start_sel(m_dims.size(), 0), count_sel(m_dims.size(), 1);
  std::vector<hsize_t> stride(m_dims.size(), 1);
  std::vector<hsize_t> block(m_dims);
  block.at(0) = 1;
  start_sel.at(0) = start;
  count_sel.at(0) = count;
  NONNEG(H5Sselect_hyperslab(file_space,
                             H5S_SELECT_SET,
                             &start_sel.at(0),  // start,0,0,0
                             &stride.at(0),     // 1,1,1,1
                             &count_sel.at(0),  // count,1,1,1
                             &block.at(0)));    // 1,dims,dims,dims
}


bool Dset::wait(hsize_t len_to_grow_to, int microseconds_to_pause, int timeout_seconds, bool verbose) {
  auto t0 = std::chrono::system_clock::now();
  
  while (true) {
    if (m_dims[0] >= len_to_grow_to) {
      if (verbose) {
        dbgInfo(std::cout) << "wait returning true, len_to_grow_to=" << len_to_grow_to << std::endl;
      }
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

    NONNEG( H5Drefresh(m_id) );
    std::vector<hsize_t> old = m_dims;
    NONNEG( H5LDget_dset_dims(m_id, &m_dims.at(0)) );
    if (verbose) {
      dbgInfo(std::cout) << "called H5Drefresh/H5LDget_dset_dims - old=" << old << " new=" << m_dims << std::endl;
    }

  }

  return false;
  
}

void Dset::close() {
  if (m_id >= 0) {
    NONNEG( H5Dclose( m_id) );
    m_id = -1;
  }
}
