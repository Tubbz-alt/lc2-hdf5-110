#include <iostream>

#include "DsetPropAccess.h"
#include "Dset.h"
#include "check_macros.h"


void print_h5space(std::ostream &ostr, const char *header, hid_t space) {
  htri_t is_simple = H5Sis_simple(space);
  ostr << header << ": is_simple=" << is_simple;

  if (is_simple < 1) return;
  int ndims = H5Sget_simple_extent_ndims(space);

  htri_t is_reg_hyper = H5Sis_regular_hyperslab(space);
  ostr << " is_reg_hyper=" << is_reg_hyper;

  if (is_reg_hyper >= 1) {
    std::vector<hsize_t> start(ndims), count(ndims), stride(ndims), block(ndims);
    H5Sget_regular_hyperslab(space, &start.at(0), &stride.at(0), &count.at(0), &block.at(0));
    ostr << " start=" << start << " count=" << count << " stride=" << stride << " block=" << block;
  } else {
    std::vector<hsize_t> dims(ndims);
    H5Sget_simple_extent_dims(space, &dims.at(0), NULL );
    ostr << " dims=" << dims;
  }
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
  dset.m_dim = start_dims;

  dsetCreate.close();
  NONNEG( H5Sclose(space_id) );

  return dset;
}


Dset Dset::open(hid_t parent, const char *name) {
  // open and close the dataset in order to read the
  // dimensions, type and chunk size
  std::vector<hsize_t> dim, chunk;
  hid_t h5type = -1;
  {
    hid_t dset = NONNEG(H5Dopen2(parent, name, H5P_DEFAULT));
    hid_t dspace_id = NONNEG(H5Dget_space(dset));
    int rank = NONNEG(H5Sget_simple_extent_ndims(dspace_id));
    dim.resize(rank);
    chunk.resize(rank);
    NONNEG(H5Sget_simple_extent_dims(dspace_id, &dim.at(0), NULL));

    hid_t type = NONNEG(H5Dget_type(dset));
    if (H5Tequal(type, H5T_NATIVE_INT64) >= 1) {
      h5type = H5T_NATIVE_INT64;
    } else if (H5Tequal(type, H5T_NATIVE_INT16) >= 1) {
      h5type = H5T_NATIVE_INT16;
    } else {
      throw std::runtime_error("this is not a int64 or int16 dataset");
    }
    hid_t proplist = NONNEG(H5Dget_create_plist(dset));
    NONNEG(H5Pget_chunk(proplist, rank, &chunk.at(0)));

    NONNEG(H5Pclose(proplist));
    NONNEG(H5Sclose(dspace_id));
    NONNEG(H5Dclose(dset));
    NONNEG(H5Tclose(type));
  }

  hsize_t chunk_cache_bytes = NONNEG(H5Tget_size(h5type));
  for (auto iter = chunk.begin(); iter != chunk.end(); ++iter) {
    chunk_cache_bytes *= *iter;
  }
  chunk_cache_bytes *= 3;

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
  dset.m_dim = dim;

  return dset;
}


void Dset::check_read(hid_t type, hsize_t start, hsize_t count) {
  if ( H5Tequal(m_type, type) <= 0) {
    throw std::runtime_error("dset::read, type mismatch");
  }
  if ((start < 0) or (count < 0)) {
    throw std::runtime_error("dset::read - start or count is negative");
  }
  if (start+count > m_dim.at(0)) {
    throw std::runtime_error("dset::read - start+count to big for dset");
  }
}

  void Dset::check_append(hid_t type, hsize_t count, size_t data_len) {
  if ( H5Tequal(m_type, type) <= 0) {
    throw std::runtime_error("dset::append, type mismatch");
  }

  hsize_t min_elem_needed_in_data = count;
  if (m_dim.size() > 1) {
    auto iter = m_dim.begin();
    ++iter;
    while (iter != m_dim.end()) {
      min_elem_needed_in_data *= *iter++;
    }
  }
  if (min_elem_needed_in_data < data_len) {
    throw std::runtime_error("Dset::append - data doesn't have enough elements");
  }

}


void Dset::generic_append(hsize_t count, const void *data) {
  hsize_t start = m_dim.at(0);
  m_dim.at(0) += count;
  NONNEG( H5Dset_extent(m_id, &m_dim[0]));

  hid_t filespace = NONNEG( H5Dget_space(m_id) );
  std::cout << "start=" << start << "end=" << count << std::endl;
  file_space_select(filespace, start, count);

  std::vector<hsize_t > mem_dims(m_dim);
  mem_dims.at(0)=count;

  hid_t memspace = NONNEG( H5Screate_simple(int(mem_dims.size()), &mem_dims.at(0), &mem_dims.at(0)));
  NONNEG( H5Sselect_all(memspace));
 // print_h5space(std::cout, "filespace", filespace);
//  print_h5space(std::cout, "memspace", memspace);

  NONNEG( H5Dwrite(m_id, m_type, memspace, filespace, H5P_DEFAULT, data));

  NONNEG(H5Sclose(filespace));
  NONNEG(H5Sclose(memspace));
}


void Dset::append(hsize_t count, const std::vector<int16_t> &data) {
  check_append(H5T_NATIVE_INT16, count, data.size());
}


void Dset::append(hsize_t count, const std::vector<int64_t> &data) {
  check_append(H5T_NATIVE_INT64, count, data.size());
  generic_append(count, &data.at(0));
}

void Dset::generic_read(hsize_t start, hsize_t count, void *data) {
  hid_t filespace = NONNEG( H5Dget_space(m_id) );
  file_space_select(filespace, start, count);

  std::vector<hsize_t > mem_dims(m_dim);
  mem_dims.at(0)=count;

  hid_t memspace = NONNEG( H5Screate_simple(int(mem_dims.size()), &mem_dims.at(0), &mem_dims.at(0)));
  NONNEG( H5Sselect_all(memspace));
  // print_h5space(std::cout, "filespace", filespace);
  // print_h5space(std::cout, "memspace", memspace);

  NONNEG( H5Dread(m_id, m_type, memspace, filespace, H5P_DEFAULT, data));

  NONNEG(H5Sclose(filespace));
  NONNEG(H5Sclose(memspace));

}

void Dset::read(hsize_t start, hsize_t count, std::vector<int64_t> &data) {
  check_read(H5T_NATIVE_INT64, start, count);
  data.resize(count);
  generic_read(start, count, &data.at(0));
}


void Dset::read(hsize_t start, hsize_t count, std::vector<int16_t> &data) {
  check_read(H5T_NATIVE_INT16, start, count);
  data.resize(count);
  generic_read(start, count, &data.at(0));
}


void Dset::file_space_select(hid_t file_space, hsize_t start, hsize_t count) {
  std::vector<hsize_t> start_sel(m_dim.size(), 0), count_sel(m_dim.size(), 1);
  std::vector<hsize_t> stride(m_dim.size(), 1);
  std::vector<hsize_t> block(m_dim);
  block.at(0) = 1;
  start_sel.at(0) = start;
  count_sel.at(0) = count;
  NONNEG(H5Sselect_hyperslab(file_space,
                             H5S_SELECT_SET,
                             &start_sel.at(0),  // start,0,0,0
                             &stride.at(0),     // 1,1,1,1
                             &count_sel.at(0),  // count,1,1,1
                             &block.at(0)));    // 1,dim,dim,dim
}

/*
void Dset::increase_extent(hsize_t extend) {
  m_dim.at(0) += extend;
  NONNEG( H5Dset_extent(m_id, &m_dim.at(0)) );

}

void Dset::file_space_select(hsize_t start, hsize_t count) {
  NONNEG( H5Sset_extent_simple(m_file_space, m_dim.size(), ))
}
void Dset::mem_space_select(hsize_t count);

void Dset::append(const std::vector<int64_t> & data) {
  if (H5Tequal(m_type, H5T_NATIVE_INT64) <= 0) {
    throw std::runtime_error("Dset::append<int64> but dataset is not int64");
  }
  hsize_t start = m_dim.at(0);
  increase_extent(data.size());
  file_space_select(start, data.size());
  mem_space_select(data.size());




  NONNEG( H5Sselect_hyperslab(m_file_select_count, H5S_SELECT_SET,
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

}

 */


/*
// -------------------------------------------------------------------
// DsetWriterInfo
void DsetWriterInfo::close() {
  Dset::close();
  if (m_filespace > -1) {
    NONNEG( H5Sclose( m_filespace ) );
  }
  if (m_mem_space > -1) {
    NONNEG( H5Sclose( m_mem_space) );
  }
}

DsetWriterInfo &DsetWriterInfo::operator=( DsetWriterInfo &o) {
  Dset::operator=(o);
  m_filespace = o.m_filespace;
  m_mem_space = o.m_mem_space;
  return *this;
}

DsetWriterInfo::DsetWriterInfo(hid_t _dset_i; std::vector<hsize_t> _dim) :
  Dset(_dset_id, _dim)
{
  dim( _dim );
}

DsetWriterInfo::DsetWriterInfo(hid_t _dset_id, hsize_t _extent) :
  Dset(_dset_id, _extent) {
}

// -------------------------------------------------------------------
// DsetReaderInfo
void DsetReaderInfo::close() {
  if (m_mem_space_id > -1) {
    NONNEG( H5Sclose( m_mem_space_id) );
  }
  if (file_space_id() > -1) {
    NONNEG( H5Sclose( file_space_id() ) );
  }
  Dset::close();
}

DsetReaderInfo::DsetReaderInfo( const DsetReaderInfo &o) :
  Dset(o),
  m_mem_space_id(o.m_mem_space_id),
  m_file_space_id(o.m_file_space_id),
  m_file_select_start(o.m_file_select_start),
  m_file_select_count(o.m_file_select_count),
  m_file_select_stride(o.m_file_select_stride),
  m_file_select_block(o.m_file_select_block)
{
}

DsetReaderInfo &DsetReaderInfo::operator=( DsetReaderInfo &o) {
  Dset::operator=(o);
  m_mem_space_id = o.m_mem_space_id;
  m_file_space_id = o.m_file_space_id;

  m_file_select_start = o.m_file_select_start;
  m_file_select_count = o.m_file_select_count;
  m_file_select_stride = o.m_file_select_stride;
  m_file_select_block = o.m_file_select_block;
  
  return *this;
}

void DsetReaderInfo::dim(const std::vector<hsize_t> &new_dim) {
  if (file_space_id() < 0) {
    file_space_id(  NONNEG( H5Screate( H5S_SIMPLE ) ) );
  }

  if (mem_space_id() < 0) {
    mem_space_id(  NONNEG( H5Screate( H5S_SIMPLE ) ) );
  }

  Dset::dim(new_dim);

  m_file_select_start = std::vector<hsize_t>(new_dim.size(), 0);

  m_file_select_count = std::vector<hsize_t>(new_dim.size(), 1);
  
  m_file_select_stride = std::vector<hsize_t>(new_dim.size(), 1);

  m_file_select_block = new_dim;
  m_file_select_block.at(0)=1;
  
  NONNEG( H5Sset_extent_simple( file_space_id(), dim().size(), &dim().at(0), NULL) );

  NONNEG( H5Sset_extent_simple( mem_space_id(), dim().size(), &m_file_select_block.at(0), NULL) );
}

}
*/

