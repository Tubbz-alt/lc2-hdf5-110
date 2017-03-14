#include <algorithm>

#include "VDSRoundRobin.h"
#include "ana_daq_util.h"

VDSRoundRobin::VDSRoundRobin(hid_t vds_location,
                             const char * vds_dset_name,
                             std::vector<std::string> src_filenames,
                             std::vector<std::string> src_dset_paths) : 
  m_src_filenames(src_filenames),
  m_src_dset_paths(src_dset_paths),
  m_rank(-1),
  m_h5type(-1),
  m_vds_dcpl(-1),
  m_vds_dset(-1)
{
  check_srcs();
  m_h5type = get_h5type_and_cache_files_dsets();
  m_rank = get_rank_and_cache_spaces();
  m_one_block = get_one_block();
  m_vds_dcpl = NONNEG( H5Pcreate(H5P_DATASET_CREATE) );
  set_vds_fill_value();
  hid_t vds_space = create_vds_space();
  hsize_t vds_stride = src_filenames.size();
  hid_t src_space = select_all_of_any_src_as_one_block();
  
  for (size_t src = 0; src < vds_stride; ++src) {
    hsize_t vds_start = src;
    select_unlimited_count_of_vds(vds_space, vds_start, vds_stride);
    add_to_virtual_mapping(vds_space, src_space, src);
  }
  m_vds_dset = H5Dcreate2(vds_location, vds_dset_name, m_h5type,
                          vds_space, H5P_DEFAULT, m_vds_dcpl, H5P_DEFAULT);

  NONNEG( H5Sclose( vds_space ) );
  NONNEG( H5Sclose( src_space ) );
  cleanup();
}


VDSRoundRobin::~VDSRoundRobin() {
  if (m_vds_dset != -1) {
    NONNEG( H5Dclose( m_vds_dset ) );
  }
}

void VDSRoundRobin::check_srcs() const
{
  if ((m_src_filenames.size() == 0) or 
      (m_src_dset_paths.size() == 0) or 
      (m_src_filenames.size() != m_src_dset_paths.size()))
    {
      throw std::runtime_error("VDSRoundRobin - parameters");
    }

  for (size_t idx = 0; idx < m_src_filenames.size(); ++idx) {
    FILE *fp = fopen(m_src_filenames.at(idx).c_str(), "r");
    if (fp == NULL) {
      throw std::runtime_error("VDSRoundRobin - src file not found");
    }
    fclose(fp);
  }
}


hid_t VDSRoundRobin::get_h5type_and_cache_files_dsets() {
  if (m_src_fids.size() != 0 ) throw std::runtime_error("VDSRoundRobin - src_fids not empty");
  if (m_src_dsets.size() != 0 ) throw std::runtime_error("VDSRoundRobin - src_dsets not empty");
  
  hid_t h5type = -1;

  for (size_t idx = 0; idx < m_src_filenames.size(); ++idx) {
    const char *fname = m_src_filenames.at(idx).c_str();
    hid_t fid = NONNEG( H5Fopen(fname, H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, H5P_DEFAULT) );
    hid_t dset = NONNEG( H5Dopen2(fid, m_src_dset_paths.at(idx).c_str(), H5P_DEFAULT) );
    
    m_src_fids.push_back(fid);
    m_src_dsets.push_back(dset);

    hid_t src_h5type = H5Dget_type(dset);

    if (idx > 0) {
      htri_t type_compare_result = H5Tequal(h5type, src_h5type);
      if ( type_compare_result == 0) {
        throw std::runtime_error("VDSRoundRobin - src h5type mistmatch");
      } else if (type_compare_result < 0) {
        throw std::runtime_error("VDSRoundRobin - H5Tequal returns neither TRUE or FALSE");
      } else {
        // equal, clean up
        NONNEG( H5Tclose(src_h5type) );
      };
    } else {
      // first one, caller cleans up
      h5type = src_h5type;
    }
  }
  return h5type;
}

int VDSRoundRobin::get_rank_and_cache_spaces() {
  if (m_src_fids.size() != m_src_filenames.size() ) throw std::runtime_error("VDSRoundRobin - src_fids not set");
  if (m_src_dsets.size() != m_src_filenames.size() ) throw std::runtime_error("VDSRoundRobin - src_dsets not set");
  if (m_src_spaces.size() != 0 ) throw std::runtime_error("VDSRoundRobin - spaces not empty");
  
  int rank = -1;
  for (size_t idx = 0; idx < m_src_filenames.size(); ++idx) {
    hid_t dset = m_src_dsets.at(idx);
    hid_t space = NONNEG( H5Dget_space(dset) );
    m_src_spaces.push_back(space);

    int src_rank = NONNEG( H5Sget_simple_extent_ndims( space ) );

    if (idx > 0) {
      if ( src_rank != rank ) {
        throw std::runtime_error("VDSRoundRobin - src ranks not equal");
      }
    } else {
      rank = src_rank;
    }
  }
  return rank;
}

std::vector<hsize_t>  VDSRoundRobin::get_one_block() {
  if (m_src_fids.size() != m_src_filenames.size() ) throw std::runtime_error("VDSRoundRobin - src_fids not set");
  if (m_src_dsets.size() != m_src_filenames.size() ) throw std::runtime_error("VDSRoundRobin - src_dsets not set");
  if (m_src_spaces.size() != m_src_filenames.size() ) throw std::runtime_error("VDSRoundRobin - spaces not set");
  if (m_rank < 0) throw std::runtime_error("VDSRoundRobin - rank not set");

  std::vector<hsize_t> one_block(m_rank);
  if (m_rank==0) return one_block;
  one_block.at(0) = 1;
  if (m_rank==1) return one_block;

  for (size_t idx = 0; idx < m_src_filenames.size(); ++idx) {
    hid_t space = m_src_spaces.at(idx);
    std::vector<hsize_t> dims(m_rank);
    int num_dims = NONNEG( H5Sget_simple_extent_dims( space, &dims.at(0), NULL) );
    if (num_dims != m_rank) throw std::runtime_error("VDSRoundRobin - m_rank is wrong");

    if (idx > 0) {
      if ( not std::equal( dims.begin()+1, dims.end(), one_block.begin() + 1) ) {
        throw std::runtime_error("VDSRoundRobin - src dims not equal");
      }
    } else {
      std::copy( dims.begin()+1, dims.end(), one_block.begin()+1);
    }
  }
  return one_block;
}

void VDSRoundRobin::set_vds_fill_value() 
{
  if (m_h5type == -1) throw std::runtime_error("VDSRoundRobin:: h5type not set for fill");

  char fill_H5T_NATIVE_CHAR = 0;
  unsigned char fill_H5T_NATIVE_UCHAR = 0;
  signed char fill_H5T_NATIVE_SCHAR = -1;

  short fill_H5T_NATIVE_SHORT = 0;
  unsigned short fill_H5T_NATIVE_USHORT = 0;

  int fill_H5T_NATIVE_INT = -1;
  unsigned fill_H5T_NATIVE_UINT = 0;

  long fill_H5T_NATIVE_LONG = -1;
  unsigned long fill_H5T_NATIVE_ULONG = 0;

  long long fill_H5T_NATIVE_LLONG = -1;
  unsigned long long fill_H5T_NATIVE_ULLONG = 0;

  float fill_H5T_NATIVE_FLOAT = -1.0;
  double fill_H5T_NATIVE_DOUBLE = -1.0;
  long double fill_H5T_NATIVE_LDOUBLE = -1.0;

  hsize_t fill_H5T_NATIVE_HSIZE = 0;
  hssize_t fill_H5T_NATIVE_HSSIZE = 0;
  herr_t fill_H5T_NATIVE_HERR = -1;
  hbool_t fill_H5T_NATIVE_HBOOL = 0;
  
  void *fill = NULL;

  if (0 < H5Tequal(H5T_NATIVE_CHAR, m_h5type)) {
    fill = &fill_H5T_NATIVE_CHAR;
  } else if (0 < H5Tequal(H5T_NATIVE_UCHAR, m_h5type)) {
    fill = &fill_H5T_NATIVE_UCHAR;
  } else if (0 < H5Tequal(H5T_NATIVE_SCHAR, m_h5type)) {
    fill = &fill_H5T_NATIVE_SCHAR;
  } else if (0 < H5Tequal(H5T_NATIVE_SHORT, m_h5type)) {
    fill = &fill_H5T_NATIVE_SHORT;
  } else if (0 < H5Tequal(H5T_NATIVE_USHORT, m_h5type)) {
    fill = &fill_H5T_NATIVE_USHORT;
  } else if (0 < H5Tequal(H5T_NATIVE_INT, m_h5type)) {
    fill = &fill_H5T_NATIVE_INT;
  } else if (0 < H5Tequal(H5T_NATIVE_UINT, m_h5type)) {
    fill = &fill_H5T_NATIVE_UINT;
  } else if (0 < H5Tequal(H5T_NATIVE_LONG, m_h5type)) {
    fill = &fill_H5T_NATIVE_LONG;
  } else if (0 < H5Tequal(H5T_NATIVE_ULONG, m_h5type)) {
    fill = &fill_H5T_NATIVE_ULONG;
  } else if (0 < H5Tequal(H5T_NATIVE_LLONG, m_h5type)) {
    fill = &fill_H5T_NATIVE_LLONG;
  } else if (0 < H5Tequal(H5T_NATIVE_ULLONG, m_h5type)) {
    fill = &fill_H5T_NATIVE_ULLONG;
  } else if (0 < H5Tequal(H5T_NATIVE_FLOAT, m_h5type)) {
    fill = &fill_H5T_NATIVE_FLOAT;
  } else if (0 < H5Tequal(H5T_NATIVE_DOUBLE, m_h5type)) {
    fill = &fill_H5T_NATIVE_DOUBLE;
  } else if (0 < H5Tequal(H5T_NATIVE_LDOUBLE, m_h5type)) {
    fill = &fill_H5T_NATIVE_LDOUBLE;
  } else if (0 < H5Tequal(H5T_NATIVE_HSIZE, m_h5type)) {
    fill = &fill_H5T_NATIVE_HSIZE;
  } else if (0 < H5Tequal(H5T_NATIVE_HSSIZE, m_h5type)) {
    fill = &fill_H5T_NATIVE_HSSIZE;
  } else if (0 < H5Tequal(H5T_NATIVE_HERR, m_h5type)) {
    fill = &fill_H5T_NATIVE_HERR;
  } else if (0 < H5Tequal(H5T_NATIVE_HBOOL, m_h5type)) {
    fill = &fill_H5T_NATIVE_HBOOL;
  } else {
    throw std::runtime_error("VDSRoundRobin: unknown datatype, can't create fill");
  }

  NONNEG( H5Pset_fill_value (m_vds_dcpl, m_h5type, &fill) );
}

hid_t VDSRoundRobin::create_vds_space() 
{
  if (m_rank < 1) throw std::runtime_error("rank is < 1");
  std::vector<hsize_t> current_dims(m_one_block);
  std::vector<hsize_t> max_dims(m_one_block);
  
  current_dims.at(0) = 0;
  max_dims.at(0) = H5S_UNLIMITED;

  return NONNEG( H5Screate_simple( m_rank, &current_dims.at(0), &max_dims.at(0) ) );
}

hid_t VDSRoundRobin::select_all_of_any_src_as_one_block() {
  if (m_rank < 1) throw std::runtime_error("rank is < 1");
  if (m_one_block.size() != size_t(m_rank)) throw std::runtime_error("block rank != rank");
  
  std::vector<hsize_t> current_dims(m_one_block), max_dims(m_one_block);
  current_dims.at(0) = 0;
  max_dims.at(0) = H5S_UNLIMITED;
  hid_t src_space = NONNEG( H5Screate_simple(m_rank, &current_dims.at(0), &max_dims.at(0)) );

  std::vector<hsize_t> start0(m_rank);
  std::vector<hsize_t> stride1(m_rank, 1);
  std::vector<hsize_t> count1(m_rank, 1);
  std::vector<hsize_t> blockUnlimited(m_one_block);
  blockUnlimited.at(0) = H5S_UNLIMITED;

  NONNEG( H5Sselect_hyperslab(src_space, H5S_SELECT_SET, 
                              &start0.at(0), &stride1.at(0), &count1.at(0), 
                              &blockUnlimited.at(0)) );

  return src_space;
}

void VDSRoundRobin::select_unlimited_count_of_vds(hid_t space, hsize_t start, hsize_t stride) {
  if (m_rank < 1) throw std::runtime_error("rank is < 1");
  if (m_one_block.size() != size_t(m_rank)) throw std::runtime_error("block rank != rank");
  
  std::vector<hsize_t> start_all_dims(m_rank, 0);
  start_all_dims.at(0) = start;

  std::vector<hsize_t> stride_all_dims(m_rank, 1);
  stride_all_dims.at(0) = stride;

  std::vector<hsize_t> count_unlimited(m_rank, 1);
  count_unlimited.at(0) = H5S_UNLIMITED;

  std::vector<hsize_t> block_one(m_rank, 1);

  NONNEG( H5Sselect_hyperslab(space, H5S_SELECT_SET, 
                              &start_all_dims.at(0), &stride_all_dims.at(0), 
                              &count_unlimited.at(0), 
                              &block_one.at(0)) );
}

void VDSRoundRobin::add_to_virtual_mapping(hid_t vds_space, hid_t src_space, size_t which_src) {
  if (m_vds_dcpl < 0) throw std::runtime_error("vds_dcpl is < 0");
  const char * src_fname = m_src_filenames.at(which_src).c_str();
  const char *src_dset_path = m_src_dset_paths.at(which_src).c_str();  
  NONNEG( H5Pset_virtual(m_vds_dcpl, vds_space, src_fname, src_dset_path, src_space) );
}

void VDSRoundRobin::cleanup() {
  for (size_t idx = 0; idx < m_src_filenames.size(); ++idx) {
    NONNEG( H5Sclose( m_src_spaces.at(idx) ) );
    NONNEG( H5Dclose( m_src_dsets.at(idx) ) );
    NONNEG( H5Sclose( m_src_fids.at(idx) ) );
  }
  NONNEG( H5Pclose(m_vds_dcpl) );
  NONNEG( H5Tclose(m_h5type) );
}

hid_t VDSRoundRobin::get_and_transfer_ownership_of_VDS() 
{
  if (m_vds_dset == -1) throw std::runtime_error("vds already transferred or not set");
  hid_t to_return = m_vds_dset;
  m_vds_dset = -1;
  return to_return;
}

