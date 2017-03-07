#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <numeric>
#include <unistd.h>

#include "lc2daq.h"


class DaqMaster : public DaqBase {
  int m_num_writers;
  int m_small_num_per_writer;
  int m_vlen_num_per_writer;
  int m_cspad_num;
  int m_small_count_all;
  int m_vlen_count_all;
  int m_cspad_stride_all;
  hid_t m_master_fid;
  
  std::vector<int> m_small_name_first, m_vlen_name_first;
  std::vector<int> m_small_count, m_vlen_count;

  std::vector<std::string> m_writer_basenames, m_writer_fnames_h5;
  std::vector<hid_t> m_writer_h5;
  
  std::string m_master_fname_h5;

  std::map<int, hid_t> m_small_id_to_number_group;
  std::map<int, hid_t> m_vlen_id_to_number_group;
  std::map<int, hid_t> m_cspad_id_to_number_group;

public:
  DaqMaster(int argc, char *argv[]);
  ~DaqMaster();

  void run();
  void wait_for_SWMR_access_to_all_writers();
  void create_master_file();
  void create_all_master_groups_datasets_and_attributes();
  void start_SWMR_access_to_master_file();
  void translation_loop();
  void close_files_and_objects();
  void create_cspad_00000_VDSes_assume_writer_layout();
  void create_link_VDSes_assume_writer_layout(const std::map<int, hid_t> &groups, bool vlen_dsets = false ); 

  /** assumes src_dest_path exists in all daq_writers.
   *  write now, sends all of that stream into vds, starting at offset == daq_writer, with
   * given stride. */
  void round_robin_VDS(const char *vds_dset_name,
                       const char *src_dset_path,
                       hid_t dset_type,
                       hsize_t writer_stride,
                       hid_t parent_group,
                       std::tuple<int,int,int> dset_xtra_dims);
};


DaqMaster::DaqMaster(int argc, char *argv[])
  : DaqBase(argc, argv, "daq_master"), 
    m_num_writers(m_config["daq_writer"]["num"].as<int>()),
    m_small_num_per_writer(m_config["daq_writer"]["datasets"]["single_source"]["small"]["num_per_writer"].as<int>()),
    m_vlen_num_per_writer(m_config["daq_writer"]["datasets"]["single_source"]["vlen"]["num_per_writer"].as<int>()),
    m_cspad_num(m_config["daq_writer"]["datasets"]["round_robin"]["cspad"]["num"].as<int>()),
    m_small_count_all(0),
    m_vlen_count_all(0),
    m_cspad_stride_all(m_config["daq_writer"]["datasets"]["round_robin"]["cspad"]["shots_per_sample_all_writers"].as<int>()),
    m_master_fid(-1)
{
  if (m_cspad_num != 1) {
    throw std::runtime_error("only 1 cspad is supported");
  }

  for (int writer = 0; writer < m_num_writers; ++writer) {
    int small_first = writer * m_small_num_per_writer; 
    int vlen_first = writer * m_vlen_num_per_writer; 
    m_small_name_first.push_back(small_first);
    m_vlen_name_first.push_back(vlen_first);
    m_small_count.push_back(m_small_num_per_writer);
    m_vlen_count.push_back(m_vlen_num_per_writer);
  }

  m_small_count_all = m_num_writers * m_small_num_per_writer;
  m_vlen_count_all = m_num_writers * m_vlen_num_per_writer;


  for (int writer = 0; writer < m_num_writers; ++writer) {
    std::string basename = DaqBase::form_basename(std::string("daq_writer"), writer);
    m_writer_basenames.push_back(basename);
    m_writer_fnames_h5.push_back(form_fullpath(std::string("daq_writer"), writer, HDF5));
    m_writer_h5.push_back(-1);
  }

  m_master_fname_h5 = DaqBase::form_fullpath("daq_master", m_id, HDF5);
}


void DaqMaster::run() {
  DaqBase::run_setup();
  wait_for_SWMR_access_to_all_writers();
  create_master_file();
  create_all_master_groups_datasets_and_attributes();
  start_SWMR_access_to_master_file();  
  translation_loop();
  close_files_and_objects();
}


void DaqMaster::translation_loop() {
}


void DaqMaster::close_files_and_objects() {
  
  for (int writer = 0; writer < m_num_writers; ++writer) {
    hid_t h5 = m_writer_h5.at(writer);
    if (m_config["verbose"].as<int>()>0) {
      std::cout<< "H5OpenObjects report for writer " << writer << std::endl;
      std::cout << H5OpenObjects(h5).dumpStr(true) << std::endl;
    }
    NONNEG( H5Fclose( h5 ) );
  }
  
  DaqBase::close_number_groups(m_small_id_to_number_group);
  DaqBase::close_number_groups(m_vlen_id_to_number_group);
  DaqBase::close_number_groups(m_cspad_id_to_number_group);
  DaqBase::close_standard_groups();
  
  if (m_config["verbose"].as<int>()>0) {
    std::cout<< "H5OpenObjects report for master" << std::endl;
    std::cout << H5OpenObjects(m_master_fid).dumpStr(true) << std::endl;
  }
  NONNEG( H5Fclose(m_master_fid) );
}


void DaqMaster::wait_for_SWMR_access_to_all_writers() {
  const int microseconds_to_wait = 100000;
  int writer_currently_waiting_for = 0;
  while (writer_currently_waiting_for < m_num_writers) {
    auto fname = m_writer_fnames_h5.at(writer_currently_waiting_for).c_str();
    FILE *fp = fopen(fname, "r");
    if (NULL != fp) {
      if (m_config["verbose"].as<int>()>0) fprintf(stdout, "daq_master: found %s\n" , fname);
      fclose(fp);
      hid_t h5 = NONNEG( H5Fopen(fname, H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, H5P_DEFAULT) );
      m_writer_h5.at(writer_currently_waiting_for) = h5;
      ++writer_currently_waiting_for;
    } else {
      if (m_config["verbose"].as<int>()>0) {
        fprintf(stdout, "daq_master_%d: still waiting for %s\n" , m_id, 
                m_writer_fnames_h5.at(writer_currently_waiting_for).c_str());
      }
      usleep(microseconds_to_wait);
    }
  }
  
}


void DaqMaster::create_master_file() {
  hid_t fapl = NONNEG( H5Pcreate(H5P_FILE_ACCESS) );
  NONNEG( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
  m_master_fid = NONNEG( H5Fcreate(m_master_fname_h5.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
  if (m_config["verbose"].as<int>()>0) {
    fprintf(stdout, "created file: %s\n", m_master_fname_h5.c_str());
    fflush(::stdout);
  }
  NONNEG( H5Pclose(fapl) );
}


void DaqMaster::create_all_master_groups_datasets_and_attributes() {
  DaqBase::create_standard_groups(m_master_fid);

  DaqBase::create_number_groups(m_small_group, m_small_id_to_number_group, 0, m_small_count_all);
  DaqBase::create_number_groups(m_vlen_group, m_vlen_id_to_number_group, 0, m_vlen_count_all);
  DaqBase::create_number_groups(m_cspad_group, m_cspad_id_to_number_group, 0, m_cspad_num);
  
  create_cspad_00000_VDSes_assume_writer_layout();
  create_link_VDSes_assume_writer_layout(m_small_id_to_number_group);
  create_link_VDSes_assume_writer_layout(m_vlen_id_to_number_group, true);
}


void DaqMaster::create_cspad_00000_VDSes_assume_writer_layout() {
  const int NUM = 3;
  const char *dset_vds_names[NUM] = {"fiducials",
                                     "nano",
                                     "data" };
  const char *dset_paths[NUM] = {"/cspad/00000/fiducials",
                                 "/cspad/00000/nano",
                                 "/cspad/00000/data" };
  const hid_t dset_types[NUM] = {H5T_NATIVE_LONG,
                                 H5T_NATIVE_LONG,
                                 H5T_NATIVE_SHORT};
  const hid_t parent_group[NUM] = {m_cspad_id_to_number_group.at(0),
                                   m_cspad_id_to_number_group.at(0),
                                   m_cspad_id_to_number_group.at(0)};
  std::vector< std::tuple<int,int, int> > dset_xtra_dims;
  dset_xtra_dims.push_back( std::make_tuple(0,0,0) );
  dset_xtra_dims.push_back( std::make_tuple(0,0,0) );
  dset_xtra_dims.push_back( std::make_tuple(CSPadDim1, CSPadDim2, CSPadDim3) );

  for (int dset=0; dset<NUM; ++dset) {
    round_robin_VDS(dset_vds_names[dset],
                    dset_paths[dset],
                    dset_types[dset],
                    m_num_writers * m_cspad_stride_all,
                    parent_group[dset],
                    dset_xtra_dims.at(dset));
  }
}


void DaqMaster::create_link_VDSes_assume_writer_layout(const std::map<int, hid_t> &groups, bool vlen_dsets) {
}


void DaqMaster::round_robin_VDS(const char *vds_dset_name,
                                const char *src_dset_path,
                                hid_t dset_type,
                                hsize_t writer_stride,
                                hid_t parent_in_master,
                                std::tuple<int, int, int> dset_xtra_dims) {

  if ( not ((dset_type == H5T_NATIVE_LONG) or (dset_type == H5T_NATIVE_SHORT)) ) {
    throw std::runtime_error("round_robin_VDS, did not get native-long or native-short for dset type");
  }
  
  short fill_value_short = -1;
  long fill_value_long = -1;

  hid_t dcpl = NONNEG( H5Pcreate (H5P_DATASET_CREATE) );

  if (dset_type == H5T_NATIVE_LONG) {
    NONNEG( H5Pset_fill_value (dcpl, H5T_NATIVE_LONG, &fill_value_long) );
  } else {
    NONNEG( H5Pset_fill_value (dcpl, H5T_NATIVE_SHORT, &fill_value_short) );
  }    

  hsize_t current_dims[4] = {(hsize_t)0,
                             (hsize_t)std::get<0>(dset_xtra_dims),
                             (hsize_t)std::get<1>(dset_xtra_dims),
                             (hsize_t)std::get<2>(dset_xtra_dims) };

  hsize_t max_dims[4] = {(hsize_t)H5S_UNLIMITED,
                         (hsize_t)std::get<0>(dset_xtra_dims),
                         (hsize_t)std::get<1>(dset_xtra_dims),
                         (hsize_t)std::get<2>(dset_xtra_dims) };
  int rank = -1;
  if ((std::get<0>(dset_xtra_dims) == 0) and (std::get<1>(dset_xtra_dims) == 0) and  (std::get<2>(dset_xtra_dims) == 0)) {
    rank = 1;
  } else if ((std::get<0>(dset_xtra_dims) > 0) and (std::get<1>(dset_xtra_dims) > 0) and  (std::get<2>(dset_xtra_dims) > 0)) {
    rank = 4;
  } else {
    throw std::runtime_error("dset_xtra_dims are wrong, should be 0,0,0 or both positive");
  }
  
  hid_t vds_space = NONNEG( H5Screate_simple(rank, current_dims, max_dims) );

  for (int writer = 0; writer < m_num_writers; ++writer) {
    hsize_t writer_current_dims[4] = {(hsize_t)0,
                                      (hsize_t)std::get<0>(dset_xtra_dims),
                                      (hsize_t)std::get<1>(dset_xtra_dims),
                                      (hsize_t)std::get<2>(dset_xtra_dims)};
    hsize_t writer_max_dims[4] = {(hsize_t)H5S_UNLIMITED,
                                  (hsize_t)std::get<0>(dset_xtra_dims),
                                  (hsize_t)std::get<1>(dset_xtra_dims),
                                  (hsize_t)std::get<2>(dset_xtra_dims)};
    hid_t src_space = NONNEG( H5Screate_simple(rank, writer_current_dims, writer_max_dims) );
    hsize_t start[4] = {(hsize_t)writer, 0, 0, 0};
    hsize_t stride[4] = {(hsize_t)writer_stride, 1, 1, 1};
    hsize_t count[4] = {(hsize_t)H5S_UNLIMITED,
                        (hsize_t)std::get<0>(dset_xtra_dims),
                        (hsize_t)std::get<1>(dset_xtra_dims),
                        (hsize_t)std::get<2>(dset_xtra_dims)};
    hsize_t block[4] =  {(hsize_t)1,
                        (hsize_t)std::get<0>(dset_xtra_dims),
                        (hsize_t)std::get<1>(dset_xtra_dims),
                        (hsize_t)std::get<2>(dset_xtra_dims)};
    
    NONNEG( H5Sselect_hyperslab( vds_space, H5S_SELECT_SET, start, stride, count, block ) );
    NONNEG( H5Pset_virtual( dcpl, vds_space, m_writer_fnames_h5.at(writer).c_str(),
                            src_dset_path, src_space ) );
    NONNEG( H5Sclose( src_space ) );
  }

  hid_t dset = NONNEG( H5Dcreate2(parent_in_master, vds_dset_name,
                                  dset_type, vds_space, H5P_DEFAULT,
                                  dcpl, H5P_DEFAULT) );
  NONNEG( H5Sclose( vds_space ) );
  NONNEG( H5Pclose( dcpl ) );
  NONNEG( H5Dclose( dset ) );
}


void DaqMaster::start_SWMR_access_to_master_file() {
  NONNEG( H5Fstart_swmr_write(m_master_fid) );
  if (m_config["verbose"].as<int>()>0) {
    printf("started SWMR access to master file\n");
    fflush(::stdout);
  }
}


DaqMaster::~DaqMaster() {
}


int main(int argc, char *argv[]) {
  H5open();
  try {
    DaqMaster daqMaster(argc, argv);
    daqMaster.run();
  } catch (...) {
    H5close();
    throw;
  }
  H5close();
  
  return 0;
}
