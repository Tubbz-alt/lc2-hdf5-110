#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <numeric>
#include <unistd.h>

#include "lc2daq.h"
#include "DaqBase.h"

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
  // if there are any datasets that have to be updated in the master,
  // we put them here, for instance the fast/slow tables
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

  // round robin datasets
  for (int cur_cspad = 0; cur_cspad < m_cspad_num; ++cur_cspad) {
    std::vector<std::string> src_data, src_fid, src_nano;
    for (size_t idx = 0; idx < m_writer_fnames_h5.size(); ++idx) {
      char cur_cspad_str[128];
      sprintf(cur_cspad_str, "%5.5d", cur_cspad);
      src_data.push_back(std::string("/cspad/") + cur_cspad_str + "/data");
      src_fid.push_back(std::string("/cspad/") + cur_cspad_str + "/fiducials");
      src_nano.push_back(std::string("/cspad/") + cur_cspad_str + "/nano");
    }
    VDSRoundRobin roundRobinData(m_cspad_id_to_number_group.at(cur_cspad), "data", m_writer_fnames_h5, src_data);
    VDSRoundRobin roundRobinFid(m_cspad_id_to_number_group.at(cur_cspad), "fiducials", m_writer_fnames_h5, src_fid);
    VDSRoundRobin roundRobinNano(m_cspad_id_to_number_group.at(cur_cspad), "nano", m_writer_fnames_h5, src_nano);
  }

  // single source datasets
  for (int writer = 0; writer < m_num_writers; ++writer) {

    const char * src_writer_fname = m_writer_fnames_h5.at(writer).c_str();

    int small_num_per_writer = m_config["daq_writer"]["datasets"]["single_source"]["small"]["num_per_writer"].as<int>();
    int vlen_num_per_writer = m_config["daq_writer"]["datasets"]["single_source"]["vlen"]["num_per_writer"].as<int>();

    int small_first = writer * small_num_per_writer;
    int vlen_first = writer * vlen_num_per_writer;
    
    for (int small_dset = small_first; small_dset < small_first + small_num_per_writer; ++small_dset) {
      char data_path[256], fid_path[256], nano_path[256];
      sprintf(data_path, "/small/%5.5d/data", small_dset);
      sprintf(fid_path, "/small/%5.5d/fiducials", small_dset);
      sprintf(nano_path, "/small/%5.5d/nano", small_dset);

      H5Lcreate_external(src_writer_fname, data_path, m_master_fid, data_path, H5P_DEFAULT, H5P_DEFAULT);
      H5Lcreate_external(src_writer_fname, fid_path, m_master_fid, fid_path, H5P_DEFAULT, H5P_DEFAULT);
      H5Lcreate_external(src_writer_fname, nano_path, m_master_fid, nano_path, H5P_DEFAULT, H5P_DEFAULT);
    }
    
    for (int vlen_dset = vlen_first; vlen_dset < vlen_first + vlen_num_per_writer; ++vlen_dset) {
      char blob_path[256], blobcount_path[256], blobstart_path[256], fid_path[256], nano_path[256];
      sprintf(blob_path, "/vlen/%5.5d/blob", vlen_dset);
      sprintf(blobcount_path, "/vlen/%5.5d/blobcount", vlen_dset);
      sprintf(blobstart_path, "/vlen/%5.5d/blobstart", vlen_dset);
      sprintf(fid_path, "/vlen/%5.5d/fiducials", vlen_dset);
      sprintf(nano_path, "/vlen/%5.5d/nano", vlen_dset);

      NONNEG( H5Lcreate_external(src_writer_fname, blob_path, m_master_fid, blob_path, H5P_DEFAULT, H5P_DEFAULT) );
      NONNEG( H5Lcreate_external(src_writer_fname, blobcount_path, m_master_fid, blobcount_path, H5P_DEFAULT, H5P_DEFAULT) );
      NONNEG( H5Lcreate_external(src_writer_fname, blobstart_path, m_master_fid, blobstart_path, H5P_DEFAULT, H5P_DEFAULT) );
      NONNEG( H5Lcreate_external(src_writer_fname, fid_path, m_master_fid, fid_path, H5P_DEFAULT, H5P_DEFAULT) );
      NONNEG( H5Lcreate_external(src_writer_fname, nano_path, m_master_fid, nano_path, H5P_DEFAULT, H5P_DEFAULT) );
    }
  }
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
  } catch (const std::exception &ex) {
    std::cout << "Caught exception: " << ex.what() << std::endl;
    std::cout << "trying to close library " << std::endl;
    H5close();
    throw ex;
  }
  H5close();
  
  return 0;
}
