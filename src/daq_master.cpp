#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <numeric>
#include <unistd.h>
#include "hdf5.h"

#include "ana_daq_util.h"
#include "daq_base.h"

const std::string usage("daq_writer - takes the following arguments:\n "
"  verbose  integer verbosity level, 0,1, etc\n"
"  rundir   string, the output directory\n"
"  group    string, this processes group (should be daq_master)\n"                        
"  id       int,    this processes id within that group\n"

"  writer_group    string, the daq_writer group name (should be daq_writer)\n"                        
"  num_writers     the number of daq writers\n"
"  num_shots       int, how many shots will the DAQ write in this run\n"

"  small_count     number of small datasets between all writers\n"
"  vlen_count      number of vlen datasets between all writers\n"
"  detector_count  number of detector datasets between all writers\n"

"  small_chunksize     int, number of elements in a small\n"
"  vlen_chunksize      int, number of elements in a vlen\n"
"  detector_chunksize  int, number of elements in a detector chunk\n"

"  detector_rows\n"
"  detector_columns\n"

"  flush_interval  how many fiducials between flushes\n"

"  masters_hang   have masters hang when done, for debugging process control\n"

"  small_name_first_0 small_name_first_1 ... small_name_first_k  ints, starting name for smalls, one for each writer\n"                        
"  small_shot_first_0 small_shot_first_1 ... small_shot_first_k  ints, starting shot for smalls, one for each writer\n"                        
"  small_shot_stride_0 small_shot_stride_1 ... small_shot_stridek  ints, stride for smalls, one for each writer\n"                        

"  vlen_name_first_0 vlen_name_first_1 ... vlen_name_first_k  ints, firsting name for vlens, one for each writer\n"                        
"  vlen_shot_first_0 vlen_shot_first_1 ... vlen_shot_first_k  ints, starting shot for vlens, one for each writer\n"                        
"  vlen_shot_stride_0 vlen_shot_stride_1 ... vlen_shot_stridek  ints, stride for vlens, one for each writer\n"                        

"  detector_name_first_0 detector_name_first_1 ... detector_name_first_k  ints, starting name for detectors, one for each writer\n"                        
"  detector_shot_first_0 detector_shot_first_1 ... detector_shot_first_k  ints, starting shot for detectors, one for each writer\n"                        
"  detector_shot_stride_0 detector_shot_stride_1 ... detector_shot_stridek  ints, stride for detectors, one for each writer\n"                        
                        );

struct DaqMasterConfig : DaqBaseConfig {
  std::string writer_group;
  int num_writers;

  std::vector<int> small_name_first, vlen_name_first, detector_name_first;
  std::vector<int> small_count, vlen_count, detector_count;
  std::vector<int> small_shot_first, vlen_shot_first, detector_shot_first;
  std::vector<int> small_shot_stride, vlen_shot_stride, detector_shot_stride;

  // will be derived from other arguments
  int small_count_all;
  int vlen_count_all;
  int detector_count_all;

  // each writer has a longer stride, but between all writers the
  // stride is shorter
  int detector_stride_all;
  
  void dump(FILE *fout);
  bool parse_command_line(int argc, char *argv[], const std::string &usage);
};

void DaqMasterConfig::dump(FILE *fout) {
  fprintf(fout, "DaqMasterConfig\n");
  fprintf(fout, "    verbose=%d\n", verbose);
  fprintf(fout, "    rundir=%s\n", rundir.c_str());
  fprintf(fout, "    group=%s\n", group.c_str());
  fprintf(fout, "    id=%d\n", id);

  fprintf(fout, "    num_shots=%ld\n", num_shots);

  fprintf(fout, "    small_chunksize=%d\n", small_chunksize);
  fprintf(fout, "    vlen_chunksize=%d\n", vlen_chunksize);
  fprintf(fout, "    detector_chunksize=%d\n", detector_chunksize);

  fprintf(fout, "    detector_rows=%d\n", detector_rows);
  fprintf(fout, "    detector_columns=%d\n", detector_columns);

  fprintf(fout, "    flush_interval=%d\n", flush_interval);
  fprintf(fout, "    hang=%d\n", hang);

  fprintf(fout, "    writer_group=%s\n", writer_group.c_str());
  fprintf(fout, "    num_writers=%d\n", num_writers);

  linedump_vector(fout, "small_name_first", small_name_first);
  linedump_vector(fout, "vlen_name_first", vlen_name_first);
  linedump_vector(fout, "detector_name_first", detector_name_first);
  linedump_vector(fout, "small_count", small_count);
  linedump_vector(fout, "vlen_count", vlen_count);
  linedump_vector(fout, "detector_count", detector_count);
  linedump_vector(fout, "small_shot_first", small_shot_first);
  linedump_vector(fout, "vlen_shot_first", vlen_shot_first);
  linedump_vector(fout, "detector_shot_first", detector_shot_first);
  linedump_vector(fout, "small_shot_stride", small_shot_stride);
  linedump_vector(fout, "vlen_shot_stride", vlen_shot_stride);
  linedump_vector(fout, "detector_shot_stride", detector_shot_stride);
  
  fprintf(fout, "    small_count_all=%d\n", small_count_all);
  fprintf(fout, "    vlen_count_all=%d\n", vlen_count_all);
  fprintf(fout, "    detector_count_all=%d\n", detector_count_all);
  fflush(fout);
}

bool DaqMasterConfig::parse_command_line(int argc, char *argv[], const std::string &usage) {
  if (argc < 15) {
    std::cerr << "ERROR: need at least 15 command line arguments, " << std::endl;
    std::cerr << usage << std::endl;
    return false;
  }
  int idx = 1;
  verbose = atoi(argv[idx++]);
  rundir = std::string(argv[idx++]);
  group = std::string(argv[idx++]);
  id = atoi(argv[idx++]);

  num_shots = atol(argv[idx++]);

  small_chunksize = atoi(argv[idx++]);
  vlen_chunksize = atoi(argv[idx++]);
  detector_chunksize = atoi(argv[idx++]);

  detector_rows = atoi(argv[idx++]);
  detector_columns = atoi(argv[idx++]);
  flush_interval = atoi(argv[idx++]);
  hang = atoi(argv[idx++]);

  writer_group = std::string(argv[idx++]);
  num_writers = atoi(argv[idx++]);

  argc -= 15;
  int args_left = num_writers * 12;

  if (argc != args_left) {
    std::cerr << "ERROR: read first 15 arguments, which specified " <<
      num_writers << " num_writers. Need " << args_left <<
      " more arguments, but there are " << argc << std::endl;
    return false;
  }

  idx = read_args(small_name_first, num_writers, argv, idx);
  idx = read_args(vlen_name_first, num_writers, argv, idx);
  idx = read_args(detector_name_first, num_writers, argv, idx);
  idx = read_args(small_count, num_writers, argv, idx);
  idx = read_args(vlen_count, num_writers, argv, idx);
  idx = read_args(detector_count, num_writers, argv, idx);
  idx = read_args(small_shot_first, num_writers, argv, idx);
  idx = read_args(vlen_shot_first, num_writers, argv, idx);
  idx = read_args(detector_shot_first, num_writers, argv, idx);
  idx = read_args(small_shot_stride, num_writers, argv, idx);
  idx = read_args(vlen_shot_stride, num_writers, argv, idx);
  idx = read_args(detector_shot_stride, num_writers, argv, idx);
  
  small_count_all = std::accumulate(small_count.begin(), small_count.end(), 0);
  vlen_count_all = std::accumulate(vlen_count.begin(), vlen_count.end(), 0);
  
  // check for one detector, but multiple small and vlen
  std::set<int> unique_small(small_name_first.begin(), small_name_first.end());
  if (unique_small.size() != small_name_first.size()) {
    throw std::runtime_error("FATAL: daq_master assumes more small datasets than daq_writers");
  }

  std::set<int> unique_vlen(vlen_name_first.begin(), vlen_name_first.end());
  if (unique_vlen.size() != vlen_name_first.size()) {
    throw std::runtime_error("FATAL: daq_master assumes more small datasets than daq_writers");
  }

  std::set<int> unique_det(detector_name_first.begin(), detector_name_first.end());
  if (unique_det.size() != 1) {
    throw std::runtime_error("FATAL: daq_master assumes only one dectetor spread among writers");
  }
  detector_count_all = 1;

  std::set<int> unique_det_stride(detector_shot_stride.begin(), detector_shot_stride.end());
  if (unique_det_stride.size() != 1) {
    throw std::runtime_error("FATAL: daq_master assumes only value for detector strides");
  }
  detector_stride_all = detector_shot_stride.at(0) / num_writers;
  if (detector_shot_stride.at(0) % num_writers != 0) {
    throw std::runtime_error("FATAL: daq_master assumes writer det stride is exact multiple of num_writers");
  }
  return true;
  
}

class DaqMaster : public DaqBase {
  DaqMasterConfig m_config;
  std::vector<std::string> m_writer_basenames, m_writer_fnames_h5;
  std::vector<hid_t> m_writer_h5;
  std::string m_master_fname;
  hid_t m_master_fid;
  
public:
  DaqMaster(const DaqMasterConfig &);
  ~DaqMaster();

  void run();
  void wait_for_SWMR_access_to_all_writers();
  void create_non_swmr_master_file();
  void create_master_file();
  void create_all_master_groups_datasets_and_attributes();
  void start_SWMR_access_to_master_file();
  void translation_loop();
  void close_files();
  void create_one_vds_detector_fiducials_assume_writer_layout();
  void dump(FILE *);
};

DaqMaster::DaqMaster(const DaqMasterConfig &config)
  : DaqBase(config), m_config(config) {
  for (int writer = 0; writer < m_config.num_writers; ++writer) {
    std::string basename = DaqBase::form_basename(m_config.writer_group, writer);
    m_writer_basenames.push_back(basename);
    m_writer_fnames_h5.push_back(m_config.rundir + "/hdf5/" + basename + ".h5");
    m_writer_h5.push_back(-1);
  }
}

void DaqMaster::dump(FILE *fout) {
  DaqBase::dump(fout);
  fprintf(fout, "======= DaqMaster.dump =======\n");
}

void DaqMaster::run() {
  DaqBase::run_setup();
  m_config.dump(::stdout);
  dump(::stdout);
  wait_for_SWMR_access_to_all_writers();
  //  create_master_file();
  create_non_swmr_master_file();
  create_all_master_groups_datasets_and_attributes();
  start_SWMR_access_to_master_file();  
  translation_loop();
  close_files();
}

void DaqMaster::translation_loop() {
}

void DaqMaster::close_files() {
  for (int writer = 0; writer < m_config.num_writers; ++writer) {
    hid_t h5 = m_writer_h5.at(writer);
    CHECK_NONNEG( H5Fclose( h5 ), "h5fclose - daqmaster  - writer");
  }
  CHECK_NONNEG( H5Fclose(m_master_fid), "h5fclose - daqmaster, the master file");
}

void DaqMaster::wait_for_SWMR_access_to_all_writers() {
  int writer_currently_waiting_for = 0;
  while (writer_currently_waiting_for < m_config.num_writers) {
    auto fname = m_writer_fnames_h5.at(writer_currently_waiting_for).c_str();
    FILE *fp = fopen(fname, "r");
    if (NULL != fp) {
      if (m_config.verbose) fprintf(stdout, "daq_master: found %s\n" , fname);
      fclose(fp);
      hid_t h5 = H5Fopen(fname, H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, H5P_DEFAULT);
      CHECK_POS(h5, "H5Fopen master on writer file");
      m_writer_h5.at(writer_currently_waiting_for) = h5;
      ++writer_currently_waiting_for;
    } else {
      if (m_config.verbose) fprintf(stdout, "daq_master: still waiting for %s\n" , m_writer_fnames_h5.at(writer_currently_waiting_for).c_str());
      sleep(1);
    }
  }
  
}

void DaqMaster::create_non_swmr_master_file() {
  m_master_fid = H5Fcreate(m_fname_h5.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
  CHECK_NONNEG(m_master_fid, "creating file");
  if (m_config.verbose) {
    fprintf(stdout, "created file: %s\n", m_fname_h5.c_str());
    fflush(::stdout);
  }
}

void DaqMaster::create_master_file() {
  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  CHECK_NONNEG( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST), "set_libver_bounds" );
  m_master_fid = H5Fcreate(m_fname_h5.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
  CHECK_NONNEG(m_master_fid, "creating file");
  if (m_config.verbose) {
    fprintf(stdout, "created file: %s\n", m_fname_h5.c_str());
    fflush(::stdout);
  }
  CHECK_NONNEG( H5Pclose(fapl), "close file properties - writer");
}

void DaqMaster::create_all_master_groups_datasets_and_attributes() {
  DaqBase::create_standard_groups(m_master_fid);

  DaqBase::create_number_groups(m_small_group, m_small_id_to_number_group, 0, m_config.small_count_all);
  DaqBase::create_number_groups(m_vlen_group, m_vlen_id_to_number_group, 0, m_config.vlen_count_all);
  DaqBase::create_number_groups(m_detector_group, m_detector_id_to_number_group, 0, m_config.detector_count_all);

  create_one_vds_detector_fiducials_assume_writer_layout();
}

void DaqMaster::create_one_vds_detector_fiducials_assume_writer_layout() {
  // create the dataset properties for the VDS
  hid_t dcpl =  H5Pcreate (H5P_DATASET_CREATE);
  CHECK_NONNEG(dcpl, "master vds dset prop list for one detector");

  long fill_value = -1;
  CHECK_NONNEG( H5Pset_fill_value (dcpl, H5T_NATIVE_LONG, &fill_value), "vds one fill value");

  // select all of each writers dataset, and map it to a stride in the master vds
  std::vector<int> writer_offsets, writer_counts;

  int detector_shots = m_config.num_shots / m_config.detector_stride_all;
  
  hsize_t current_dims[1] = {(hsize_t)detector_shots};
  const int RANK1 = 1;
  hid_t vds_space = H5Screate_simple(RANK1, current_dims, NULL);
  CHECK_NONNEG(vds_space, "master vds space for one detector");

  if (m_config.verbose>0) {
    fprintf(stdout, "daq_master: will divide %d detector shots between %d writers into vds\n",
           detector_shots,  m_config.num_writers);
  }

  std::vector<hid_t> src_spaces;
  divide_evenly(detector_shots, m_config.num_writers, writer_offsets, writer_counts);
  const char * fiducials_dataset = "/detector/00000/fiducials";
  for (int writer = 0; writer < m_config.num_writers; ++writer) {
    hsize_t writer_current_dims[1] = {(hsize_t)writer_counts.at(writer)};
    hid_t src_space = H5Screate_simple(RANK1, writer_current_dims, NULL);
    src_spaces.push_back(src_space);
    hsize_t start[1] = {(hsize_t)writer};
    hsize_t stride[1] = {(hsize_t)m_config.num_writers};
    hsize_t count[1] = {(hsize_t)writer_counts.at(writer)};
    hsize_t *block = NULL;
    if (m_config.verbose>0) printf("daq_master: mapping %s%s with %Ld elements to start=%Ld stride=%Ld count=%Ld in vds\n",
                                   m_writer_fnames_h5.at(writer).c_str(), fiducials_dataset,
                                   writer_current_dims[0], start[0], stride[0], count[0]);
    
    CHECK_NONNEG( H5Sselect_hyperslab( vds_space, H5S_SELECT_SET, start, stride, count, block ),
                  "master - one vds - select hyperslab in master vds to correspond to a writer");
    CHECK_NONNEG( H5Pset_virtual( dcpl,
                                  vds_space,
                                  m_writer_fnames_h5.at(writer).c_str(),
                                  fiducials_dataset,
                                  src_space ),
                  "master - one vds set - H5Pset_virtual call to map a writer to master vds");
    
  }

  //
  hid_t detGroup = m_detector_id_to_number_group.at(0);
  hid_t dset = H5Dcreate2(detGroup, "fiducials", H5T_NATIVE_LONG, vds_space, H5P_DEFAULT,
                          dcpl, H5P_DEFAULT);
  CHECK_NONNEG( H5Sclose( vds_space ), "create one vds - closing vds space");
  CHECK_NONNEG( dset, "create vds dset");  

  CHECK_NONNEG( H5Pclose( dcpl ), "Create one vds - closing vds property list");

  for (size_t idx = 0; idx < src_spaces.size(); ++idx) {
    CHECK_NONNEG( H5Sclose( src_spaces.at(idx) ), "master - one vds set, closing space from writer");
  }

  CHECK_NONNEG( H5Dclose( dset ), "create one vds - closing master vds");
}


void DaqMaster::start_SWMR_access_to_master_file() {
}


DaqMaster::~DaqMaster() {
}


int main(int argc, char *argv[]) {
  DaqMasterConfig config;
  if (not config.parse_command_line(argc, argv, usage)) {
    return -1;
  }
  H5open();
  try {
    DaqMaster daqMaster(config);
    daqMaster.run();
  } catch (...) {
    H5close();
    throw;
  }
  H5close();
  
  return 0;
}
