#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <numeric>
#include <unistd.h>

#include "lc2daq.h"

const std::string usage("daq_master - takes the following arguments:\n "
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
  void create_master_file();
  void create_all_master_groups_datasets_and_attributes();
  void start_SWMR_access_to_master_file();
  void translation_loop();
  void close_files_and_objects();
  void create_detector_00000_VDSes_assume_writer_layout();
  void create_link_VDSes_assume_writer_layout(const std::map<int, hid_t> &groups, bool vlen_dsets = false ); 
  void round_robin_VDS(const char *vds_dset_name,
                       const char *src_dset_path,
                       hid_t dset_type,
                       const std::pair<int,int> &dset_xtra_dims);
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
  create_master_file();
  create_all_master_groups_datasets_and_attributes();
  start_SWMR_access_to_master_file();  
  translation_loop();
  close_files_and_objects();
}


void DaqMaster::translation_loop() {
}


void DaqMaster::close_files_and_objects() {
  
  for (int writer = 0; writer < m_config.num_writers; ++writer) {
    hid_t h5 = m_writer_h5.at(writer);
    if (m_config.verbose) {
      std::cout<< "H5OpenObjects report for writer " << writer << std::endl;
      std::cout << H5OpenObjects(h5).dumpStr(true) << std::endl;
    }
    NONNEG( H5Fclose( h5 ) );
  }

  
  DaqBase::close_number_groups(m_small_id_to_number_group);
  DaqBase::close_number_groups(m_vlen_id_to_number_group);
  DaqBase::close_number_groups(m_detector_id_to_number_group);
  DaqBase::close_standard_groups();
  
  if (m_config.verbose) {
    std::cout<< "H5OpenObjects report for master" << std::endl;
    std::cout << H5OpenObjects(m_master_fid).dumpStr(true) << std::endl;
  }
  NONNEG( H5Fclose(m_master_fid) );
}


void DaqMaster::wait_for_SWMR_access_to_all_writers() {
  const int microseconds_to_wait = 100000;
  int writer_currently_waiting_for = 0;
  while (writer_currently_waiting_for < m_config.num_writers) {
    auto fname = m_writer_fnames_h5.at(writer_currently_waiting_for).c_str();
    FILE *fp = fopen(fname, "r");
    if (NULL != fp) {
      if (m_config.verbose) fprintf(stdout, "daq_master: found %s\n" , fname);
      fclose(fp);
      hid_t h5 = POS( H5Fopen(fname, H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, H5P_DEFAULT) );
      m_writer_h5.at(writer_currently_waiting_for) = h5;
      ++writer_currently_waiting_for;
    } else {
      if (m_config.verbose) fprintf(stdout, "%s_%d: still waiting for %s\n" , m_config.group.c_str(), m_config.id, m_writer_fnames_h5.at(writer_currently_waiting_for).c_str());
      usleep(microseconds_to_wait);
    }
  }
  
}


void DaqMaster::create_master_file() {
  hid_t fapl = NONNEG( H5Pcreate(H5P_FILE_ACCESS) );
  NONNEG( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
  m_master_fid = NONNEG( H5Fcreate(m_fname_h5.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
  if (m_config.verbose) {
    fprintf(stdout, "created file: %s\n", m_fname_h5.c_str());
    fflush(::stdout);
  }
  NONNEG( H5Pclose(fapl) );
}


void DaqMaster::create_all_master_groups_datasets_and_attributes() {
  DaqBase::create_standard_groups(m_master_fid);

  DaqBase::create_number_groups(m_small_group, m_small_id_to_number_group, 0, m_config.small_count_all);
  DaqBase::create_number_groups(m_vlen_group, m_vlen_id_to_number_group, 0, m_config.vlen_count_all);
  DaqBase::create_number_groups(m_detector_group, m_detector_id_to_number_group, 0, m_config.detector_count_all);
  
  create_detector_00000_VDSes_assume_writer_layout();
  create_link_VDSes_assume_writer_layout(m_small_id_to_number_group);
  create_link_VDSes_assume_writer_layout(m_vlen_id_to_number_group, true);
}


void DaqMaster::create_detector_00000_VDSes_assume_writer_layout() {
  const int NUM = 3;
  const char *dset_vds_names[NUM] = {"fiducials",
                                     "nano",
                                     "data" };
  const char *dset_paths[NUM] = {"/detector/00000/fiducials",
                                 "/detector/00000/nano",
                                 "/detector/00000/data" };
  const hid_t dset_types[NUM] = {H5T_NATIVE_LONG,
                                 H5T_NATIVE_LONG,
                                 H5T_NATIVE_SHORT};
  std::vector< std::pair<int,int> > dset_xtra_dims;
  dset_xtra_dims.push_back( std::pair<int,int>(0,0) );
  dset_xtra_dims.push_back( std::pair<int,int>(0,0) );
  dset_xtra_dims.push_back( std::pair<int,int>(m_config.detector_rows,
                                               m_config.detector_columns) );

  for (int dset=0; dset<NUM; ++dset) {
    round_robin_VDS(dset_vds_names[dset],
                    dset_paths[dset],
                    dset_types[dset],
                    dset_xtra_dims.at(dset));
  }
}


void DaqMaster::create_link_VDSes_assume_writer_layout(const std::map<int, hid_t> &groups, bool vlen_dsets) {
}


void DaqMaster::round_robin_VDS(const char *vds_dset_name,
                                const char *src_dset_path,
                                hid_t dset_type,
                                const std::pair<int, int> &dset_xtra_dims) {
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

  // select all of each writers dataset, and map it to a stride in the master vds
  std::vector<int> writer_offsets, writer_counts;
  int detector_shots = m_config.num_shots / m_config.detector_stride_all;
  
  hsize_t current_dims[3] = {(hsize_t)detector_shots,
                             (hsize_t)dset_xtra_dims.first,
                             (hsize_t)dset_xtra_dims.second };
  int rank = -1;
  if ((dset_xtra_dims.first == 0) and (dset_xtra_dims.second == 0)) {
    rank = 1;
  } else if ((dset_xtra_dims.first > 0) and (dset_xtra_dims.second > 0)) {
    rank = 3;
  } else {
    throw std::runtime_error("dset_xtra_dims are wrong, should be 0,0 or both positive");
  }
  
  hid_t vds_space = NONNEG( H5Screate_simple(rank, current_dims, NULL) );

  if (m_config.verbose>0) {
    fprintf(stdout, "%s_%d: will divide %d detector shots between %d writers into vds\n",
            m_config.group.c_str(), m_config.id, detector_shots,  m_config.num_writers);
  }

  divide_evenly(detector_shots, m_config.num_writers, writer_offsets, writer_counts);
  for (int writer = 0; writer < m_config.num_writers; ++writer) {
    hsize_t writer_current_dims[3] = {(hsize_t)writer_counts.at(writer),
                                      (hsize_t)dset_xtra_dims.first,
                                      (hsize_t)dset_xtra_dims.second};
    hid_t src_space = NONNEG( H5Screate_simple(rank, writer_current_dims, NULL) );
    hsize_t start[3] = {(hsize_t)writer, 0, 0};
    hsize_t stride[3] = {(hsize_t)m_config.num_writers, 1, 1};
    hsize_t count[3] = {(hsize_t)writer_counts.at(writer),
                        (hsize_t)dset_xtra_dims.first,
                        (hsize_t)dset_xtra_dims.second};
    hsize_t *block = NULL;
    if (m_config.verbose>0) printf("%s_%d: mapping %s%s with %Ld elements to start=%Ld stride=%Ld count=%Ld in vds\n",
                                   m_config.group.c_str(), m_config.id, m_writer_fnames_h5.at(writer).c_str(), src_dset_path,
                                   writer_current_dims[0], start[0], stride[0], count[0]);
    
    NONNEG( H5Sselect_hyperslab( vds_space, H5S_SELECT_SET, start, stride, count, block ) );
    NONNEG( H5Pset_virtual( dcpl, vds_space, m_writer_fnames_h5.at(writer).c_str(),
                            src_dset_path, src_space ) );
    NONNEG( H5Sclose( src_space ) );
  }

  hid_t detGroup = m_detector_id_to_number_group.at(0);
  hid_t dset = NONNEG( H5Dcreate2(detGroup, vds_dset_name,
                                  dset_type, vds_space, H5P_DEFAULT,
                                  dcpl, H5P_DEFAULT) );
  NONNEG( H5Sclose( vds_space ) );
  NONNEG( H5Pclose( dcpl ) );
  NONNEG( H5Dclose( dset ) );
}


void DaqMaster::start_SWMR_access_to_master_file() {
  NONNEG( H5Fstart_swmr_write(m_master_fid) );
  if (m_config.verbose) {
    printf("started SWMR access to master file\n");
    fflush(::stdout);
  }
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
