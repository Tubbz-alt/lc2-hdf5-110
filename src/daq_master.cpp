#include <string>
#include <vector>
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
  detector_count_all = std::accumulate(detector_count.begin(), detector_count.end(), 0);
  return true;
  
}

class DaqMaster : public DaqBase {
  DaqMasterConfig m_config;
  std::vector<std::string> m_writer_basenames, m_writer_fnames_h5;
  std::vector<hid_t> m_writer_h5;
  
public:
  DaqMaster(const DaqMasterConfig &);
  ~DaqMaster();

  void run();
  void wait_for_SWMR_access_to_all_writers();
  void create_master_file();
  void create_all_master_groups_datasets_and_attributes();
  void start_SWMR_access_to_master_file();
  void close_files();
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

  close_files();
}

void DaqMaster::close_files() {
  for (int writer = 0; writer < m_config.num_writers; ++writer) {
    hid_t h5 = m_writer_h5.at(writer);
    CHECK_NONNEG( H5Fclose( h5 ), "h5fclose - daqmaster  - writer");
  }
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

void DaqMaster::create_master_file() {
}

void DaqMaster::create_all_master_groups_datasets_and_attributes() {
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
