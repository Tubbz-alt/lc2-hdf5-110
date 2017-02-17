#include <string>
#include <vector>
#include <set>
#include <iostream>
#include <numeric>
#include <unistd.h>

#include "lc2daq.h"

const std::string usage("ana_reader_master - takes the following arguments:\n "
"  verbose  integer verbosity level, 0,1, etc\n"
"  rundir   string, the output directory\n"
"  group    string, this processes group (should be ana_reader_master)\n"                        
"  id       int,    this processes id within that group\n"
"  master_fname string, basename of master filename, in rundir/hdf5/ \n"
"  num_shots       int, how many shots will the DAQ write in this run\n"

"  small_count     number of small datasets between all writers\n"
"  vlen_count      number of vlen datasets between all writers\n"
"  detector_count  number of detector datasets between all writers\n"

"  detector_rows\n"
"  detector_columns\n"
);


struct AnaReaderMasterConfig {
  int verbose;
  std::string rundir;
  std::string group;
  int id;

  std::string master_fname;
  
  long num_shots;

  int detector_rows;
  int detector_columns;

  int small_count_all;
  int vlen_count_all;
  int detector_count_all;

  void dump(FILE *fout);
  bool parse_command_line(int argc, char *argv[], const std::string &usage);
};


void AnaReaderMasterConfig::dump(FILE *fout) {
  fprintf(fout, "AnaReaderMasterConfig\n");
  fprintf(fout, "    verbose=%d\n", verbose);
  fprintf(fout, "    rundir=%s\n", rundir.c_str());
  fprintf(fout, "    group=%s\n", group.c_str());
  fprintf(fout, "    id=%d\n", id);
  fprintf(fout, "     master=%s\n", master_fname.c_str());
  fprintf(fout, "    num_shots=%ld\n", num_shots);

  fprintf(fout, "    small_count_all=%d\n", small_count_all);
  fprintf(fout, "    vlen_count_all=%d\n", vlen_count_all);
  fprintf(fout, "    detector_count_all=%d\n", detector_count_all);

  fprintf(fout, "    detector_rows=%d\n", detector_rows);
  fprintf(fout, "    detector_columns=%d\n", detector_columns);

  fflush(fout);
}


bool AnaReaderMasterConfig::parse_command_line(int argc, char *argv[], const std::string &usage) {
  if (argc != 12) {
    std::cerr << "ERROR: need 11 command line arguments, " << std::endl;
    std::cerr << usage << std::endl;
    return false;
  }
  int idx = 1;
  verbose = atoi(argv[idx++]);
  rundir = std::string(argv[idx++]);
  group = std::string(argv[idx++]);
  id = atoi(argv[idx++]);

  master_fname = std::string(argv[idx++]);
  num_shots = atol(argv[idx++]);

  small_count_all = atoi(argv[idx++]);
  vlen_count_all = atoi(argv[idx++]);
  detector_count_all = atoi(argv[idx++]);

  detector_rows = atoi(argv[idx++]);
  detector_columns = atoi(argv[idx++]);

  // check for one detector
  if (detector_count_all != 1) {
    throw std::runtime_error("FATAL: ana_reader_master assumes 1 detector");
  }

  return true;
}


class AnaReaderMaster {
  AnaReaderMasterConfig m_config;
  std::string m_master_fname;
  hid_t m_master_fid;
  
public:
  AnaReaderMaster(const AnaReaderMasterConfig &);
  ~AnaReaderMaster();

  void run();
  void wait_for_SWMR_access_to_master();
  void analysis_loop();
};


AnaReaderMaster::AnaReaderMaster(const AnaReaderMasterConfig &config)
  : m_config(config) {
  m_master_fname = m_config.rundir + "/hdf5/" + m_config.master_fname;
}


void AnaReaderMaster::run() {
  m_config.dump(::stdout);
  wait_for_SWMR_access_to_master();
  analysis_loop();
}


void AnaReaderMaster::analysis_loop() {
  //  hid_t detector_fiducials_dset = NONNEG( 
}


void AnaReaderMaster::wait_for_SWMR_access_to_master() {
  const int microseconds_to_wait = 100000;
  while (true) {
    FILE *fp = fopen(m_master_fname.c_str(), "r");
    if (NULL != fp) {
      if (m_config.verbose) fprintf(stdout, "ana_reader_master: found %s\n" , m_master_fname.c_str());
      fclose(fp);
      m_master_fid = POS( H5Fopen(m_master_fname.c_str(), H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, H5P_DEFAULT) );
      return;
    } else {
      if (m_config.verbose) fprintf(stdout, "%s_%d: still waiting for %s\n" , m_config.group.c_str(), m_config.id, m_master_fname.c_str());
      usleep(microseconds_to_wait);
    }
  }
  
}



AnaReaderMaster::~AnaReaderMaster() {
}


int main(int argc, char *argv[]) {
  AnaReaderMasterConfig config;
  if (not config.parse_command_line(argc, argv, usage)) {
    return -1;
  }
  H5open();
  try {
    AnaReaderMaster anaReaderMaster(config);
    anaReaderMaster.run();
  } catch (...) {
    H5close();
    throw;
  }
  H5close();
  
  return 0;
}

