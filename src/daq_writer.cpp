#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <stdexcept>
#include "hdf5.h"

#include "ana_daq_util.h"

const std::string usage("daq_writer - takes the following arguments:\n "
"  verbose  integer verbosity level, 0,1, etc\n"
"  h5output full path of h5output filename for this writer\n"
"  logstd   full path of log text file stdout from this writer\n"
"  logerr   full path of log test file stderr from this writer\n"

"  num_shots     int, how many shots will the DAQ write in this run\n"

"  small_name_first     int, first small dataset to write\n"
"  vlen_name_first      int, first vlen dataset to write\n"
"  detector_name_first  int, first detector dataset to write\n"

"  small_name_count     int, count of small datasets to write\n"
"  vlen_name_count      int, count of vlen datasets to write\n"
"  detector_name_count  int, count of detector datasets to write\n"

"  small_shot_first   int, which shot, in the global timing counter for all writers, to start writing small datasets\n"
"  vlen_shot_first   int, which shot, in the global timing counter for all writers, to start writing vlen datasets\n"
"  detector_shot_first   int, which shot, in the global timing counter for all writers, to start writing detector datasets\n"

"  small_shot_stride   int, which shot, in the global timing counter for all writers, to stride writing small datasets\n"
"  vlen_shot_stride   int, which shot, in the global timing counter for all writers, to stride writing vlen datasets\n"
"  detector_shot_stride   int, which shot, in the global timing counter for all writers, to stride writing detector datasets\n"

"  small_chunksize     int, number of elements in a small\n"
"  vlen_chunksize      int, number of elements in a vlen\n"
"  detector_chunksize  int, number of elements in a detector chunk\n"

"  vlen_min_per_shot int\n"
"  vlen_max_per_shot  int\n"

"  detector_rows\n"
"  detector_columns\n"

"  flush_interval  how many fiducials between flushes\n"
"\n");

struct DaqWriterConfig {
  int verbose;
  std::string h5output;
  std::string logstd;
  std::string logerr;

  long num_shots;

  int small_name_first;
  int vlen_name_first;
  int detector_name_first;

  int  small_name_count;
  int vlen_name_count;
  int detector_name_count;

  int small_shot_first;
  int vlen_shot_first;
  int detector_shot_first;

  int small_shot_stride;
  int vlen_shot_stride;
  int detector_shot_stride;

  int small_chunksize;
  int vlen_chunksize;
  int detector_chunksize;

  int vlen_min_per_shot;
  int vlen_max_per_shot;

  int detector_rows;
  int detector_columns;

  int flush_interval;
  
  static const int num_args = 25;
};

class DaqWriter {
  DaqWriterConfig config;
  FILE *logstd, *logerr;
public:
  DaqWriter(const DaqWriterConfig & config_arg);
  ~DaqWriter();

  void run();
  void createFile();
  void createAllGroupsDatasetsAndAttributes();
  void start_SWMR_access_to_file();
  void write(long fiducial);
  void flushData();
};


DaqWriter::DaqWriter(const DaqWriterConfig & config_arg)
  : config(config_arg)
{
  logstd = fopen(config.logstd.c_str(),"w");
  logerr = fopen(config.logerr.c_str(),"w");
  if ((NULL == logstd) or (NULL == logerr)) {
    std::cerr << "Could not create a log file, tried: " << config.logstd << " and " << config.logerr << std::endl;
    throw std::runtime_error("FATAL");
  }
}


DaqWriter::~DaqWriter() {
  fclose(logstd);
  fclose(logerr);
}


void DaqWriter::run() {
  createFile();
  createAllGroupsDatasetsAndAttributes();
  start_SWMR_access_to_file();
  for (long fiducial = 0; fiducial < config.num_shots; ++fiducial) {
    write(fiducial);
    if (fiducial % config.flush_interval) {
      flushData();
    }
  }
}


void DaqWriter::createFile() {
};


void DaqWriter::createAllGroupsDatasetsAndAttributes() {
};


void DaqWriter::start_SWMR_access_to_file() {
};


void DaqWriter::write(long fiducial) {
};


void DaqWriter::flushData() {
};


int main(int argc, char *argv[]) {
  DaqWriterConfig config;
  if (argc-1 != DaqWriterConfig::num_args) {
    std::cerr << "ERROR: need " << DaqWriterConfig::num_args << " arguments, but received " << argc-1 << std::endl;
    std::cerr << usage << std::endl;
    return -1;
  }
  int idx = 1;
  config.verbose = atoi(argv[idx++]);
  config.h5output = std::string(argv[idx++]);
  config.logstd = std::string(argv[idx++]);
  config.logerr = std::string(argv[idx++]);
  config.num_shots = atol(argv[idx++]);
  config.small_name_first = atoi(argv[idx++]);
  config.vlen_name_first = atoi(argv[idx++]);
  config.detector_name_first = atoi(argv[idx++]);
  config.small_name_count = atoi(argv[idx++]);
  config.vlen_name_count = atoi(argv[idx++]);
  config.detector_name_count = atoi(argv[idx++]);
  config.small_shot_first = atoi(argv[idx++]);
  config.vlen_shot_first = atoi(argv[idx++]);
  config.detector_shot_first = atoi(argv[idx++]);
  config.small_shot_stride = atoi(argv[idx++]);
  config.vlen_shot_stride = atoi(argv[idx++]);
  config.detector_shot_stride = atoi(argv[idx++]);
  config.small_chunksize = atoi(argv[idx++]);
  config.vlen_chunksize = atoi(argv[idx++]);
  config.detector_chunksize = atoi(argv[idx++]);
  config.vlen_min_per_shot = atoi(argv[idx++]);
  config.vlen_max_per_shot = atoi(argv[idx++]);
  config.detector_rows = atoi(argv[idx++]);
  config.detector_columns = atoi(argv[idx++]);
  config.flush_interval = atoi(argv[idx++]);

  DaqWriter daqWriter(config);
  daqWriter.run();
    
  std::cout << "daq_writer: foo=" << foo() << std::endl;
  return 0;
}
