#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <map>
#include <chrono>

#include "hdf5.h"

#include "ana_daq_util.h"

typedef std::chrono::high_resolution_clock Clock;

const std::string usage("daq_writer - takes the following arguments:\n "
"  verbose  integer verbosity level, 0,1, etc\n"
"  rundir   string, the output directory\n"
"  group    string, this processes group\n"                        
"  id       int,    this processes id within that group\n"

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

"  writers_hang   have writers hang when done, for debugging process control\n"
"\n");

struct DaqWriterConfig {
  int verbose;
  std::string rundir;
  std::string group;
  int id;

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

  int writers_hang;

  static const int num_args = 26;
  
  void dump(FILE *fout);
};

void DaqWriterConfig::dump(FILE *fout) {
  fprintf(fout, "DaqWriterConfig -- %d args\n", num_args);
  fprintf(fout, "    verbose=%d\n", verbose);
  fprintf(fout, "    rundir=%s\n", rundir.c_str());
  fprintf(fout, "    group=%s\n", group.c_str());
  fprintf(fout, "    id=%d\n", id);
  fprintf(fout, "    num_shots=%ld\n", num_shots);
  fprintf(fout, "    small_name_first=%d\n", small_name_first);
  fprintf(fout, "    vlen_name_first=%d\n", vlen_name_first);
  fprintf(fout, "    detector_name_first=%d\n", detector_name_first);
  fprintf(fout, "    small_name_count=%d\n", small_name_count);
  fprintf(fout, "    vlen_name_count=%d\n", vlen_name_count);
  fprintf(fout, "    detector_name_count=%d\n", detector_name_count);
  fprintf(fout, "    small_shot_first=%d\n", small_shot_first);
  fprintf(fout, "    vlen_shot_first=%d\n", vlen_shot_first);
  fprintf(fout, "    detector_shot_first=%d\n", detector_shot_first);
  fprintf(fout, "    small_shot_stride=%d\n", small_shot_stride);
  fprintf(fout, "    vlen_shot_stride=%d\n", vlen_shot_stride);
  fprintf(fout, "    detector_shot_stride=%d\n", detector_shot_stride);
  fprintf(fout, "    small_chunksize=%d\n", small_chunksize);
  fprintf(fout, "    vlen_chunksize=%d\n", vlen_chunksize);
  fprintf(fout, "    detector_chunksize=%d\n", detector_chunksize);
  fprintf(fout, "    vlen_min_per_shot=%d\n", vlen_min_per_shot);
  fprintf(fout, "    vlen_max_per_shot=%d\n", vlen_max_per_shot);
  fprintf(fout, "    detector_rows=%d\n", detector_rows);
  fprintf(fout, "    detector_columns=%d\n", detector_columns);
  fprintf(fout, "    flush_interval=%d\n", flush_interval);
  fprintf(fout, "    writers_hang=%d\n", writers_hang);
  fflush(fout);
}

class DaqWriter {
  DaqWriterConfig config;
  std::string basename, fname_h5, fname_log, fname_pid, fname_finished;
  FILE *log;

  hid_t m_fid, m_fapl, m_smallGroup, m_vlenGroup, m_detectorGroup;

  std::map<int, hid_t> m_smallDsetGroups, m_vlenDsetGroups, m_detectorDsetGroups;

  Clock t0, t1;

public:
  DaqWriter(const DaqWriterConfig & config_arg);
  ~DaqWriter();

  void run();
  void createFile();
  void createAllGroupsDatasetsAndAttributes();
  void start_SWMR_access_to_file();
  void write(long fiducial);
  void flushData();

protected:
  void check_nonneg(herr_t, int);
  void write_pid_file();
  void createDsetGroups(hid_t, std::map<int, hid_t> &, int, int);
};


DaqWriter::DaqWriter(const DaqWriterConfig & config_arg)
  : config(config_arg)
{
  static char idstr[128];
  sprintf(idstr, "%4.4d", config.id);
  basename = config.group + "-r" + idstr;
  fname_h5 = config.rundir + "/hdf5/" + basename + ".h5";
  fname_log = config.rundir + "/logs/" + basename + ".log"; 
  fname_pid = config.rundir + "/pids/" + basename + ".pid";
  fname_finished = config.rundir + "/logs/" + basename + ".finished";
  write_pid_file();
  log = fopen(fname_log.c_str(),"w");
  if ((NULL == log)) {
    std::cerr << "Could not create a log file, tried: " << fname_log << std::endl;
    throw std::runtime_error("FATAL - DaqWriter - logfile");
  }
}

void DaqWriter::write_pid_file() {
  pid_t pid = ::getpid();
  char hostname[256];
  if (0 != gethostname(hostname, 256)) {
    sprintf(hostname, "--unknown--");
    std::cerr << "DaqWriter: gethostname failed in write_pid_file" << std::endl;
  }

  FILE *pid_f = ::fopen(fname_pid.c_str(), "w");
  if (NULL == pid_f) {
    std::cerr << "Could not create file: " << fname_pid << std::endl;
    throw std::runtime_error("FATAL - write_pid_file");
  }
  fprintf(pid_f, "group=%s idx=%d hostname=%s pid=%d\n", 
          config.group.c_str(), config.id, hostname, pid);
  fclose(pid_f);
}


DaqWriter::~DaqWriter() {
  fclose(log);
  FILE *finished_f = fopen(fname_finished.c_str(), "w");
  if (NULL==finished_f) {
    std::cerr << "could not create finished file\n" << std::endl;
    return;
  }
  fprintf(finished_f,"done.\n");
  fclose(finished_f);
}


void DaqWriter::run() {
  config.dump(log);
  createFile();
  createAllGroupsDatasetsAndAttributes();
  start_SWMR_access_to_file();
  for (long fiducial = 0; fiducial < config.num_shots; ++fiducial) {
    write(fiducial);
    if (fiducial % config.flush_interval) {
      flushData();
    }
  }
  if (config.writers_hang != 0) {
    fprintf(log, "MSG: hanging\n");
    fflush(log);
    while (true) {}
  }
  check_nonneg( H5Fclose(m_fid), __LINE__);
}


void DaqWriter::createFile() {
  m_fapl = H5Pcreate(H5P_FILE_ACCESS);
  check_nonneg( H5Pset_libver_bounds(m_fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST), __LINE__ );
  m_fid = H5Fcreate(fname_h5.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, m_fapl);
  check_nonneg(m_fid, __LINE__);
};


void DaqWriter::createAllGroupsDatasetsAndAttributes() {
  m_smallGroup = H5Gcreate2(m_fid, "small", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  m_vlenGroup = H5Gcreate2(m_fid, "vlen", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  m_detectorGroup = H5Gcreate2(m_fid, "detctor", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  check_nonneg(m_smallGroup, __LINE__);
  check_nonneg(m_vlenGroup, __LINE__);
  check_nonneg(m_detectorGroup, __LINE__);  

  createDsetGroups(m_smallGroup, m_smallDsetGroups, 
                   config.small_name_first, config.small_name_count);
  createDsetGroups(m_vlenGroup, m_vlenDsetGroups, 
                   config.vlen_name_first, config.vlen_name_count);
  createDsetGroups(m_detectorGroup, m_detectorDsetGroups, 
                   config.detector_name_first, config.detector_name_count);

  
};

void DaqWriter::createDsetGroups(hid_t parent, std::map<int, hid_t> &name2group, int first, int count) {
  for (int name = first; name < (first + count); ++name) {
    char strname[128];
    sprintf(strname, "%5.5d", name);
    hid_t dsetGroup = H5Gcreate2(parent, strname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    check_nonneg(dsetGroup, __LINE__);
    name2group[name]=dsetGroup;    
  } 
}


void DaqWriter::start_SWMR_access_to_file() {
  check_nonneg(H5Fstart_swmr_write(m_fid), __LINE__);
};


void DaqWriter::write(long fiducial) {
};


void DaqWriter::flushData() {
};

void DaqWriter::check_nonneg(herr_t err, int lineno) {
  if (err < 0) {
    fprintf(log, "ERROR: check_nonneg line=%d\n", lineno);
    throw std::runtime_error("FATAL: check_nonneg");
  } 
}

int main(int argc, char *argv[]) {
  DaqWriterConfig config;
  if (argc-1 != DaqWriterConfig::num_args) {
    std::cerr << "ERROR: need " << DaqWriterConfig::num_args << " arguments, but received " << argc-1 << std::endl;
    std::cerr << usage << std::endl;
    return -1;
  }
  int idx = 1;
  config.verbose = atoi(argv[idx++]);
  config.rundir = std::string(argv[idx++]);
  config.group = std::string(argv[idx++]);
  config.id = atoi(argv[idx++]);
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
  config.writers_hang = atoi(argv[idx++]);

  std::cout << "daq_writer: " << foo() << std::endl;
  DaqWriter daqWriter(config);
  daqWriter.run();

  return 0;
}
