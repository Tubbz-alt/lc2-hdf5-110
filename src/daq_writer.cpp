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
  DaqWriterConfig m_config;
  std::string m_basename, m_fname_h5, m_fname_pid, m_fname_finished;

  hid_t m_fid, m_fapl, m_small_group, m_vlen_group, m_detector_group;

  std::map<int, hid_t> m_small_id_to_group,
    m_vlen_id_to_group,
    m_detector_id_to_group;

  std::map<int, hid_t> m_small_id_to_fiducials_dset,
    m_vlen_id_to_fiducials_dset,
    m_detector_id_to_fiducials_dset;

  std::map<int, hid_t> m_small_id_to_nano_dset,
    m_vlen_id_to_nano_dset,
    m_detector_id_to_nano_dset;

  std::map<int, hid_t> m_small_id_to_data_dset,
    m_vlen_id_to_blob_dset,
    m_detector_id_to_data_dset;

  std::map<int, hid_t> m_vlen_id_to_blob_start_dset,
    m_vlen_id_to_blob_cound_dset;
  
  std::chrono::time_point<Clock> m_t0, m_t1;

public:
  DaqWriter(const DaqWriterConfig & config_arg);
  ~DaqWriter();

  void run();
  void create_file();
  void create_all_groups_datasets_and_attributes();
  void start_SWMR_access_to_file();
  void write(long fiducial);
  void flush_data();

protected:
  void create_dset_groups(hid_t, std::map<int, hid_t> &, int, int);
  void create_fiducials_dsets(const std::map<int, hid_t> &, std::map<int, hid_t> &);
  void create_nano_dsets(const std::map<int, hid_t> &, std::map<int, hid_t> &);

  void create_small_data_dsets();
  void create_detector_data_dsets();
  void create_vlen_blob_dsets();

  void write_pid_file();
};


DaqWriter::DaqWriter(const DaqWriterConfig & config_arg)
  : m_config(config_arg)
{
  static char idstr[128];
  sprintf(idstr, "%4.4d", m_config.id);
  m_basename = m_config.group + "-s" + idstr;
  m_fname_h5 = m_config.rundir + "/hdf5/" + m_basename + ".h5";
  m_fname_pid = m_config.rundir + "/pids/" + m_basename + ".pid";
  m_fname_finished = m_config.rundir + "/logs/" + m_basename + ".finished";
  write_pid_file();
}

void DaqWriter::write_pid_file() {
  pid_t pid = ::getpid();
  char hostname[256];
  if (0 != gethostname(hostname, 256)) {
    sprintf(hostname, "--unknown--");
    std::cerr << "DaqWriter: gethostname failed in write_pid_file" << std::endl;
  }

  FILE *pid_f = ::fopen(m_fname_pid.c_str(), "w");
  if (NULL == pid_f) {
    std::cerr << "Could not create file: " << m_fname_pid << std::endl;
    throw std::runtime_error("FATAL - write_pid_file");
  }
  fprintf(pid_f, "group=%s idx=%d hostname=%s pid=%d\n", 
          m_config.group.c_str(), m_config.id, hostname, pid);
  fclose(pid_f);
}


DaqWriter::~DaqWriter() {
  FILE *finished_f = fopen(m_fname_finished.c_str(), "w");
  if (NULL==finished_f) {
    std::cerr << "could not create finished file\n" << std::endl;
    return;
  }
  fprintf(finished_f,"done.\n");
  fclose(finished_f);
}


void DaqWriter::run() {
  std::chrono::time_point<std::chrono::system_clock> start_run, end_run;
  start_run = std::chrono::system_clock::now();
  std::time_t start_run_time = std::chrono::system_clock::to_time_t(start_run);
  m_t0 = Clock::now();
    
  std::cout << m_basename << ": start_time: " << std::ctime(&start_run_time) << std::endl;
  
  m_config.dump(::stdout);
  create_file();
  create_all_groups_datasets_and_attributes();
  start_SWMR_access_to_file();
  for (long fiducial = 0; fiducial < m_config.num_shots; ++fiducial) {
    write(fiducial);
    if (fiducial % m_config.flush_interval) {
      flush_data();
    }
  }
  if (m_config.writers_hang != 0) {
    printf("MSG: hanging\n");
    fflush(::stdout);
    while (true) {}
  }
  CHECK_NONNEG( H5Fclose(m_fid), "H5Fclose");
}


void DaqWriter::create_file() {
  m_fapl = H5Pcreate(H5P_FILE_ACCESS);
  CHECK_NONNEG( H5Pset_libver_bounds(m_fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST), "set_libver_bounds" );
  m_fid = H5Fcreate(m_fname_h5.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, m_fapl);
  CHECK_NONNEG(m_fid, "creating file");
};


void DaqWriter::create_all_groups_datasets_and_attributes() {
  m_small_group = H5Gcreate2(m_fid, "small", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  m_vlen_group = H5Gcreate2(m_fid, "vlen", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  m_detector_group = H5Gcreate2(m_fid, "detctor", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  CHECK_NONNEG(m_small_group, "small group");
  CHECK_NONNEG(m_vlen_group, "vlen group");
  CHECK_NONNEG(m_detector_group, "detector group");  

  create_dset_groups(m_small_group, m_small_id_to_group, 
                   m_config.small_name_first, m_config.small_name_count);
  create_dset_groups(m_vlen_group, m_vlen_id_to_group, 
                   m_config.vlen_name_first, m_config.vlen_name_count);
  create_dset_groups(m_detector_group, m_detector_id_to_group, 
                   m_config.detector_name_first, m_config.detector_name_count);

  create_fiducials_dsets(m_small_id_to_group, m_small_id_to_fiducials_dset);
  create_fiducials_dsets(m_vlen_id_to_group, m_vlen_id_to_fiducials_dset);
  create_fiducials_dsets(m_detector_id_to_group, m_detector_id_to_fiducials_dset);

  create_nano_dsets(m_small_id_to_group, m_small_id_to_nano_dset);
  create_nano_dsets(m_vlen_id_to_group, m_vlen_id_to_nano_dset);
  create_nano_dsets(m_detector_id_to_group, m_detector_id_to_nano_dset);

  create_small_data_dsets();
  create_detector_data_dsets();
  create_vlen_blob_dsets();
  
};

void DaqWriter::create_dset_groups(hid_t parent, std::map<int, hid_t> &name_to_group, int first, int count) {
  for (int name = first; name < (first + count); ++name) {
    char strname[128];
    sprintf(strname, "%5.5d", name);
    hid_t dset_group = H5Gcreate2(parent, strname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    CHECK_NONNEG(dset_group, strname);
    name_to_group[name]=dset_group;
  } 
}

void DaqWriter::create_fiducials_dsets(const std::map<int, hid_t> &id_to_group, std::map<int, hid_t> &id_to_dset) {
}
  
void DaqWriter::create_nano_dsets(const std::map<int, hid_t> &id_to_group, std::map<int, hid_t> &id_to_dset) {
}
  
void DaqWriter::create_small_data_dsets() {
}
  
void DaqWriter::create_detector_data_dsets() {
}
  
void DaqWriter::create_vlen_blob_dsets() {
}
    

void DaqWriter::start_SWMR_access_to_file() {
  CHECK_NONNEG(H5Fstart_swmr_write(m_fid), "start_swmr");
};


void DaqWriter::write(long fiducial) {
};


void DaqWriter::flush_data() {
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
