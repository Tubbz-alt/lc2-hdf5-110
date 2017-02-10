#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <map>

#include "lc2daq.h"

const std::string usage("daq_writer - takes the following arguments:\n "
"  verbose  integer verbosity level, 0,1, etc\n"
"  rundir   string, the output directory\n"
"  group    string, this processes group\n"                        
"  id       int,    this processes id within that group\n"

"  num_shots     int, how many shots will the DAQ write in this run\n"

"  small_name_first     int, first small dataset to write\n"
"  vlen_name_first      int, first vlen dataset to write\n"
"  detector_name_first  int, first detector dataset to write\n"

"  small_count     int, count of small datasets to write\n"
"  vlen_count      int, count of vlen datasets to write\n"
"  detector_count  int, count of detector datasets to write\n"

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

struct DaqWriterConfig : DaqBaseConfig {
  int small_name_first;
  int vlen_name_first;
  int detector_name_first;

  int small_count;
  int vlen_count;
  int detector_count;

  int small_shot_first;
  int vlen_shot_first;
  int detector_shot_first;

  int small_shot_stride;
  int vlen_shot_stride;
  int detector_shot_stride;

  int vlen_min_per_shot;
  int vlen_max_per_shot;

  static const int num_args = 26;
  
  void dump(FILE *fout);
  bool parse_command_line(int argc, char *argv[], const std::string &usage);
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
  fprintf(fout, "    small_count=%d\n", small_count);
  fprintf(fout, "    vlen_count=%d\n", vlen_count);
  fprintf(fout, "    detector_count=%d\n", detector_count);
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
  fprintf(fout, "    hang=%d\n", hang);
  fflush(fout);
}

bool DaqWriterConfig::parse_command_line(int argc, char *argv[], const std::string &usage) {
  if (argc-1 != DaqWriterConfig::num_args) {
    std::cerr << "ERROR: need " << DaqWriterConfig::num_args << " arguments, but received " << argc-1 << std::endl;
    std::cerr << usage << std::endl;
    return false;
  }
  int idx = 1;
  verbose = atoi(argv[idx++]);
  rundir = std::string(argv[idx++]);
  group = std::string(argv[idx++]);
  id = atoi(argv[idx++]);
  num_shots = atol(argv[idx++]);
  small_name_first = atoi(argv[idx++]);
  vlen_name_first = atoi(argv[idx++]);
  detector_name_first = atoi(argv[idx++]);
  small_count = atoi(argv[idx++]);
  vlen_count = atoi(argv[idx++]);
  detector_count = atoi(argv[idx++]);
  small_shot_first = atoi(argv[idx++]);
  vlen_shot_first = atoi(argv[idx++]);
  detector_shot_first = atoi(argv[idx++]);
  small_shot_stride = atoi(argv[idx++]);
  vlen_shot_stride = atoi(argv[idx++]);
  detector_shot_stride = atoi(argv[idx++]);
  small_chunksize = atoi(argv[idx++]);
  vlen_chunksize = atoi(argv[idx++]);
  detector_chunksize = atoi(argv[idx++]);
  vlen_min_per_shot = atoi(argv[idx++]);
  vlen_max_per_shot = atoi(argv[idx++]);
  detector_rows = atoi(argv[idx++]);
  detector_columns = atoi(argv[idx++]);
  flush_interval = atoi(argv[idx++]);
  hang = atoi(argv[idx++]);
  return true;
}

class DaqWriter : public DaqBase {
  DaqWriterConfig m_config;

  hid_t m_writer_fid;

  std::map<int, hid_t> m_small_id_to_number_group,
    m_vlen_id_to_number_group,
    m_detector_id_to_number_group;

  std::map<int, DsetInfo> m_small_id_to_fiducials_dset,
    m_vlen_id_to_fiducials_dset,
    m_detector_id_to_fiducials_dset;

  std::map<int, DsetInfo> m_small_id_to_nano_dset,
    m_vlen_id_to_nano_dset,
    m_detector_id_to_nano_dset;

  std::map<int, DsetInfo> m_small_id_to_data_dset,
    m_vlen_id_to_blob_dset,
    m_detector_id_to_data_dset;

  std::map<int, DsetInfo> m_vlen_id_to_blob_start_dset,
    m_vlen_id_to_blob_count_dset;
  
  int m_next_small, m_next_vlen, m_next_detector;
  int m_next_vlen_count;
  std::vector<long> m_vlen_data;
  std::vector<short> m_detector_data;
  
public:
  DaqWriter(const DaqWriterConfig & config_arg);
  ~DaqWriter();

  void run();
  void create_file();
  void create_all_groups_datasets_and_attributes();
  void start_SWMR_access_to_file();
  void write(long fiducial);
  void flush_data(long fiducial);

protected:
  void create_fiducials_dsets(const std::map<int, hid_t> &, std::map<int, DsetInfo> &);
  void create_nano_dsets(const std::map<int, hid_t> &, std::map<int, DsetInfo> &);

  void create_small_data_dsets();
  void create_detector_data_dsets();
  void create_vlen_blob_and_index_dsets();

  void write_small(long fiducial);
  void write_vlen(long fiducial);
  void write_detector(long fiducial);

  void create_small_dsets_helper(const std::map<int, hid_t> &,
                                 std::map<int, DsetInfo> &,
                                 const char *, hid_t, size_t, int);

  void flush_helper(const std::map<int, DsetInfo> &);

};


DaqWriter::DaqWriter(const DaqWriterConfig & config)
  : DaqBase(config), m_config(config)
{
  m_next_small = m_config.small_shot_first;
  m_next_vlen = m_config.vlen_shot_first;
  m_next_detector = m_config.detector_shot_first;
  m_next_vlen_count = m_config.vlen_min_per_shot;
  m_vlen_data.resize(m_config.vlen_max_per_shot);
  m_detector_data.resize(m_config.detector_columns * m_config.detector_rows);
  write_pid_file();
}


DaqWriter::~DaqWriter() {}


void DaqWriter::run() {
  DaqBase::run_setup();
  m_config.dump(::stdout);
  create_file();
  create_all_groups_datasets_and_attributes();
  start_SWMR_access_to_file();
  long fiducial = -1;
  for (fiducial = 0; fiducial < m_config.num_shots; ++fiducial) {
    write(fiducial);
    if ((fiducial > 0) and (0 == (fiducial % m_config.flush_interval))) {
      flush_data(fiducial);
    }
  }
  if (m_config.hang != 0) {
    printf("MSG: hanging\n");
    fflush(::stdout);
    while (true) {}
  }
  NONNEG( H5Fclose(m_writer_fid) );
  m_t1 = Clock::now();

  auto total_diff = m_t1 - m_t0;
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(total_diff);
  std::cout << "num seconds=" << seconds.count() << " num events=" << fiducial << std::endl;  
}


void DaqWriter::create_file() {
  hid_t fapl = NONNEG( H5Pcreate(H5P_FILE_ACCESS) );
  NONNEG( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
  m_writer_fid = NONNEG( H5Fcreate(m_fname_h5.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
  if (m_config.verbose) {
    printf("created file: %s\n", m_fname_h5.c_str());
    fflush(::stdout);
  }
  NONNEG( H5Pclose(fapl) );
};


void DaqWriter::create_all_groups_datasets_and_attributes() {
  DaqBase::create_standard_groups(m_writer_fid);
  
  DaqBase::create_number_groups(m_small_group, m_small_id_to_number_group, 
                   m_config.small_name_first, m_config.small_count);
  DaqBase::create_number_groups(m_vlen_group, m_vlen_id_to_number_group, 
                   m_config.vlen_name_first, m_config.vlen_count);
  DaqBase::create_number_groups(m_detector_group, m_detector_id_to_number_group, 
                   m_config.detector_name_first, m_config.detector_count);

  create_fiducials_dsets(m_small_id_to_number_group, m_small_id_to_fiducials_dset);
  create_fiducials_dsets(m_vlen_id_to_number_group, m_vlen_id_to_fiducials_dset);
  create_fiducials_dsets(m_detector_id_to_number_group, m_detector_id_to_fiducials_dset);

  create_nano_dsets(m_small_id_to_number_group, m_small_id_to_nano_dset);
  create_nano_dsets(m_vlen_id_to_number_group, m_vlen_id_to_nano_dset);
  create_nano_dsets(m_detector_id_to_number_group, m_detector_id_to_nano_dset);

  create_small_data_dsets();
  create_detector_data_dsets();
  create_vlen_blob_and_index_dsets();
  
  if (m_config.verbose) {
    printf("created all groups and datasets: %s\n", m_fname_h5.c_str());
    fflush(::stdout);
  }
};

void DaqWriter::create_small_dsets_helper(const std::map<int, hid_t> &id_to_parent,
                                          std::map<int, DsetInfo> &id_to_dset,
                                          const char *dset_name,
                                          hid_t h5_type,
                                          size_t type_size_bytes,
                                          int chunksize)
{
  for (CMapIter iter = id_to_parent.begin(); iter != id_to_parent.end(); ++iter) {
    int group_id = iter->first;
    hid_t h5_group = iter->second;
    if (id_to_dset.find(group_id) != id_to_dset.end()) {
      throw std::runtime_error("create_small_dsets_helper, id already in map");
    }
    id_to_dset[group_id] = ::create_1d_dataset(h5_group, dset_name, h5_type, chunksize, type_size_bytes);
  }
}

void DaqWriter::create_fiducials_dsets(const std::map<int, hid_t> &id_to_number_group, std::map<int, DsetInfo> &id_to_dset) {
  create_small_dsets_helper(id_to_number_group, id_to_dset,
                            "fiducials", H5T_NATIVE_LONG, 8, m_config.small_chunksize);
}
  
void DaqWriter::create_nano_dsets(const std::map<int, hid_t> &id_to_number_group, std::map<int, DsetInfo> &id_to_dset) {
  create_small_dsets_helper(id_to_number_group, id_to_dset,
                            "nano", H5T_NATIVE_LONG, 8, m_config.small_chunksize);
}
  
void DaqWriter::create_small_data_dsets() {
  create_small_dsets_helper(m_small_id_to_number_group, m_small_id_to_data_dset,
                            "data", H5T_NATIVE_LONG, 8, m_config.small_chunksize);
}
  
void DaqWriter::create_detector_data_dsets() {
  size_t size_bytes = 2;
  for (CMapIter iter = m_detector_id_to_number_group.begin(); iter != m_detector_id_to_number_group.end(); ++iter) {
    int group_id = iter->first;
    hid_t h5_group = iter->second;
    if (m_detector_id_to_data_dset.find(group_id) != m_detector_id_to_data_dset.end()) {
      throw std::runtime_error("create_detector_data_dsets, id already in map");
    }

    m_detector_id_to_data_dset[group_id] = ::create_3d_dataset(h5_group, "data", H5T_NATIVE_SHORT,
                                                               m_config.detector_rows,
                                                               m_config.detector_columns,
                                                               m_config.detector_chunksize,
                                                               size_bytes);
  }
}
  
void DaqWriter::create_vlen_blob_and_index_dsets() {
  create_small_dsets_helper(m_vlen_id_to_number_group, m_vlen_id_to_blob_dset,
                            "blob", H5T_NATIVE_LONG, 8, m_config.small_chunksize);
  create_small_dsets_helper(m_vlen_id_to_number_group, m_vlen_id_to_blob_start_dset,
                            "blobstart", H5T_NATIVE_LONG, 8, m_config.small_chunksize);
  create_small_dsets_helper(m_vlen_id_to_number_group, m_vlen_id_to_blob_count_dset,
                            "blobcount", H5T_NATIVE_LONG, 8, m_config.small_chunksize);
}
    

void DaqWriter::start_SWMR_access_to_file() {
  NONNEG( H5Fstart_swmr_write(m_writer_fid) );
  if (m_config.verbose) {
    printf("started SWMR access\n");
    fflush(::stdout);
  }
};


void DaqWriter::write(long fiducial) {
  if (m_config.verbose >= 2) {
    printf("entering write(%ld)\n", fiducial);
    fflush(::stdout);
  }
  write_small(fiducial);
  write_vlen(fiducial);
  write_detector(fiducial);
}


void DaqWriter::write_small(long fiducial) {
  if (fiducial == m_next_small) {
    m_next_small += std::max(1, m_config.small_shot_stride);
    auto small_time = Clock::now();
    auto diff = small_time - m_t0;
    auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(diff);
    for (int small_id = m_config.small_name_first;
         small_id < m_config.small_name_first + m_config.small_count;
         ++small_id)
      {
        DsetInfo & fid_dset = m_small_id_to_fiducials_dset[small_id];
        DsetInfo & nano_dset = m_small_id_to_nano_dset[small_id];
        DsetInfo & data_dset = m_small_id_to_data_dset[small_id];

        ::append_to_1d_dset(fid_dset, fiducial);
        ::append_to_1d_dset(nano_dset, nano.count());
        ::append_to_1d_dset(data_dset, fiducial);        
      }
  }
}


void DaqWriter::write_vlen(long fiducial) {
  if (fiducial == m_next_vlen) {
    m_next_vlen += std::max(1, m_config.vlen_shot_stride);
    auto vlen_time = Clock::now();
    auto diff = vlen_time - m_t0;
    auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(diff);

    m_next_vlen_count += 1;
    m_next_vlen_count %= m_config.vlen_max_per_shot;
    m_next_vlen_count = std::max(m_config.vlen_min_per_shot, m_next_vlen_count);
    for (size_t idx = 0; idx < unsigned(m_next_vlen_count); ++idx) m_vlen_data[idx]=fiducial;
      
    for (int vlen_id = m_config.vlen_name_first;
         vlen_id < m_config.vlen_name_first + m_config.vlen_count;
         ++vlen_id)
      {
        DsetInfo & fid_dset = m_vlen_id_to_fiducials_dset[vlen_id];
        DsetInfo & nano_dset = m_vlen_id_to_nano_dset[vlen_id];
        DsetInfo & blobdata_dset = m_vlen_id_to_blob_dset[vlen_id];
        DsetInfo & blobstart_dset = m_vlen_id_to_blob_start_dset[vlen_id];
        DsetInfo & blobcount_dset = m_vlen_id_to_blob_count_dset[vlen_id];

        ::append_to_1d_dset(fid_dset, fiducial);
        ::append_to_1d_dset(nano_dset, nano.count());
        long start_idx = ::append_many_to_1d_dset(blobdata_dset, m_next_vlen_count, &m_vlen_data[0]);
        ::append_to_1d_dset(blobstart_dset, start_idx);
        ::append_to_1d_dset(blobcount_dset, m_next_vlen_count);
      }
  }
}


void DaqWriter::write_detector(long fiducial) {
  if (fiducial == m_next_detector) {
    m_next_detector += std::max(1, m_config.detector_shot_stride);
    auto detector_time = Clock::now();
    auto diff = detector_time - m_t0;
    auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(diff);
    for (std::vector<short>::iterator idx = m_detector_data.begin();
         idx != m_detector_data.end(); ++idx) {
      *idx = short(fiducial);
    }
      
    for (int detector_id = m_config.detector_name_first;
         detector_id < m_config.detector_name_first + m_config.detector_count;
         ++detector_id)
      {
        DsetInfo & fid_dset = m_detector_id_to_fiducials_dset[detector_id];
        DsetInfo & nano_dset = m_detector_id_to_nano_dset[detector_id];
        DsetInfo & data_dset = m_detector_id_to_data_dset[detector_id];

        ::append_to_1d_dset(fid_dset, fiducial);
        ::append_to_1d_dset(nano_dset, nano.count());
        ::append_to_3d_dset(data_dset, m_config.detector_rows, m_config.detector_columns, &m_detector_data[0]);
      }
  }
};


void DaqWriter::flush_helper(const std::map<int, DsetInfo> &id_to_dset) {
  typedef std::map<int, DsetInfo>::const_iterator Iter;
  for (Iter iter = id_to_dset.begin(); iter != id_to_dset.end(); ++iter) {
    const DsetInfo &dsetInfo = iter->second;
    NONNEG( H5Dflush(dsetInfo.dset_id) );
  }
}

void DaqWriter::flush_data(long fiducial) {
  if (m_config.verbose) {
    printf("flush_data: fiducial=%ld\n", fiducial);
    fflush(::stdout);
  }
  flush_helper(m_small_id_to_fiducials_dset);
  flush_helper(m_small_id_to_nano_dset);
  flush_helper(m_small_id_to_data_dset);

  flush_helper(m_vlen_id_to_fiducials_dset);
  flush_helper(m_vlen_id_to_nano_dset);
  flush_helper(m_vlen_id_to_blob_dset);
  flush_helper(m_vlen_id_to_blob_count_dset);
  flush_helper(m_vlen_id_to_blob_start_dset);

  flush_helper(m_detector_id_to_fiducials_dset);
  flush_helper(m_detector_id_to_nano_dset);
  flush_helper(m_detector_id_to_data_dset);
};

int main(int argc, char *argv[]) {
  DaqWriterConfig config;
  if (not config.parse_command_line(argc, argv, usage)) {
    return -1;
  }
  
  H5open();
  try {
    DaqWriter daqWriter(config);
    daqWriter.run();
  } catch (...) {
    H5close();
    throw;
  }
  H5close();
  return 0;
}
