#include <string>
#include <vector>
#include <iostream>
#include "hdf5.h"

#include "ana_daq_util.h"

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

"  masters_hang   have masters hang when done, for debugging process control\n");

struct DaqMasterConfig {
  int verbose;
  std::string rundir;
  std::string group;
  int id;

  std::string writer_group;
  int num_writers;
  long num_shots;

  int small_count;
  int vlen_count;
  int detector_count;

  int small_chunksize;
  int vlen_chunksize;
  int detector_chunksize;

  int detector_rows;
  int detector_columns;

  int flush_interval;

  int masters_hang;

  static const int num_args = 17;
  
  void dump(FILE *fout);
};

void DaqMasterConfig::dump(FILE *fout) {
  fprintf(fout, "DaqMasterConfig -- %d args\n", num_args);
  fprintf(fout, "    verbose=%d\n", verbose);
  fprintf(fout, "    rundir=%s\n", rundir.c_str());
  fprintf(fout, "    group=%s\n", group.c_str());
  fprintf(fout, "    id=%d\n", id);
  fprintf(fout, "    writer_group=%s\n", writer_group.c_str());
  fprintf(fout, "    num_writers=%d\n", num_writers);
  fprintf(fout, "    num_shots=%ld\n", num_shots);
  fprintf(fout, "    small_count=%d\n", small_count);
  fprintf(fout, "    vlen_count=%d\n", vlen_count);
  fprintf(fout, "    detector_count=%d\n", detector_count);
  fprintf(fout, "    small_chunksize=%d\n", small_chunksize);
  fprintf(fout, "    vlen_chunksize=%d\n", vlen_chunksize);
  fprintf(fout, "    detector_chunksize=%d\n", detector_chunksize);
  fprintf(fout, "    detector_rows=%d\n", detector_rows);
  fprintf(fout, "    detector_columns=%d\n", detector_columns);
  fprintf(fout, "    flush_interval=%d\n", flush_interval);
  fprintf(fout, "    masters_hang=%d\n", masters_hang);
  fflush(fout);
}

class DaqMaster {
  DaqMasterConfig m_config;
  std::string m_basename, m_fname_h5, m_fname_pid, m_fname_finished;
  std::vector<std::string> m_writer_basenames, m_writer_fnames_h5;

public:
  DaqMaster(const DaqMasterConfig &);
  ~DaqMaster();

  void run();
};

  
  
int main() {
  return 0;
}
