#ifndef DAQ_BASE_H
#define DAQ_BASE_H

#include <cstdio>
#include <string>
#include "ana_daq_util.h"

struct DaqBaseConfig {
  int verbose;
  std::string rundir;
  std::string group;
  int id;

  long num_shots;

  int small_chunksize;
  int vlen_chunksize;
  int detector_chunksize;

  int detector_rows;
  int detector_columns;

  int flush_interval;

  int hang;
};

class DaqBase {
  DaqBaseConfig m_base_config;
 protected:
  std::string m_basename, m_fname_h5, m_fname_pid, m_fname_finished;
  static std::string form_basename(std::string group, int idx);
 public:
  DaqBase(const DaqBaseConfig &base_config);
  virtual ~DaqBase();
  void dump(FILE *);
 protected:

  std::chrono::time_point<Clock> m_t0, m_t1;
  void run_setup();
  void write_pid_file();
};

#endif
