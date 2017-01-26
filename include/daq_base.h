#ifndef DAQ_BASE_H
#define DAQ_BASE_H

#include<string>

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

 public:
  DaqBase(const DaqBaseConfig &base_config);
  virtual ~DaqBase();
  
 protected:
  void write_pid_file();
};

#endif
