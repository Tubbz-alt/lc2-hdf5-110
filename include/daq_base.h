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

  hid_t m_small_group, m_vlen_group, m_detector_group;

  std::map<int, hid_t> m_small_id_to_number_group,
    m_vlen_id_to_number_group,
    m_detector_id_to_number_group;

  std::chrono::time_point<Clock> m_t0, m_t1;
  
  static std::string form_basename(std::string group, int idx);

  void run_setup();
  void write_pid_file();

  void create_standard_groups(hid_t parent);
  void create_number_groups(hid_t parent, std::map<int, hid_t> &name_to_group, int first, int count);

  static hid_t get_dataset(hid_t fid_parent, const char *group1, int group2, const char *dsetname);
  
 public:

  DaqBase(const DaqBaseConfig &base_config);
  virtual ~DaqBase();
  void dump(FILE *);

};

#endif
