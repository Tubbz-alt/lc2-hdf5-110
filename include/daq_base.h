#ifndef DAQ_BASE_H
#define DAQ_BASE_H

#include <cstdio>
#include <string>

#include "yaml-cpp/yaml.h"
#include "lc2daq.h"


class DaqBase {
  
 public:
  enum Location {HDF5, PID, LOG, FINISHED};

 protected:

  YAML::Node m_config;
  YAML::Node m_group_config;

  int m_id;
  
  std::string m_group, 
    m_basename, 
    m_fname_h5, 
    m_fname_pid, 
    m_fname_finished;

  hid_t m_small_group, 
    m_vlen_group, 
    m_cspad_group;

  typedef std::map<int, hid_t> TSubMap;
  TSubMap m_small_id_to_number_group,
    m_vlen_id_to_number_group,
    m_cspad_id_to_number_group;
  
  std::chrono::time_point<Clock> m_t0, m_t1;

  void run_setup();
  void write_pid_file();

  void create_standard_groups(hid_t parent);
  void create_number_groups(hid_t parent, TSubMap &name_to_group, int first, int count);
  void close_number_groups(TSubMap &name_to_group);
  void close_standard_groups();
  
  std::string form_fullpath(std::string group, int idx, enum Location location);

  static std::string form_basename(std::string group, int idx);
  static hid_t get_dataset(hid_t fid_parent, const char *group1, int group2, const char *dsetname);

  static void load_cspad(const std::string &h5_filename,
                         const std::string &dataset,
                         int length,
                         std::vector<short> &cspad_buffer);
  
 public:

  /**
   * argv should be "config.yaml","id", group should be one of the config.yaml entries
   * sets filenames based on group and id, i.e, h5, pid, finished.
   */ 
  DaqBase(int argc, char *argv[], const char *group);
  
  virtual ~DaqBase();

};

#endif // DAQ_BASE_HH
