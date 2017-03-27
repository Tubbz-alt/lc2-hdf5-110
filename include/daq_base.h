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
  YAML::Node m_process_config;

  int m_id;
  
  std::string m_process, 
    m_basename, 
    m_fname_h5, 
    m_fname_pid, 
    m_fname_finished;

  hid_t m_small_group, 
    m_vlen_group, 
    m_cspad_group;

  typedef std::map<int, hid_t> TSubMap;
  TSubMap m_small_map;
  TSubMap m_vlen_map;
  TSubMap m_cspad_map;
  
  std::chrono::time_point<Clock> m_t0, m_t1;

  void run_setup();
  void write_pid_file();

  void create_standard_groups(hid_t parent);
  void create_number_groups(hid_t parent, TSubMap &sub_map, int first, int count);
  void close_number_groups(TSubMap &sub_map);
  void close_standard_groups();
  
  std::string form_fullpath(std::string process, int idx, enum Location location);

  static std::string form_basename(std::string process, int idx);

  static void load_cspad(const std::string &h5_filename,
                         const std::string &dataset,
                         int length,
                         std::vector<int16_t> &cspad_buffer);
  
 public:

  /**
   * argv should be "config.yaml","id", group should be one of the config.yaml entries
   * sets filenames based on group and id, i.e, h5, pid, finished.
   */ 
  DaqBase(int argc, char *argv[], const char *group);
  
  virtual ~DaqBase();

};

#endif // DAQ_BASE_HH
