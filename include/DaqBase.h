#ifndef DAQ_BASE_H
#define DAQ_BASE_H

#include <cstdio>
#include <string>
#include <chrono>

#include "yaml-cpp/yaml.h"
#include "hdf5.h"
#include "Dset.h"

typedef std::chrono::high_resolution_clock Clock;

// map "fiducials" or "milli" to a Dset
typedef std::map<std::string, Dset> Name2Dset;

// map group numbers, i.e, 00000, to above
typedef std::map<int, Name2Dset> Number2Dsets;

const int CSPadDim1 = 32;
const int CSPadDim2 = 185;
const int CSPadDim3 = 388;
const int CSPadNumElem = CSPadDim1*CSPadDim2*CSPadDim3;


//-----------------------------------------------------
// returns { 'small':[f,d,n],
//           'cspad':[f,d,n],
//           'vlen':[f,b,bc,n]}
std::map<std::string, std::vector<std::string> > get_top_group_to_final_dsets() {
  std::map<std::string, std::vector<std::string> > group2dsets;
  std::vector<std::string> not_vlen, vlen;
  not_vlen.push_back(std::string("fiducials"));
  not_vlen.push_back(std::string("data"));
  not_vlen.push_back(std::string("milli"));
  
  vlen.push_back(std::string("fiducials"));
  vlen.push_back(std::string("blob"));
  vlen.push_back(std::string("blobcount"));
  vlen.push_back(std::string("blobstart"));
  vlen.push_back(std::string("milli"));
  
  group2dsets[std::string("small")] = not_vlen;
  group2dsets[std::string("cspad")] = not_vlen;
  group2dsets[std::string("vlen")] = vlen;

  return group2dsets;
}


//-----------------------------------------------------
class DaqBase {
  
 public:
  enum Location {HDF5, PID, LOG, FINISHED};
  enum DsetAccess {CREATE_DSETS, OPEN_DSETS};

 protected:

  YAML::Node m_config;
  YAML::Node m_process_config;

  int m_id;
  
  std::string m_process, 
    m_basename, 
    m_fname_h5, 
    m_fname_pid, 
    m_fname_finished;

  // overall schema, 'vlen' -> ...
  std::map<std::string, Number2Dsets> m_topGroups;

  // map "small", "vlen", "cspad" to the list of dsets, ie ["fiducials", "blobdata", ...]
  std::map<std::string, std::vector<std::string> > m_group2dsets;

  // examples of access
  //     get at a specific dset
  //   m_topGroups["small"][0]["fiducials"].dset_id
  //                                       .dims
  //
  //   loop over all numbered dsets for a top group/ dset name
  // for (int sub=0; sub < m_top_group_2_num_subgroups["small"]; ++sub) {
  //      m_topGroups["small"][sub]["fiducials"]
  //
  //   loop over all numbers/dsets for a top group
  // for (int sub=0; sub < m_top_group_2_num_subgroups["small"]; ++sub) {
  //   for (auto dsetNameIter = m_group2dsets["small"].begin(); 
  //        dsetNameIter ! m_group2dsets["small"].end(); ++dsetNameIter) {
  //        m_topGroups["small"][sub][*dsetNameIter]
  //
  //   loop over everything
  // for (auto topIter = m_top_group_2_num_subgroups.begin();
  //      topIter != m_top_group_2_num_subgroups.end(); ++topIter) {
  //    std::string topName = topIter->first;
  //    size_t numSub = topIter->second;
  //    for (int sub=0; sub < numSub; ++sub) {
  //       for (auto dsetNameIter = m_group2dsets[topName].begin(); 
  //        dsetNameIter ! m_group2dsets[topName].end(); ++dsetNameIter) {
  //          m_topGroups[topName][sub][*dsetNameIter]
  //
  // }

  hid_t m_small_group,  m_vlen_group, m_cspad_group;

  // schema is /small/0  ... /small/n where 0..n are subgroups, 
  // below map returns the group for 0, n, etc
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
  std::string logHdr();

  std::string form_fullpath(std::string process, int idx, enum Location location);

  
  static std::string form_basename(std::string process, int idx);

  void load_cspad(const std::string &h5_filename,
                  const std::string &dataset,
                  int length,
                  std::vector<int16_t> &cspad_buffer);
  
 public:

  /**
   * argv should be "config.yaml","id", group should be one of the config.yaml entries
   * sets filenames based on group and id, i.e, h5, pid, finished.
   */ 
  DaqBase(int argc, char *argv[], const char *group);

  
  typedef const std::map<std::string, std::pair<int,int> > SubGroup2StartCount;
  void initialize(hid_t parent, const SubGroup2StartCount &subgroup2startcount);

  virtual ~DaqBase();

};

#endif // DAQ_BASE_HH
