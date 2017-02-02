#include <cstdio>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <stdexcept>

#include "daq_base.h"

DaqBase::DaqBase(const DaqBaseConfig &base_config) : m_base_config(base_config) {
  m_basename = form_basename(m_base_config.group, m_base_config.id);
  m_fname_h5 = m_base_config.rundir + "/hdf5/" + m_basename + ".h5";
  m_fname_pid = m_base_config.rundir + "/pids/" + m_basename + ".pid";
  m_fname_finished = m_base_config.rundir + "/logs/" + m_basename + ".finished";
}

std::string DaqBase::form_basename(std::string group, int idx) {
  static char idstr[128];
  sprintf(idstr, "%4.4d", idx);
  return group + "-s" + idstr;
}

void DaqBase::dump(FILE *fout) {
  fprintf(fout, "basename:  %s\n", m_basename.c_str());
  fprintf(fout, "fname_h5:  %s\n", m_fname_h5.c_str());
  fprintf(fout, "fname_pid: %s\n", m_fname_pid.c_str());
  
}

void DaqBase::run_setup() {
  std::chrono::time_point<std::chrono::system_clock> start_run, end_run;
  start_run = std::chrono::system_clock::now();
  std::time_t start_run_time = std::chrono::system_clock::to_time_t(start_run);
  m_t0 = Clock::now();
    
  std::cout << m_basename << ": start_time: " << std::ctime(&start_run_time) << std::endl;
}

void DaqBase::write_pid_file() {
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
          m_base_config.group.c_str(), m_base_config.id, hostname, pid);
  fclose(pid_f);
}


void DaqBase::create_standard_groups(hid_t parent) {
  m_small_group = H5Gcreate2(parent, "small", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  m_vlen_group = H5Gcreate2(parent, "vlen", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  m_detector_group = H5Gcreate2(parent, "detector", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  CHECK_NONNEG(m_small_group, "small group");
  CHECK_NONNEG(m_vlen_group, "vlen group");
  CHECK_NONNEG(m_detector_group, "detector group");  
}


void DaqBase::create_number_groups(hid_t parent, std::map<int, hid_t> &name_to_group, int first, int count) {
  for (int name = first; name < (first + count); ++name) {
    char strname[128];
    sprintf(strname, "%5.5d", name);
    hid_t dset_group = H5Gcreate2(parent, strname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    CHECK_NONNEG(dset_group, strname);
    name_to_group[name]=dset_group;
  } 
}


hid_t DaqBase::get_dataset(hid_t fid, const char *group1, int group2, const char *dsetname) {
  hid_t gid_group1 = H5Gopen2(fid, group1, H5P_DEFAULT);
  CHECK_NONNEG(gid_group1, "base - get dataset - first group");

  char group2str[128];
  sprintf(group2str, "%5.5d", group2);
  
  hid_t gid_group2 = H5Gopen2(gid_group1, group2str, H5P_DEFAULT);
  CHECK_NONNEG(gid_group2, "base - get dataset - 2nd group");

  hid_t dset = H5Dopen2(gid_group2, dsetname, H5P_DEFAULT);
  CHECK_NONNEG(dset, "base - get dataset - final dset name");

  CHECK_NONNEG( H5Gclose(gid_group2), "base - get dataset - closing group2");
  CHECK_NONNEG( H5Gclose(gid_group1), "base - get dataset - closing group1");

  return dset;
}


DaqBase::~DaqBase() {
  FILE *finished_f = fopen(m_fname_finished.c_str(), "w");
  if (NULL==finished_f) {
    std::cerr << "could not create finished file\n" << std::endl;
    return;
  }
  fprintf(finished_f,"done.\n");
  fclose(finished_f);
}
