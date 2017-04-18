#include <cstdio>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <stdexcept>

#include "check_macros.h"
#include "DaqBase.h"


DaqBase::DaqBase(int argc, char *argv[], const char *process) : m_process(process) {
  if (argc != 3) {
    std::cerr << "Usage: " << process << " takes 2 arguments: "<< std::endl;
    std::cerr << "       config.yaml"<< std::endl;
    std::cerr << "       id within process"<< std::endl;
    throw std::runtime_error("Invalid command line arguments");
  }

  m_config = YAML::LoadFile(argv[1]);
  m_process_config = m_config[process];
  m_id = atoi(argv[2]);

  m_basename = form_basename(m_process, m_id);
  m_fname_h5 = form_fullpath(m_process, m_id, HDF5);
  m_fname_pid = form_fullpath(m_process, m_id, PID);
  m_fname_finished = form_fullpath(m_process, m_id, FINISHED);

  // will set to "small" -> ["fiducials", "milli", "data"] ...
  m_group2dsets = get_top_group_to_final_dsets();

}


std::string DaqBase::form_basename(std::string process, int idx) {
  static char idstr[128];
  sprintf(idstr, "%4.4d", idx);
  return process + "-s" + idstr;
}

std::string DaqBase::logHdr() {
  auto milli_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count();
  return std::to_string(milli_since_epoch) + " " + m_basename + ": ";
}

std::string DaqBase::form_fullpath(std::string process, int idx, enum Location location) {
  std::string basename = form_basename(process, idx);
  std::string full_path = m_config["rootdir"].as<std::string>() + 
    "/" +
    m_config["rundir"].as<std::string>();
  switch (location) {
  case HDF5:
    full_path += "/hdf5/" + basename + ".h5";
    break;
  case PID:
    full_path += "/pids/" + basename + ".pid";
    break;
  case LOG:
    full_path += "/logs/" + basename + ".log";
    break;
  case FINISHED:
    full_path += "/logs/" + basename + ".finished";
    break;
  }
  return full_path;
}


void DaqBase::run_setup() {
  std::chrono::time_point<std::chrono::system_clock> start_run, end_run;
  start_run = std::chrono::system_clock::now();
  std::time_t start_run_time = std::chrono::system_clock::to_time_t(start_run);
  m_t0 = Clock::now();
    
  std::cout << logHdr() << "start_time: " << std::ctime(&start_run_time) << std::endl;
}


void DaqBase::write_pid_file() {
  pid_t pid = ::getpid();
  char hostname[256];
  if (0 != gethostname(hostname, 256)) {
    sprintf(hostname, "--unknown--");
    std::cerr << logHdr() << "gethostname failed in write_pid_file" << std::endl;
  }

  FILE *pid_f = ::fopen(m_fname_pid.c_str(), "w");
  if (NULL == pid_f) {
    std::cerr << logHdr() << "Could not create file: " << m_fname_pid << std::endl;
    throw std::runtime_error("FATAL - write_pid_file");
  }
  fprintf(pid_f, "process=%s idx=%d hostname=%s pid=%d\n", 
          m_process.c_str(), m_id, hostname, pid);
  fclose(pid_f);
}


hid_t DaqBase::H5Fopen_with_polling(const std::string &fname, unsigned flags, hid_t fapl_id, bool verbose, int max_seconds) {
  const int microseconds_to_wait = 100000;

  int max_microseconds = max_seconds * 1000000;
  int waited_so_far = 0;

  /* Save old error handler */
  H5E_auto2_t old_func;
  void *old_client_data;
  H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
  
  /* Turn off error handling */
  H5Eset_auto2(H5E_DEFAULT, NULL, NULL);

  hid_t h5 = -1;
  while (true) {
    if (waited_so_far > max_microseconds) {
      std::cout << logHdr() << "timeout waiting for " << fname << std::endl;
      H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
      throw std::runtime_error("timeout");
    }
    h5 = H5Fopen(fname.c_str(), flags, fapl_id);
    if (h5 >= 0) {
      if (verbose) {
        std::cout << logHdr() << "found " << fname << std::endl;
      }
      break;
    }
    std::cout << logHdr() << "still waiting for " << fname << std::endl;
    usleep(microseconds_to_wait);
    waited_so_far += microseconds_to_wait;
  }

  /* Restore previous error handler */
  H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
	
  return h5;
}


void DaqBase::create_standard_groups(hid_t parent) {
  m_small_group = NONNEG( H5Gcreate2(parent, "small", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );  
  m_vlen_group = NONNEG( H5Gcreate2(parent, "vlen", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );  
  m_cspad_group = NONNEG( H5Gcreate2(parent, "cspad", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );  
}


void DaqBase::create_number_groups(hid_t parent, TSubMap &sub_map, int first, int count) {
  for (int name = first; name < (first + count); ++name) {
    char strname[128];
    sprintf(strname, "%5.5d", name);
    hid_t dset_group = NONNEG( H5Gcreate2(parent, strname, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );
    sub_map[name]=dset_group;
  } 
}


void DaqBase::close_number_groups(std::map<int, hid_t> &name_to_group) {
  std::map<int, hid_t>::iterator pos;
  for (pos = name_to_group.begin(); pos != name_to_group.end(); ++pos) {
    NONNEG( H5Gclose( pos->second ) );
  }
  name_to_group.clear();
}


void DaqBase::close_standard_groups() {
  NONNEG( H5Gclose( m_cspad_group ) );
  NONNEG( H5Gclose( m_vlen_group ) );
  NONNEG( H5Gclose( m_small_group ) );
}
  

void DaqBase::load_cspad(const std::string &h5_filename,
                         const std::string &dataset,
                         int length,
                         std::vector<short> &cspad_buffer) {
  size_t total = size_t(CSPadNumElem) * size_t(length);
  cspad_buffer.resize(total);
  hid_t fid = POS( H5Fopen(h5_filename.data(), H5F_ACC_RDONLY , H5P_DEFAULT) );
  Dset dset = Dset::open(fid, dataset.data(), Dset::if_vds_first_missing);
  dset.read(0, length, cspad_buffer);
  dset.close();
  std::cout << logHdr() << "loaded cspad" << std::endl;
  NONNEG( H5Fclose( fid ) );                         
}

DaqBase::~DaqBase() {
  FILE *finished_f = fopen(m_fname_finished.c_str(), "w");
  if (NULL==finished_f) {
    std::cerr << logHdr() << "could not create finished file\n" << std::endl;
    return;
  }
  fprintf(finished_f,"done.\n");
  fclose(finished_f);
}
