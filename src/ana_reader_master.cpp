#include <climits>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <iostream>
#include <numeric>
#include <unistd.h>
#include <stdint.h>

#include "lc2daq.h"
#include "hdf5_hl.h"


struct AnaReaderDsetInfo {
  hid_t dset_id;
  std::vector<hsize_t> dims;
};


std::map<std::string, std::vector<std::string> > get_top_group_to_final_dsets() {
  std::map<std::string, std::vector<std::string> > group2dsets;
  std::vector<std::string> not_vlen, vlen;
  not_vlen.push_back(std::string("fiducials"));
  not_vlen.push_back(std::string("data"));
  not_vlen.push_back(std::string("nano"));
  
  vlen.push_back(std::string("fiducials"));
  vlen.push_back(std::string("blob"));
  vlen.push_back(std::string("blobcount"));
  vlen.push_back(std::string("blobstart"));
  vlen.push_back(std::string("nano"));
  
  group2dsets[std::string("small")] = not_vlen;
  group2dsets[std::string("cspad")] = not_vlen;
  group2dsets[std::string("vlen")] = vlen;

  return group2dsets;
}


class AnaReaderMaster : public DaqBase {
  long m_event_block_size;
  long m_num_cspad;
  long m_small_rate, m_vlen_rate, m_cspad_rate;
  long m_num_samples;
  int m_num_readers;
  int m_num_writers;
  int m_num_small_per_writer;
  int m_num_vlen_per_writer;
  int m_vlen_max_per_shot;
  int m_wait_for_dsets_microsecond_pause;
  int m_wait_for_dsets_timeout;
  std::string m_master_fname, m_output_fname;
  hid_t m_master_fid;
  hid_t m_output_fid;

  // map "fiducials", "data", etc to AnaReaderDsetInfo
  typedef std::map<std::string, AnaReaderDsetInfo> DsetName2Info;

  // map group numbers, i.e, 00000, to above
  typedef std::map<int, DsetName2Info> DsetNumber2GroupInfo;

  // map "small", "vlen", etc, to above
  std::map<std::string, DsetNumber2GroupInfo> m_topGroups;

  // map "small", "vlen", "cspad" to the list of dsets, ie ["fiducials", "blobdata", ...]
  std::map<std::string, std::vector<std::string> > m_group2dsets;

  // map "small" -> 100, "vlen" -> 80, i.e, the total number of subgroups in the master
  std::map<std::string, size_t> m_top_group_2_num_subgroups;

  // map "small" -> 1 if they appear on every shot, etc
  std::map<std::string, int> m_rates;
  
  std::vector<uint8_t> m_event_data;

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

protected:
  void wait_for_SWMR_access_to_master();
  void analysis_loop();
  void initialize_dset_info();
  // return the event that they get to
  long wait_for_all_dsets_to_get_to_at_least(long event_number);
  long calc_event_checksum(long event_number);
  
public:
  AnaReaderMaster(int argc, char *argv[]);
  ~AnaReaderMaster();
  void run();
};


AnaReaderMaster::AnaReaderMaster(int argc, char *argv[])
  : DaqBase(argc, argv, "ana_reader_master"), 
    m_event_block_size(m_config["ana_reader_master"]["event_block_size"].as<long>()),
    m_num_cspad(m_config["daq_writer"]["datasets"]["round_robin"]["cspad"]["num"].as<int>()),
    m_small_rate(m_config["daq_writer"]["datasets"]["single_source"]["small"]["shots_per_sample"].as<long>()),
    m_vlen_rate(m_config["daq_writer"]["datasets"]["single_source"]["vlen"]["shots_per_sample"].as<long>()),
    m_cspad_rate(m_config["daq_writer"]["datasets"]["round_robin"]["cspad"]["shots_per_sample_all_writers"].as<long>()),
    m_num_samples(m_config["num_samples"].as<long>()),
    m_num_readers(m_config["ana_reader_master"]["num"].as<int>()),
    m_num_writers(m_config["daq_writer"]["num"].as<int>()),
    m_num_small_per_writer(m_config["daq_writer"]["datasets"]["single_source"]["small"]["num_per_writer"].as<int>()),
    m_num_vlen_per_writer(m_config["daq_writer"]["datasets"]["single_source"]["vlen"]["num_per_writer"].as<int>()),
    m_vlen_max_per_shot(m_config["daq_writer"]["datasets"]["single_source"]["vlen"]["max_per_shot"].as<int>()),
    m_wait_for_dsets_microsecond_pause(m_config["ana_reader_master"]["wait_for_dsets_microsecond_pause"].as<int>()),
    m_wait_for_dsets_timeout(m_config["ana_reader_master"]["wait_for_dsets_timeout"].as<int>()),
    m_master_fid(-1),
    m_output_fid(-1)
{  
  m_master_fname = DaqBase::form_fullpath("daq_master", m_id, HDF5);
  m_output_fname = DaqBase::form_fullpath("ana_reader_master", m_id, HDF5);

  // will set to "small" -> ["fiducials", "nano", "data"] ...
  m_group2dsets = get_top_group_to_final_dsets();

  m_top_group_2_num_subgroups[std::string("small")] = m_num_small_per_writer * m_num_writers;
  m_top_group_2_num_subgroups[std::string("vlen")] = m_num_vlen_per_writer * m_num_writers;
  m_top_group_2_num_subgroups[std::string("cspad")] = size_t(m_num_cspad);

  m_rates[std::string("small")] = m_small_rate;
  m_rates[std::string("vlen")] = m_vlen_rate;
  m_rates[std::string("cspad")] = m_cspad_rate;  
}


void AnaReaderMaster::run() {
  wait_for_SWMR_access_to_master();

  hid_t fapl = NONNEG( H5Pcreate(H5P_FILE_ACCESS) );
  NONNEG( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
  m_output_fid = NONNEG( H5Fcreate(m_output_fname.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
  if (m_config["verbose"].as<int>()>0) {
    fprintf(stdout, "created file: %s\n", m_output_fname.c_str());
    fflush(::stdout);
  }
  NONNEG( H5Pclose(fapl) );

  analysis_loop();

  NONNEG( H5Fclose(m_master_fid) );
  NONNEG( H5Fclose(m_output_fid) );
}


void AnaReaderMaster::wait_for_SWMR_access_to_master() {
  const int microseconds_to_wait = 100000;
  while (true) {
    FILE *fp = fopen(m_master_fname.c_str(), "r");
    if (NULL != fp) {
      if (m_config["verbose"].as<int>() > 0) fprintf(stdout, "ana_reader_master: found %s\n" , m_master_fname.c_str());
      fclose(fp);
      m_master_fid = POS( H5Fopen(m_master_fname.c_str(), H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, H5P_DEFAULT) );
      return;
    } else {
      if (m_config["verbose"].as<int>() > 0) fprintf(stdout, "ana_reader_master_%d: still waiting for %s\n" , m_id, m_master_fname.c_str());
      usleep(microseconds_to_wait);
    }
  }
}


void AnaReaderMaster::analysis_loop() {    
  std::vector<long> event_checksums;
  std::vector<long> event_numbers;
  std::vector<long> event_processed_times;

  size_t max_event_data_size = 0;
  max_event_data_size += sizeof(long) * 
    m_top_group_2_num_subgroups[std::string("small")] * 
    m_group2dsets[std::string("small")].size();
  max_event_data_size += sizeof(long) * 
    m_top_group_2_num_subgroups[std::string("vlen")] * 
    m_group2dsets[std::string("vlen")].size();
  max_event_data_size += sizeof(long) * m_vlen_max_per_shot * 
    m_top_group_2_num_subgroups[std::string("vlen")]; // blobdata
  max_event_data_size += m_num_cspad * sizeof(long) * 
    m_group2dsets[std::string("cspad")].size();
  max_event_data_size += sizeof(short) * m_num_cspad * 32l * 185 * 388;

  m_event_data.resize(max_event_data_size);
  
  initialize_dset_info();

  long event_block_start = m_id * m_event_block_size;
  long next_available_event = -1;

  while (event_block_start < m_num_samples) {
    long first = event_block_start;
    long count = std::min(m_event_block_size, m_num_samples - first);
    event_block_start += (m_num_readers * m_event_block_size);

    for (long event_in_block = 0; event_in_block < count; ++event_in_block) {
      long event = first + event_in_block;
      if (event > next_available_event) {
        next_available_event = wait_for_all_dsets_to_get_to_at_least(event);
      }
      event_checksums.push_back(calc_event_checksum(event));
      event_numbers.push_back(event);
      m_t1 = Clock::now();
      auto diff = m_t1 - m_t0;
      auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(diff).count();
      event_processed_times.push_back(nanoseconds);
    }
  }

  int rank1=1;
  hsize_t dim=event_numbers.size();

  H5LTmake_dataset(m_output_fid, "/event_checksums", rank1, &dim, H5T_NATIVE_LONG, &event_checksums.at(0));
  H5LTmake_dataset(m_output_fid, "/event_numbers", rank1, &dim, H5T_NATIVE_LONG, &event_numbers.at(0));
  H5LTmake_dataset(m_output_fid, "/event_processed_times", rank1, &dim, H5T_NATIVE_LONG, &event_processed_times.at(0));
}


void AnaReaderMaster::initialize_dset_info() {
  char buffer[512];
  
  for (auto iter = m_group2dsets.begin(); iter != m_group2dsets.end(); ++iter) {
    auto topGroup = iter->first;
    auto dsetList = iter->second;

    m_topGroups[topGroup] = DsetNumber2GroupInfo();
    size_t num_sub_groups = m_top_group_2_num_subgroups[topGroup];

    for (size_t sub_group=0; sub_group < num_sub_groups; ++sub_group) {
      m_topGroups[topGroup][sub_group] = DsetName2Info();
      for (auto dsetIter = dsetList.begin(); dsetIter != dsetList.end(); ++dsetIter) {
        auto dsetName = *dsetIter;
        AnaReaderDsetInfo dsetInfo;
        sprintf(buffer, "/%s/%5.5ld/%s", topGroup.c_str(), sub_group, dsetName.c_str());
        
        dsetInfo.dset_id = NONNEG( H5Dopen2( m_master_fid, buffer, H5P_DEFAULT) );
        hid_t dspace_id = NONNEG( H5Dget_space( dsetInfo.dset_id ) );
        int rank = NONNEG( H5Sget_simple_extent_ndims( dspace_id ) );
        dsetInfo.dims.resize(rank);
        NONNEG( H5Sget_simple_extent_dims( dspace_id, &dsetInfo.dims.at(0), NULL ) );
        NONNEG( H5Sclose( dspace_id ) );
        m_topGroups[topGroup][sub_group][dsetName] = dsetInfo;
      }
    }
  }
}


long AnaReaderMaster::calc_event_checksum(long event_number) {

  uint8_t * next_event_data = &m_event_data.at(0);

  typedef enum {unknown, check_event_number, copy_cspad, copy_vlen_blob, copy_long} Action;

  for (auto topIter = m_top_group_2_num_subgroups.begin();
       topIter != m_top_group_2_num_subgroups.end(); ++topIter) {

    std::string topName = topIter->first;

    if (event_number % m_rates[topName] != 0) continue;

    hsize_t event_index = event_number/m_rates[topName];

    size_t numSub = topIter->second;

    auto dsetNameList = m_group2dsets[topName];

    auto sub2dsetname2info = m_topGroups[topName];

    for (auto dsetNameIter = dsetNameList.begin(); 
         dsetNameIter != dsetNameList.end(); ++dsetNameIter) {

      auto dsetName = *dsetNameIter;

      Action action = copy_long;
      if (dsetName == std::string("fiducials")) {
        action = check_event_number;
      } else if (dsetName == std::string("nano")) {
        continue;
      } else if ((topName == std::string("cspad")) and (dsetName == std::string("data"))) {
        action = copy_cspad;
      } else if  ((topName == std::string("vlen")) and (dsetName == std::string("blobdata"))) {
        action = copy_vlen_blob;
      }

      long value;
      for (size_t sub = 0; sub < numSub; ++ sub) {
        auto dsetname2info = sub2dsetname2info[sub];
        auto info = dsetname2info[dsetName];
          
        switch (action) {
        case check_event_number:
          value = read_long_from_1d(info.dset_id, event_index);
          if (value != event_number) {
            std::cerr << "check_event_number failure: " << topName 
                      << "/" << sub << "/" << dsetName << " value=" << value 
                      << " != event_number=" << event_number << std::endl;
            throw std::runtime_error("check_event_number failed");
          }
          break;
        case copy_long:
          value = read_long_from_1d(info.dset_id, event_index);
          *((long *)next_event_data) = value;
          next_event_data += sizeof(long);
          break;
        case copy_cspad:
          break;
        case copy_vlen_blob:
          break;
        case unknown:
          throw std::runtime_error("unknown action for checksum - internal error");
        }
      }
    }
  }
  return 0;
}


long AnaReaderMaster::wait_for_all_dsets_to_get_to_at_least(long event_number) {
  hsize_t last_avail_event = LLONG_MAX;

  for (auto iter = m_group2dsets.begin(); iter != m_group2dsets.end(); ++iter) {
    auto top_group_name = iter->first;
    auto dset_names_list = iter->second;
    auto num_subgroups = m_top_group_2_num_subgroups[top_group_name];
    auto sub2dsetname2info = m_topGroups[top_group_name];
    for (size_t sub=0; sub < num_subgroups; ++sub) {
      auto dsetname2info = sub2dsetname2info[sub];
      for (auto nameIter = dset_names_list.begin(); nameIter != dset_names_list.end(); ++nameIter) {
        auto dset_name = *nameIter;
        auto dsetInfo = dsetname2info[dset_name];
        if (dsetInfo.dims.at(0) < hsize_t(event_number)) {
          bool success = wait_for_dataset_to_grow(dsetInfo.dset_id,
                                                  &dsetInfo.dims[0],
                                                  event_number+1,
                                                  m_wait_for_dsets_microsecond_pause,
                                                  m_wait_for_dsets_timeout);
          bool timed_out = not success;
          if (timed_out) {
            std::cerr << "Timeout waiting for " << top_group_name 
                      <<  " / " << sub << " / " << dset_name 
                      << " to get to size " << event_number + 1 << std::endl;
            throw std::runtime_error("AnaReaderMaster - timeout");
          }
        }
        last_avail_event = std::min(last_avail_event, dsetInfo.dims[0]);
      }
    }
  }
  return last_avail_event;
}


AnaReaderMaster::~AnaReaderMaster() {
}


int main(int argc, char *argv[]) {
  H5open();
  try {
    AnaReaderMaster anaReaderMaster(argc, argv);
    anaReaderMaster.run();
  } catch (...) {
    H5close();
    throw;
  }
  H5close();
  
  return 0;
}

