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
#include "DaqBase.h"
#include "hdf5_hl.h"


class AnaReaderMaster : public DaqBase {
  int64_t m_event_block_size;
  int64_t m_num_cspad;
  int64_t m_small_rate, m_vlen_rate, m_cspad_rate;
  int64_t m_num_samples;
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


  std::map<std::string, int> m_top_group_2_num_subgroups;

  // map "small" -> 1 if they appear on every shot, etc
  std::map<std::string, int> m_rates;

  // map "small" -> N where N will be in terms of number of events
  std::map<std::string, int> m_events_per_dataset_chunkcache;

  std::vector<uint8_t> m_event_data;


protected:
  void wait_for_SWMR_access_to_master();
  void analysis_loop();
  void initialize_dset_info();
  void close_dset_info();
  // return the event that they get to
  int64_t wait_for_all_dsets_to_get_to_at_least(int64_t event_number);
  int64_t calc_event_checksum(int64_t event_number);
  
public:
  AnaReaderMaster(int argc, char *argv[]);
  ~AnaReaderMaster();
  void run();
};


AnaReaderMaster::AnaReaderMaster(int argc, char *argv[])
  : DaqBase(argc, argv, "ana_reader_master"), 
    m_event_block_size(m_config["ana_reader_master"]["event_block_size"].as<int64_t>()),
    m_num_cspad(m_config["daq_writer"]["datasets"]["round_robin"]["cspad"]["num"].as<int>()),
    m_small_rate(m_config["daq_writer"]["datasets"]["single_source"]["small"]["shots_per_sample"].as<int64_t>()),
    m_vlen_rate(m_config["daq_writer"]["datasets"]["single_source"]["vlen"]["shots_per_sample"].as<int64_t>()),
    m_cspad_rate(m_config["daq_writer"]["datasets"]["round_robin"]["cspad"]["shots_per_sample_all_writers"].as<int64_t>()),
    m_num_samples(m_config["num_samples"].as<int64_t>()),
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

  m_top_group_2_num_subgroups[std::string("small")] = m_num_small_per_writer * m_num_writers;
  m_top_group_2_num_subgroups[std::string("vlen")] = m_num_vlen_per_writer * m_num_writers;
  m_top_group_2_num_subgroups[std::string("cspad")] = size_t(m_num_cspad);

  m_rates[std::string("small")] = m_small_rate;
  m_rates[std::string("vlen")] = m_vlen_rate;
  m_rates[std::string("cspad")] = m_cspad_rate;  

  int num_writer_chunks_per_dataset_chunk_cache = m_config["ana_reader_master"]["num_writer_chunks_per_dataset_chunk_cache"].as<int>();
  int small_chunksize = m_config["daq_writer"]["datasets"]["single_source"]["small"]["chunksize"].as<int>();
  int vlen_chunksize = m_config["daq_writer"]["datasets"]["single_source"]["vlen"]["chunksize"].as<int>();
  int cspad_chunksize = m_config["daq_writer"]["datasets"]["round_robin"]["cspad"]["chunksize"].as<int>();
  
  m_events_per_dataset_chunkcache[std::string("small")] = num_writer_chunks_per_dataset_chunk_cache * small_chunksize;
  m_events_per_dataset_chunkcache[std::string("vlen")] = num_writer_chunks_per_dataset_chunk_cache * vlen_chunksize;
  m_events_per_dataset_chunkcache[std::string("cspad")] = num_writer_chunks_per_dataset_chunk_cache * cspad_chunksize;
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
  std::vector<int64_t> event_checksums;
  std::vector<int64_t> event_numbers;
  std::vector<int64_t> event_processed_times;

  size_t max_event_data_size = 0;
  max_event_data_size += sizeof(int64_t) * 
    m_top_group_2_num_subgroups[std::string("small")] * 
    m_group2dsets[std::string("small")].size();
  max_event_data_size += sizeof(int64_t) * 
    m_top_group_2_num_subgroups[std::string("vlen")] * 
    m_group2dsets[std::string("vlen")].size();
  max_event_data_size += sizeof(int64_t) * m_vlen_max_per_shot * 
    m_top_group_2_num_subgroups[std::string("vlen")]; // blobdata
  max_event_data_size += m_num_cspad * sizeof(int64_t) * 
    m_group2dsets[std::string("cspad")].size();
  max_event_data_size += sizeof(short) * m_num_cspad * 32l * 185 * 388;

  m_event_data.resize(max_event_data_size);
  
  initialize_dset_info();

  int64_t event_block_start = m_id * m_event_block_size;
  int64_t next_available_event = -1;

  while (event_block_start < m_num_samples) {
    int64_t first = event_block_start;
    int64_t count = std::min(m_event_block_size, m_num_samples - first);
    event_block_start += (m_num_readers * m_event_block_size);

    for (int64_t event_in_block = 0; event_in_block < count; ++event_in_block) {
      int64_t event = first + event_in_block;
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

  close_dset_info();

  int rank1=1;
  hsize_t dim=event_numbers.size();

  H5LTmake_dataset(m_output_fid, "/event_checksums", rank1, &dim, H5T_NATIVE_INT64, &event_checksums.at(0));
  H5LTmake_dataset(m_output_fid, "/event_numbers", rank1, &dim, H5T_NATIVE_INT64, &event_numbers.at(0));
  H5LTmake_dataset(m_output_fid, "/event_processed_times", rank1, &dim, H5T_NATIVE_INT64, &event_processed_times.at(0));
}


void AnaReaderMaster::initialize_dset_info() {
  char dset_path[512];
  // HERE XXXXXXXXXXX
  
  for (auto iter = m_group2dsets.begin(); iter != m_group2dsets.end(); ++iter) {
    auto topGroup = iter->first;   // 'small'
    auto dsetList = iter->second;
    int num_events_in_dataset_chunk_cache = m_events_per_dataset_chunkcache[topGroup];

    m_topGroups[topGroup] = Number2Dsets();
    size_t num_sub_groups = m_top_group_2_num_subgroups[topGroup];

    for (size_t sub_group=0; sub_group < num_sub_groups; ++sub_group) {
      for (auto dsetIter = dsetList.begin(); dsetIter != dsetList.end(); ++dsetIter) {
        auto dsetName = *dsetIter;
        sprintf(dset_path, "/%s/%5.5ld/%s", topGroup.c_str(), sub_group, dsetName.c_str());
        
        DsetReaderInfo dsetInfo = createReaderDsetInfo(m_master_fid, 
                                                       dset_path,
                                                       num_events_in_dataset_chunk_cache);
        m_topGroups[topGroup][sub_group][dsetName] = dsetInfo;
      }
    }
  }
}


void AnaReaderMaster::close_dset_info() {
  for (auto iter = m_group2dsets.begin(); iter != m_group2dsets.end(); ++iter) {
    auto topGroup = iter->first;
    auto dsetList = iter->second;

    m_topGroups[topGroup] = DsetNumber2GroupInfo();
    size_t num_sub_groups = m_top_group_2_num_subgroups[topGroup];

    for (size_t sub_group=0; sub_group < num_sub_groups; ++sub_group) {
      m_topGroups[topGroup][sub_group] = DsetName2Info();
      for (auto dsetIter = dsetList.begin(); dsetIter != dsetList.end(); ++dsetIter) {
        auto dsetName = *dsetIter;
        auto dsetInfo = m_topGroups[topGroup][sub_group][dsetName];
        dsetInfo.close();
        m_topGroups[topGroup][sub_group].erase(dsetName);
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

      int64_t value;
      for (size_t sub = 0; sub < numSub; ++ sub) {
        auto dsetname2info = sub2dsetname2info[sub];
        auto info = dsetname2info[dsetName];
          
        switch (action) {
        case check_event_number:
          value = read_int64_from_1d(info, event_index);
          if (value != event_number) {
            std::cerr << "check_event_number failure: " << topName 
                      << "/" << sub << "/" << dsetName << " value=" << value 
                      << " != event_number=" << event_number << std::endl;
            throw std::runtime_error("check_event_number failed");
          }
          break;
        case copy_long:
          value = read_int64_from_1d(info, event_index);
          *((int64_t *)next_event_data) = value;
          next_event_data += sizeof(int64_t);
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
        if (dsetInfo.dim().at(0) < hsize_t(event_number)) {
          bool success = wait_for_dataset_to_grow(dsetInfo.dset_id(),
                                                  &dsetInfo.dim()[0],
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
        last_avail_event = std::min(last_avail_event, dsetInfo.dim()[0]);
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

