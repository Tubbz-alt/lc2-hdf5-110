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
  int m_wait_master_seconds_max;
  std::string m_master_fname, m_output_fname;
  hid_t m_master_fid;
  hid_t m_output_fid;

  Dset m_avail_events;

  std::map<std::string, int> m_top_group_2_num_subgroups;

  // map "small" -> 1 if they appear on every shot, etc
  std::map<std::string, int> m_rates;

  // map "small" -> N where N will be in terms of number of events
  std::map<std::string, int> m_events_per_dataset_chunkcache;

  std::vector<int64_t> m_event_data;


protected:
  void wait_for_SWMR_access_to_master();
  void analysis_loop();
  void initialize_dsets();
  void close_dsets();
  void wait_for_event_to_be_available(int64_t event);
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
    m_wait_master_seconds_max(m_config["ana_reader_master"]["wait_master_seconds_max"].as<int>()),
    m_master_fid(-1),
    m_output_fid(-1)
{  
  m_master_fname = DaqBase::form_fullpath("daq_master", 0, HDF5);
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
    std::cout << logHdr() << "created file: " << m_output_fname.c_str() << std::endl;
  }
  NONNEG( H5Pclose(fapl) );

  analysis_loop();

  NONNEG( H5Fclose(m_master_fid) );
  NONNEG( H5Fclose(m_output_fid) );
}


void AnaReaderMaster::wait_for_SWMR_access_to_master() {
  bool verbose = m_config["verbose"].as<int>() > 0;
  m_master_fid = H5Fopen_with_polling(m_master_fname, 
                                      H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, 
                                      H5P_DEFAULT, verbose);
}


void AnaReaderMaster::analysis_loop() {    
  std::vector<int64_t> event_checksums;
  std::vector<int64_t> event_numbers;
  std::vector<int64_t> event_processed_times;

  size_t max_event_data_bytes = 0;
  max_event_data_bytes += sizeof(int64_t) * 
    m_top_group_2_num_subgroups[std::string("small")] * 
    m_group2dsets[std::string("small")].size();
  max_event_data_bytes += sizeof(int64_t) * 
    m_top_group_2_num_subgroups[std::string("vlen")] * 
    m_group2dsets[std::string("vlen")].size();
  max_event_data_bytes += sizeof(int64_t) * m_vlen_max_per_shot * 
    m_top_group_2_num_subgroups[std::string("vlen")]; // blobdata
  max_event_data_bytes += m_num_cspad * sizeof(int64_t) * 
    m_group2dsets[std::string("cspad")].size();
  max_event_data_bytes += sizeof(short) * m_num_cspad * 32l * 185 * 388;
  max_event_data_bytes *= 2; // just to be sure
  size_t max_event_data_count = max_event_data_bytes / sizeof(int64_t);
  m_event_data.resize(max_event_data_count);
  
  initialize_dsets();

  int64_t event_block_start = m_id * m_event_block_size;

  bool verbose2 = m_config["verbose"].as<int>()>=2;
  int64_t report_interval = 50;
  if (verbose2) report_interval = 1;

  int64_t last_report = 0;

  std::cout << logHdr() << " starting loop" << std::endl;
  while (event_block_start < m_num_samples) {
    int64_t first = event_block_start;
    int64_t count = std::min(m_event_block_size, m_num_samples - first);
    event_block_start += (m_num_readers * m_event_block_size);

    for (int64_t event_in_block = 0; event_in_block < count; ++event_in_block) {
      int64_t event = first + event_in_block;
      if ( not (small_writes(event) or vlen_writes(event) or cspad_roundrobin_writes(event))) {
        if (verbose2) {
          std::cout << logHdr() << " no data recorded for event " << event << " skipping" << std::endl; 
        }
        continue;
      }
      wait_for_event_to_be_available(event);
      if (event - last_report >= report_interval) {
        last_report = event;
        std::cout << logHdr() << " starting to process " << event << std::endl;
      }

      event_checksums.push_back(calc_event_checksum(event));
      event_numbers.push_back(event);
      auto milli = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count();
      event_processed_times.push_back(milli);
    }
  }

  close_dsets();

  int rank1=1;
  hsize_t dim=event_numbers.size();

  H5LTmake_dataset(m_output_fid, "/event_checksums", rank1, &dim, H5T_NATIVE_INT64, &event_checksums.at(0));
  H5LTmake_dataset(m_output_fid, "/event_numbers", rank1, &dim, H5T_NATIVE_INT64, &event_numbers.at(0));
  H5LTmake_dataset(m_output_fid, "/event_processed_times", rank1, &dim, H5T_NATIVE_INT64, &event_processed_times.at(0));
}


void AnaReaderMaster::initialize_dsets() {
  char dset_path[512];
  bool verbose1 = m_config["verbose"].as<int>()>=1;
  bool verbose2 = m_config["verbose"].as<int>()>=2;

  for (auto iter = m_group2dsets.begin(); iter != m_group2dsets.end(); ++iter) {
    auto &topGroup = iter->first;  
    auto &dsetNames = iter->second;
    m_topGroups[topGroup] = Number2Dsets();
    size_t num_sub_groups = m_top_group_2_num_subgroups[topGroup];

    if (verbose2) {
      std::cout << logHdr()  << "initialize_dsets: " << topGroup << std::endl;
    }

    for (size_t sub_group=0; sub_group < num_sub_groups; ++sub_group) {
      for (auto dsetIter = dsetNames.begin(); dsetIter != dsetNames.end(); ++dsetIter) {
        auto &dsetName = *dsetIter;
        sprintf(dset_path, "/%s/%5.5ld/%s", topGroup.c_str(), sub_group, dsetName.c_str());
        if (verbose2) {
          std::cout << logHdr()  << "about to open dset: " << dset_path << std::endl;
        }
        m_topGroups[topGroup][sub_group][dsetName] = Dset::open(m_master_fid, dset_path, Dset::if_vds_first_missing);
      }
    }
  }

  m_avail_events = Dset::open(m_master_fid, "avail_events", Dset::if_vds_first_missing);

  if (verbose1) {
    std::cout << logHdr()  << "initialized dsets" << std::endl;
  }
}


void AnaReaderMaster::close_dsets() {
  for (auto iter = m_group2dsets.begin(); iter != m_group2dsets.end(); ++iter) {
    auto &topGroup = iter->first;   // 'small'
    auto &dsetList = iter->second;

    size_t num_sub_groups = m_top_group_2_num_subgroups[topGroup];
    for (size_t sub_group=0; sub_group < num_sub_groups; ++sub_group) {
      for (auto dsetIter = dsetList.begin(); dsetIter != dsetList.end(); ++dsetIter) {
        auto &dsetName = *dsetIter;
        m_topGroups[topGroup][sub_group][dsetName].close();
      }
    }
  }
  m_avail_events.close();
}


int64_t AnaReaderMaster::calc_event_checksum(int64_t event_number) {
  static const std::string fiducials_str("fiducials"), 
    milli_str("milli"), cspad_str("cspad"), 
    data_str("data"), blobdata_str("blobdata"), vlen_str("vlen");
  static bool verbose2 = m_config["verbose"].as<int>()>=2;
  size_t next_idx = 0;
  
  typedef enum {unknown, check_event_number, copy_cspad, copy_vlen_blob, copy_int64_t} Action;
  
  hsize_t count = 1;
  hsize_t bigger_buffer_to_be_safe = 10*count;
  std::vector<int64_t> value(bigger_buffer_to_be_safe);
  
  for (auto topIter = m_top_group_2_num_subgroups.begin();
       topIter != m_top_group_2_num_subgroups.end(); ++topIter) {
    
    std::string topName = topIter->first;
    int64_t event_idx_in_master = get_event_idx_in_master(topName, event_number);
    if (event_idx_in_master == -1) continue;
    
    size_t numSub = topIter->second;
    auto & dsetNameList = m_group2dsets[topName];
    auto &num2dsetNameList = m_topGroups[topName];
    
    for (auto dsetNameIter = dsetNameList.begin(); 
         dsetNameIter != dsetNameList.end(); ++dsetNameIter) {
      
      auto &dsetName = *dsetNameIter;
      
      Action action = copy_int64_t;
      if (dsetName == fiducials_str) {
        action = check_event_number;
      } else if (dsetName == milli_str) {
        continue;
      } else if ((topName == cspad_str) and (dsetName == data_str)) {
        action = copy_cspad;
      } else if  ((topName == vlen_str) and (dsetName == blobdata_str)) {
        action = copy_vlen_blob;
      }

      for (size_t sub = 0; sub < numSub; ++sub) {
        auto &dsetnameList = num2dsetNameList[sub];
        auto &dset = dsetnameList[dsetName];
        dset.wait(event_idx_in_master+1, m_wait_for_dsets_microsecond_pause, 
                  m_wait_for_dsets_timeout, verbose2);
        switch (action) {
        case check_event_number:
          dset.read(event_idx_in_master, count, value, true);
          if (value.at(0) != event_number) {
            std::cerr << "ERROR: check_event_number failure: " << topName 
                      << "/" << sub << "/" << dsetName << "["
                      << event_idx_in_master << "]=" << value.at(0) 
                      << " != event_number=" << event_number << std::endl;
            //            throw std::runtime_error("check_event_number failed");
          }
          break;
        case copy_int64_t:
          dset.read(event_idx_in_master, count, value);
          if (next_idx + 1 >= m_event_data.size()) {
            std::cerr << "ERROR: copy_int64_t: m_event_data too short - it is " 
                      << m_event_data.size() << " but need " 
                      <<  (next_idx + 1) << " len." << std::endl;
            throw std::runtime_error("copy_int64_t failed");
          }
          m_event_data.at(next_idx) = value.at(0);
          next_idx += 1;
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

  size_t event_len_in_int64_ts = next_idx;
  int64_t checksum = 0;
  for (size_t idx = 0; idx < event_len_in_int64_ts; ++idx) {
    checksum += m_event_data.at(idx);
  }

  return checksum;
}


void AnaReaderMaster::wait_for_event_to_be_available(int64_t event) {
  m_avail_events.wait(event+1, m_wait_for_dsets_microsecond_pause, m_wait_for_dsets_timeout, true);
}


AnaReaderMaster::~AnaReaderMaster() {
  std::cout << logHdr() << "done" << std::endl;
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

