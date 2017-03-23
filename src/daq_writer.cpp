#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <map>

#include "daq_base.h"


class DaqWriter : public DaqBase {
  
  hid_t m_writer_fid;
  int m_small_chunksize;
  int m_next_small, m_next_vlen, m_next_cspad;
  int m_small_shot_stride, m_vlen_shot_stride, m_cspad_shot_stride;

  int m_small_first, m_vlen_first, m_cspad_first;
  int m_small_count, m_vlen_count, m_cspad_count;
  
  int m_next_vlen_count;
  int m_vlen_max_per_shot;
  int m_next_cspad_in_source;
  
  std::map<int, hid_t> m_small_id_to_number_group,
    m_vlen_id_to_number_group,
    m_cspad_id_to_number_group;

  std::map<int, DsetWriterInfo> m_small_id_to_fiducials_dset,
    m_vlen_id_to_fiducials_dset,
    m_cspad_id_to_fiducials_dset;

  std::map<int, DsetWriterInfo> m_small_id_to_nano_dset,
    m_vlen_id_to_nano_dset,
    m_cspad_id_to_nano_dset;

  std::map<int, DsetWriterInfo> m_small_id_to_data_dset,
    m_vlen_id_to_blob_dset,
    m_cspad_id_to_data_dset;

  std::map<int, DsetWriterInfo> m_vlen_id_to_blob_start_dset,
    m_vlen_id_to_blob_count_dset;
  
  std::vector<int64_t> m_vlen_data;
  std::vector<int16_t> m_cspad_source;
  
public:
  DaqWriter(int argc, char *argv[]);
  ~DaqWriter();

  void run();
  void create_file();
  void create_all_groups_datasets_and_attributes();
  void close_all_groups_datasets();
  void start_SWMR_access_to_file();
  void write(int64_t fiducial);
  void flush_data(int64_t fiducial);

protected:
  void create_fiducials_dsets(const std::map<int, hid_t> &id_to_number_group, 
                              std::map<int, DsetWriterInfo> &id_to_dset);
  void create_nano_dsets(const std::map<int, hid_t> &, std::map<int, DsetWriterInfo> &);

  void create_small_data_dsets();
  void create_cspad_data_dsets();
  void create_vlen_blob_and_index_dsets();

  void write_small(int64_t fiducial);
  void write_vlen(int64_t fiducial);
  void write_cspad(int64_t fiducial);

  void create_small_dsets_helper(const std::map<int, hid_t> &,
                                 std::map<int, DsetWriterInfo> &,
                                 const char *, int);

  void flush_helper(const std::map<int, DsetWriterInfo> &);

};


DaqWriter::DaqWriter(int argc, char *argv[])
  : DaqBase(argc, argv, "daq_writer"),
    m_writer_fid(-1),
    m_small_chunksize(-1),
    m_next_small(0),
    m_next_vlen(0),
    m_next_cspad(0),

    m_small_shot_stride(0),
    m_vlen_shot_stride(0),
    m_cspad_shot_stride(0),

    m_small_first(0),
    m_vlen_first(0),
    m_cspad_first(0),

    m_small_count(0),
    m_vlen_count(0),
    m_cspad_count(0),

    m_next_vlen_count(0),
    m_vlen_max_per_shot(0),
    m_next_cspad_in_source(0)
{
  YAML::Node cspad_config = m_group_config["datasets"]["round_robin"]["cspad"]["source"];
  std::string h5_filename = cspad_config["filename"].as<std::string>();
  std::string h5_dataset = cspad_config["dataset"].as<std::string>();
  int number_cspad_in_source = cspad_config["length"].as<int>();
  DaqBase::load_cspad(h5_filename, h5_dataset, number_cspad_in_source, m_cspad_source);
  m_small_chunksize = m_group_config["datasets"]["single_source"]["small"]["chunksize"].as<int>();
  m_small_shot_stride = m_group_config["datasets"]["single_source"]["small"]["shots_per_sample"].as<int>();
  m_vlen_shot_stride = m_group_config["datasets"]["single_source"]["vlen"]["shots_per_sample"].as<int>();
  
  int cspad_shots_per_sample_all_writers = m_group_config["datasets"]["round_robin"]["cspad"]["shots_per_sample_all_writers"].as<int>();

  m_cspad_shot_stride = cspad_shots_per_sample_all_writers * m_group_config["num"].as<int>();

  m_small_count = m_group_config["datasets"]["single_source"]["small"]["num_per_writer"].as<int>();
  m_small_first = m_id * m_small_count;
    
  m_vlen_count = m_group_config["datasets"]["single_source"]["vlen"]["num_per_writer"].as<int>();
  m_vlen_first = m_id * m_vlen_count;
  m_vlen_max_per_shot = m_group_config["datasets"]["single_source"]["vlen"]["max_per_shot"].as<int>();
  m_cspad_first = 0;
  m_cspad_count = m_group_config["datasets"]["round_robin"]["cspad"]["num"].as<int>();

  m_vlen_data.resize(m_vlen_max_per_shot);
}


DaqWriter::~DaqWriter() {}

void DaqWriter::run() {
  DaqBase::run_setup();
  create_file();
  create_all_groups_datasets_and_attributes();
  start_SWMR_access_to_file();
  int64_t fiducial = -1;
  for (fiducial = 0; fiducial < m_config["num_samples"].as<int64_t>(); ++fiducial) {
    write(fiducial);
    if ((fiducial > 0) and (0 == (fiducial % m_config["flush_interval"].as<int64_t>()))) {
      flush_data(fiducial);
    }
  }
  if (m_config["writers_hang"].as<bool>()) {
    printf("MSG: hanging\n");
    fflush(::stdout);
    while (true) {}
  }
  close_all_groups_datasets();
  NONNEG( H5Fclose(m_writer_fid) );
  m_t1 = Clock::now();

  auto total_diff = m_t1 - m_t0;
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(total_diff);
  std::cout << "num seconds=" << seconds.count() << " num events=" << fiducial << std::endl;  
}


void DaqWriter::create_file() {
  hid_t fapl = NONNEG( H5Pcreate(H5P_FILE_ACCESS) );
  NONNEG( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
  m_writer_fid = NONNEG( H5Fcreate(m_fname_h5.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
  if (m_config["verbose"].as<int>() > 0) {
    printf("created file: %s\n", m_fname_h5.c_str());
    fflush(::stdout);
  }
  NONNEG( H5Pclose(fapl) );
};


void DaqWriter::close_all_groups_datasets() {
  // need to implement? or is file close Ok?
}


void DaqWriter::create_all_groups_datasets_and_attributes() {
  DaqBase::create_standard_groups(m_writer_fid);

  DaqBase::create_number_groups(m_small_group, m_small_id_to_number_group,
                                m_small_first, m_small_count);
  DaqBase::create_number_groups(m_vlen_group, m_vlen_id_to_number_group, 
                                m_vlen_first, m_vlen_count);
  DaqBase::create_number_groups(m_cspad_group, m_cspad_id_to_number_group, 
                                m_cspad_first, m_cspad_count);

  create_fiducials_dsets(m_small_id_to_number_group, m_small_id_to_fiducials_dset);
  create_fiducials_dsets(m_vlen_id_to_number_group, m_vlen_id_to_fiducials_dset);
  create_fiducials_dsets(m_cspad_id_to_number_group, m_cspad_id_to_fiducials_dset);

  create_nano_dsets(m_small_id_to_number_group, m_small_id_to_nano_dset);
  create_nano_dsets(m_vlen_id_to_number_group, m_vlen_id_to_nano_dset);
  create_nano_dsets(m_cspad_id_to_number_group, m_cspad_id_to_nano_dset);

  create_small_data_dsets();
  create_cspad_data_dsets();
  create_vlen_blob_and_index_dsets();
  
  if (m_config["verbose"].as<int>() > 0) {
    printf("created all groups and datasets: %s\n", m_fname_h5.c_str());
    fflush(::stdout);
  }
};


void DaqWriter::create_small_dsets_helper(const std::map<int, hid_t> &id_to_parent,
                                          std::map<int, DsetWriterInfo> &id_to_dset,
                                          const char *dset_name,
                                          int chunksize)
{
  for (CMapIter iter = id_to_parent.begin(); iter != id_to_parent.end(); ++iter) {
    int group_id = iter->first;
    hid_t h5_group = iter->second;
    if (id_to_dset.find(group_id) != id_to_dset.end()) {
      throw std::runtime_error("create_small_dsets_helper, id already in map");
    }
    DsetWriterInfo info =  create_1d_int64_dataset(h5_group, dset_name, chunksize);
    id_to_dset[group_id] = info;
  }
}


void DaqWriter::create_fiducials_dsets(const std::map<int, hid_t> &id_to_number_group, 
                                       std::map<int, DsetWriterInfo> &id_to_dset) {
  create_small_dsets_helper(id_to_number_group, id_to_dset,
                            "fiducials", m_small_chunksize);
}
  

void DaqWriter::create_nano_dsets(const std::map<int, hid_t> &id_to_number_group, std::map<int, DsetWriterInfo> &id_to_dset) {
  create_small_dsets_helper(id_to_number_group, id_to_dset,
                            "nano", m_small_chunksize);
}
  

void DaqWriter::create_small_data_dsets() {
  create_small_dsets_helper(m_small_id_to_number_group, m_small_id_to_data_dset,
                            "data", m_small_chunksize);
}
  

void DaqWriter::create_cspad_data_dsets() {
  for (CMapIter iter = m_cspad_id_to_number_group.begin(); iter != m_cspad_id_to_number_group.end(); ++iter) {
    int group_id = iter->first;
    hid_t h5_group = iter->second;
    if (m_cspad_id_to_data_dset.find(group_id) != m_cspad_id_to_data_dset.end()) {
      throw std::runtime_error("create_cspad_data_dsets, id already in map");
    }

    int chunksize = m_group_config["datasets"]["round_robin"]["cspad"]["chunksize"].as<int>();
    DsetWriterInfo info = create_4d_int16_dataset(h5_group, "data",
                                                                CSPadDim1,
                                                                CSPadDim2,
                                                                CSPadDim3, 
                                                                chunksize);
    m_cspad_id_to_data_dset[group_id] = info;
  }
}


void DaqWriter::create_vlen_blob_and_index_dsets() {
  create_small_dsets_helper(m_vlen_id_to_number_group, m_vlen_id_to_blob_dset,
                            "blob", m_small_chunksize);
  create_small_dsets_helper(m_vlen_id_to_number_group, m_vlen_id_to_blob_start_dset,
                            "blobstart", m_small_chunksize);
  create_small_dsets_helper(m_vlen_id_to_number_group, m_vlen_id_to_blob_count_dset,
                            "blobcount", m_small_chunksize);
}
    

void DaqWriter::start_SWMR_access_to_file() {
  NONNEG( H5Fstart_swmr_write(m_writer_fid) );
  if (m_config["verbose"].as<int>() > 0) {
    printf("started SWMR access to writer file\n");
    fflush(::stdout);
  }
};


void DaqWriter::write(int64_t fiducial) {
  if (m_config["verbose"].as<int>()>= 2) {
    printf("entering write(%ld)\n", fiducial);
    fflush(::stdout);
  }
  write_small(fiducial);
  write_vlen(fiducial);
  write_cspad(fiducial);
}


void DaqWriter::write_small(int64_t fiducial) {
  if (fiducial == m_next_small) {
    m_next_small += std::max(1, m_small_shot_stride);
    auto small_time = Clock::now();
    auto diff = small_time - m_t0;
    auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(diff);
    for (int small_id = m_small_first;
         small_id < m_small_first + m_small_count;
         ++small_id)
      {
        DsetWriterInfo & fid_dset = m_small_id_to_fiducials_dset[small_id];
        DsetWriterInfo & nano_dset = m_small_id_to_nano_dset[small_id];
        DsetWriterInfo & data_dset = m_small_id_to_data_dset[small_id];

        ::append_to_1d_int64_dset(fid_dset, fiducial);
        ::append_to_1d_int64_dset(nano_dset, nano.count());
        ::append_to_1d_int64_dset(data_dset, fiducial);        
      }
  }
}


void DaqWriter::write_vlen(int64_t fiducial) {
  if (fiducial == m_next_vlen) {
    m_next_vlen += std::max(1, m_vlen_shot_stride);
    auto vlen_time = Clock::now();
    auto diff = vlen_time - m_t0;
    auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(diff);

    YAML::Node vlen = m_group_config["datasets"]["single_source"]["vlen"];
    m_next_vlen_count += 1;
    m_next_vlen_count %= vlen["max_per_shot"].as<int>();
    m_next_vlen_count = std::max(vlen["min_per_shot"].as<int>(), m_next_vlen_count);
    for (size_t idx = 0; idx < unsigned(m_next_vlen_count); ++idx) m_vlen_data.at(idx)=fiducial;
    
    for (int vlen_id = m_vlen_first;
         vlen_id < m_vlen_first + m_vlen_count;
         ++vlen_id)
      {
        DsetWriterInfo & fid_dset = m_vlen_id_to_fiducials_dset[vlen_id];
        DsetWriterInfo & nano_dset = m_vlen_id_to_nano_dset[vlen_id];
        DsetWriterInfo & blobdata_dset = m_vlen_id_to_blob_dset[vlen_id];
        DsetWriterInfo & blobstart_dset = m_vlen_id_to_blob_start_dset[vlen_id];
        DsetWriterInfo & blobcount_dset = m_vlen_id_to_blob_count_dset[vlen_id];

        std::cout << "vlen " << vlen_id << " fiducial=" << fiducial
                  << " next_vlen_count=" << m_next_vlen_count
                  << std::endl;
        append_to_1d_int64_dset(fid_dset, fiducial);
        append_to_1d_int64_dset(nano_dset, nano.count());
        int64_t start_idx = blobdata_dset.dim().at(0);
        append_many_to_1d_int64_dset(blobdata_dset, m_next_vlen_count, &m_vlen_data[0]);
        append_to_1d_int64_dset(blobstart_dset, start_idx);
        append_to_1d_int64_dset(blobcount_dset, m_next_vlen_count);
      }
  }
}


void DaqWriter::write_cspad(int64_t fiducial) {
  if (fiducial != m_next_cspad) return;

  m_next_cspad += std::max(1, m_cspad_shot_stride);
  m_next_cspad_in_source += 1;
  
  if (size_t(m_next_cspad_in_source) >= m_cspad_source.size()/size_t(CSPadNumElem)) {
    m_next_cspad_in_source = 0;
  }
    
  auto cspad_time = Clock::now();
  auto diff = cspad_time - m_t0;
  auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(diff);
      
  for (int cspad_id = m_cspad_first;
       cspad_id < m_cspad_first + m_cspad_count;
       ++cspad_id) {

      DsetWriterInfo & fid_dset = m_cspad_id_to_fiducials_dset[cspad_id];
      DsetWriterInfo & nano_dset = m_cspad_id_to_nano_dset[cspad_id];
      DsetWriterInfo & data_dset = m_cspad_id_to_data_dset[cspad_id];
      
      size_t to_write_start = size_t(CSPadNumElem) * size_t(m_next_cspad_in_source);
      if (to_write_start + CSPadNumElem > m_cspad_source.size()) {
        fprintf(stderr, "Trying to write fiducial=%ld cspad_source=%d, but internal error\n",
                fiducial, m_next_cspad_in_source);
        throw std::runtime_error("write_cspad");
      }
      int16_t *data = & m_cspad_source.at(to_write_start);

      append_to_1d_int64_dset(fid_dset, fiducial);
      append_to_1d_int64_dset(nano_dset, nano.count());
      append_to_4d_int16_dset(data_dset, data);
  }  
};


void DaqWriter::flush_helper(const std::map<int, DsetWriterInfo> &id_to_dset) {
  typedef std::map<int, DsetWriterInfo>::const_iterator Iter;
  for (Iter iter = id_to_dset.begin(); iter != id_to_dset.end(); ++iter) {
    const DsetWriterInfo &dsetInfo = iter->second;
    NONNEG( H5Dflush(dsetInfo.dset_id()) );
  }
}


void DaqWriter::flush_data(int64_t fiducial) {
  if (m_config["verbose"].as<int>() > 0 ) {
    printf("flush_data: fiducial=%ld\n", fiducial);
    fflush(::stdout);
  }
  flush_helper(m_small_id_to_fiducials_dset);
  flush_helper(m_small_id_to_nano_dset);
  flush_helper(m_small_id_to_data_dset);

  flush_helper(m_vlen_id_to_fiducials_dset);
  flush_helper(m_vlen_id_to_nano_dset);
  flush_helper(m_vlen_id_to_blob_dset);
  flush_helper(m_vlen_id_to_blob_count_dset);
  flush_helper(m_vlen_id_to_blob_start_dset);

  flush_helper(m_cspad_id_to_fiducials_dset);
  flush_helper(m_cspad_id_to_nano_dset);
  flush_helper(m_cspad_id_to_data_dset);
};


int main(int argc, char *argv[]) {
  H5open();
  try {
    DaqWriter daqWriter(argc, argv);
    daqWriter.run();
  } catch (...) {
    H5close();
    throw;
  }
  H5close();
  return 0;
}
