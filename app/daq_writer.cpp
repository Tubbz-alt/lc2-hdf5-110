#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <map>

#include "lc2daq.h"
#include "DaqBase.h"

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

  std::map<int, Dset> m_small_id_to_fiducials_dset,
    m_vlen_id_to_fiducials_dset,
    m_cspad_id_to_fiducials_dset;

  std::map<int, Dset> m_small_id_to_nano_dset,
    m_vlen_id_to_nano_dset,
    m_cspad_id_to_nano_dset;

  std::map<int, Dset> m_small_id_to_data_dset,
    m_vlen_id_to_blob_dset,
    m_cspad_id_to_data_dset;

  std::map<int, Dset> m_vlen_id_to_blob_start_dset,
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
                              std::map<int, Dset> &id_to_dset);
  void create_nano_dsets(const std::map<int, hid_t> &, std::map<int, Dset> &);

  void create_small_data_dsets();
  void create_cspad_data_dsets();
  void create_vlen_blob_and_index_dsets();

  void write_small(int64_t fiducial);
  void write_vlen(int64_t fiducial);
  void write_cspad(int64_t fiducial);

  void create_small_dsets_helper(const std::map<int, hid_t> &,
                                 std::map<int, Dset> &,
                                 const char *, int);

  void flush_helper(const std::map<int, Dset> &);

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
  YAML::Node cspad_config = m_process_config["datasets"]["round_robin"]["cspad"]["source"];
  std::string h5_filename = cspad_config["filename"].as<std::string>();
  std::string h5_dataset = cspad_config["dataset"].as<std::string>();
  int number_cspad_in_source = cspad_config["length"].as<int>();
  DaqBase::load_cspad(h5_filename, h5_dataset, number_cspad_in_source, m_cspad_source);
  m_small_chunksize = m_process_config["datasets"]["single_source"]["small"]["chunksize"].as<int>();
  m_small_shot_stride = m_process_config["datasets"]["single_source"]["small"]["shots_per_sample"].as<int>();
  m_vlen_shot_stride = m_process_config["datasets"]["single_source"]["vlen"]["shots_per_sample"].as<int>();
  
  int cspad_shots_per_sample_all_writers = m_process_config["datasets"]["round_robin"]["cspad"]["shots_per_sample_all_writers"].as<int>();

  m_cspad_shot_stride = cspad_shots_per_sample_all_writers * m_process_config["num"].as<int>();

  m_small_count = m_process_config["datasets"]["single_source"]["small"]["num_per_writer"].as<int>();
  m_small_first = m_id * m_small_count;
    
  m_vlen_count = m_process_config["datasets"]["single_source"]["vlen"]["num_per_writer"].as<int>();
  m_vlen_first = m_id * m_vlen_count;
  m_vlen_max_per_shot = m_process_config["datasets"]["single_source"]["vlen"]["max_per_shot"].as<int>();
  m_cspad_first = 0;
  m_cspad_count = m_process_config["datasets"]["round_robin"]["cspad"]["num"].as<int>();

  // cspad is round robin, everyone start on their id
  m_next_cspad = m_id;
  
  m_vlen_data.resize(m_vlen_max_per_shot);
}


DaqWriter::~DaqWriter() {}

void DaqWriter::run() {
  DaqBase::run_setup();
  create_file();
  create_all_groups_datasets_and_attributes();
  start_SWMR_access_to_file();
  int64_t fiducial = -1;
  fprintf(stdout, "daq_writer: about to loop through %ld fiducials\n", m_config["num_samples"].as<int64_t>());
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
  printf("close all groups datasets\n");
  fflush(::stdout);
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
                                          std::map<int, Dset> &id_to_dset,
                                          const char *dset_name,
                                          int chunksize)
{
  std::vector<hsize_t> chunk_dims(1);
  chunk_dims.at(0)=chunksize;
  for (auto iter = id_to_parent.begin(); iter != id_to_parent.end(); ++iter) {
    int group_id = iter->first;
    hid_t h5_group = iter->second;
    if (id_to_dset.find(group_id) != id_to_dset.end()) {
      throw std::runtime_error("create_small_dsets_helper, id already in map");
    }
    Dset dset =  Dset::create(h5_group, dset_name, H5T_NATIVE_INT64, chunk_dims);
    id_to_dset[group_id] = dset;
  }
}


void DaqWriter::create_fiducials_dsets(const std::map<int, hid_t> &id_to_number_group, 
                                       std::map<int, Dset> &id_to_dset) {
  create_small_dsets_helper(id_to_number_group, id_to_dset,
                            "fiducials", m_small_chunksize);
}
  

void DaqWriter::create_nano_dsets(const std::map<int, hid_t> &id_to_number_group, std::map<int, Dset> &id_to_dset) {
  create_small_dsets_helper(id_to_number_group, id_to_dset,
                            "nano", m_small_chunksize);
}
  

void DaqWriter::create_small_data_dsets() {
  create_small_dsets_helper(m_small_id_to_number_group, m_small_id_to_data_dset,
                            "data", m_small_chunksize);
}
  

void DaqWriter::create_cspad_data_dsets() {
  std::vector<hsize_t> chunk(4);
  chunk.at(0) = m_process_config["datasets"]["round_robin"]["cspad"]["chunksize"].as<int>();
  chunk.at(1) = CSPadDim1;
  chunk.at(2) = CSPadDim2;
  chunk.at(3) = CSPadDim3;
  for (auto iter = m_cspad_id_to_number_group.begin(); 
       iter != m_cspad_id_to_number_group.end(); ++iter) {
    int group_id = iter->first;
    hid_t h5_group = iter->second;
    if (m_cspad_id_to_data_dset.find(group_id) != m_cspad_id_to_data_dset.end()) {
      throw std::runtime_error("create_cspad_data_dsets, id already in map");
    }
    
    Dset info = Dset::create(h5_group, "data", H5T_NATIVE_INT16, chunk);
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
    printf("daq_writer: entering write(%ld)\n", fiducial);
    fflush(::stdout);
  }
  write_small(fiducial);
  write_vlen(fiducial);
  write_cspad(fiducial);
}


void DaqWriter::write_small(int64_t fiducial) {
  const hsize_t start = 0;
  const hsize_t count = 1;
  std::vector<int64_t> fid_data(count), nano_data(count);
  fid_data.at(0)=fiducial;

  if (m_config["verbose"].as<int>()>= 2) {
    printf("  smal(%ld)\n", fiducial);
    fflush(::stdout);
  }
  if (fiducial == m_next_small) {
    m_next_small += std::max(1, m_small_shot_stride);
    auto small_time = Clock::now();
    auto diff = small_time - m_t0;
    auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(diff);
    nano_data[0]=nano.count();
    for (int small_id = m_small_first;
         small_id < m_small_first + m_small_count;
         ++small_id)
      {
        Dset & fid_dset = m_small_id_to_fiducials_dset[small_id];
        Dset & nano_dset = m_small_id_to_nano_dset[small_id];
        Dset & data_dset = m_small_id_to_data_dset[small_id];

        fid_dset.append(start, count, fid_data);
        nano_dset.append(start, count, nano_data);
        data_dset.append(start, count, fid_data);        
      }
  }
}


void DaqWriter::write_vlen(int64_t fiducial) {
  const hsize_t start = 0;
  const hsize_t count = 1;
  std::vector<int64_t> fid_data(count), nano_data(count), blob_start_data(1), blob_count_data(1);
  fid_data.at(0)=fiducial;

  if (m_config["verbose"].as<int>()>= 2) {
    printf("  vlen(%ld)\n", fiducial);
    fflush(::stdout);
  }

  if (fiducial == m_next_vlen) {
    m_next_vlen += std::max(1, m_vlen_shot_stride);
    auto vlen_time = Clock::now();
    auto diff = vlen_time - m_t0;
    auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(diff);
    nano_data[0]=nano.count();

    YAML::Node vlen = m_process_config["datasets"]["single_source"]["vlen"];
    m_next_vlen_count += 1;
    m_next_vlen_count %= vlen["max_per_shot"].as<int>();
    m_next_vlen_count = std::max(vlen["min_per_shot"].as<int>(), m_next_vlen_count);
    for (size_t idx = 0; idx < unsigned(m_next_vlen_count); ++idx) m_vlen_data.at(idx)=fiducial;
    
    for (int vlen_id = m_vlen_first;
         vlen_id < m_vlen_first + m_vlen_count;
         ++vlen_id)
      {
        Dset & fid_dset = m_vlen_id_to_fiducials_dset[vlen_id];
        Dset & nano_dset = m_vlen_id_to_nano_dset[vlen_id];
        Dset & blobdata_dset = m_vlen_id_to_blob_dset[vlen_id];
        Dset & blobstart_dset = m_vlen_id_to_blob_start_dset[vlen_id];
        Dset & blobcount_dset = m_vlen_id_to_blob_count_dset[vlen_id];

        if (m_config["verbose"].as<int>()>=2) {
            std::cout << "vlen " << vlen_id << " fiducial=" << fiducial
                      << " next_vlen_count=" << m_next_vlen_count
                      << std::endl;
        }
        fid_dset.append(start, count, fid_data);
        nano_dset.append(start, count, nano_data);
        blob_start_data[0] = blobdata_dset.dim().at(0);
        blob_count_data[0] = m_next_vlen_count;
        blobstart_dset.append(start, count, blob_start_data);
        blobcount_dset.append(start, count, blob_count_data);
        blobdata_dset.append(start, m_next_vlen_count, m_vlen_data);
      }
  }
}


void DaqWriter::write_cspad(int64_t fiducial) {
  if (fiducial != m_next_cspad) return;

  if (m_config["verbose"].as<int>()>= 2) {
    printf("  cspad(%ld)\n", fiducial);
    fflush(::stdout);
  }

  m_next_cspad += std::max(1, m_cspad_shot_stride);
  m_next_cspad_in_source += 1;
  
  if (size_t(m_next_cspad_in_source) >= m_cspad_source.size()/size_t(CSPadNumElem)) {
    m_next_cspad_in_source = 0;
  }
    
  auto cspad_time = Clock::now();
  auto diff = cspad_time - m_t0;
  auto nano = std::chrono::duration_cast<std::chrono::nanoseconds>(diff);
      
  const hsize_t count = 1;
  std::vector<int64_t> fid_data(count), nano_data(count);
  fid_data.at(0)=fiducial;
  nano_data[0]=nano.count();

  for (int cspad_id = m_cspad_first;
       cspad_id < m_cspad_first + m_cspad_count;
       ++cspad_id) {

      Dset & fid_dset = m_cspad_id_to_fiducials_dset[cspad_id];
      Dset & nano_dset = m_cspad_id_to_nano_dset[cspad_id];
      Dset & data_dset = m_cspad_id_to_data_dset[cspad_id];
      
      size_t cspad_start = size_t(CSPadNumElem) * size_t(m_next_cspad_in_source);
      const hsize_t start=0;
      fid_dset.append(start, count, fid_data);
      nano_dset.append(start, count, nano_data);
      data_dset.append(cspad_start, count, m_cspad_source);
  }  
};


void DaqWriter::flush_helper(const std::map<int, Dset> &id_to_dset) {
  typedef std::map<int, Dset>::const_iterator Iter;
  for (Iter iter = id_to_dset.begin(); iter != id_to_dset.end(); ++iter) {
    const Dset &dset = iter->second;
    NONNEG( H5Dflush(dset.id()) );
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
  } catch (const std::exception &ex) {
    std::cout << "Caught exception: " << ex.what() << std::endl;
    std::cout << "trying to close library " << std::endl;
    H5close();
    throw ex;
  }
  H5close();
  return 0;
}
