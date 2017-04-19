#include <cstdio>
#include <cstdlib>
#include <vector>
#include <string>
#include <algorithm>
#include <stdexcept>

#include "hdf5.h"
#include "hdf5_hl.h"

#include "params.h"

namespace  {
hid_t H5Fopen_with_polling(const char *fname, unsigned flags, hid_t fapl_id) {
  const int one_second_in_milliseconds = 1000000;
  int waited_so_far = 0;

  /* Save old error handler */
  H5E_auto2_t old_func;
  void *old_client_data;
  H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
  
  /* Turn off error handling */
  H5Eset_auto2(H5E_DEFAULT, NULL, NULL);

  hid_t h5 = -1;
  while (true) {
    if (waited_so_far > max_seconds_reader_waits_for_master * one_second_in_milliseconds) {
      std::cout << "timeout waiting for " << fname << std::endl;
      H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
      throw std::runtime_error("timeout");
    }
    h5 = H5Fopen(fname, flags, fapl_id);
    if (h5 >= 0) {
      break;
    }
    usleep(reader_microseconds_to_wait_when_polling_for_master);
    waited_so_far += reader_microseconds_to_wait_when_polling_for_master;
  }
  
  /* Restore previous error handler */
  H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
	
  return h5;
}

void get_current_lens(const std::vector<hid_t> &dsets, std::vector<hsize_t> &output_dims) {
  for (size_t idx = 0; idx < dsets.size(); ++idx) {
    hid_t dset = dsets.at(idx);
    hsize_t dim;
    H5Drefresh(dset);
    H5LDget_dset_dims(dset, &dim);
    output_dims.at(idx) = dim;
  }
}

}; // local namespace 


int main(int argc, const char *argv[]) {
  int num_args = 2 + num_writers;
  if (argc != num_args) {
    fprintf(stderr, "usage: outputfile writer_file_1 writer_file_2 ...\n");
    return -1;
  }

  const char *master_outputfile = argv[1];
  std::vector<std::string> writer_files;
  for (int idx=0; idx < num_writers; ++idx) {
    writer_files.push_back(std::string(argv[2+idx]));
  }

  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid = H5Fcreate(master_outputfile, H5F_ACC_TRUNC, H5P_DEFAULT, fapl);

  int64_t fill = 0xaabbccddeeff;
  hid_t dcpl = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_fill_value(dcpl, H5T_NATIVE_INT64, &fill);
 
  const int rank1 = 1;
  hsize_t size0 = 0;
  hsize_t unlimited = H5S_UNLIMITED, countUnlimited=H5S_UNLIMITED, blockUnlimited=H5S_UNLIMITED;
  hsize_t start0=0, block1=1, count1=1, stride1=1;

  hid_t src_space = H5Screate_simple(rank1, &size0, &unlimited);
  H5Sselect_hyperslab(src_space, H5S_SELECT_SET, &start0, &stride1, &count1, &blockUnlimited);
  
  hid_t vds_space = H5Screate_simple(rank1, &size0, &unlimited);
  
  for (size_t writer = 0; writer < writer_files.size(); ++writer) {
    hsize_t vds_stride = num_writers;
    hsize_t vds_start = writer;
    
    H5Sselect_hyperslab(vds_space, H5S_SELECT_SET, &vds_start, &vds_stride, &countUnlimited, &block1);
    H5Pset_virtual(dcpl, vds_space, writer_files.at(writer).c_str(), "/data", src_space);
  }
  hid_t access_id = H5Pcreate(H5P_DATASET_ACCESS);
  H5Pset_virtual_view( access_id, H5D_VDS_FIRST_MISSING);
  hid_t dset = H5Dcreate2(fid, "vds", H5T_NATIVE_INT64, vds_space, 
                          H5P_DEFAULT, dcpl, access_id);

  hid_t avail_dset = -1;
  {
    hid_t avail_space = H5Screate_simple(rank1, &size0, &unlimited);
    hid_t plist_id = H5Pcreate(H5P_DATASET_CREATE);
    H5Pset_chunk(plist_id, rank1, &chunk_size);
    int8_t fill = 1;
    H5Pset_fill_value(plist_id, H5T_NATIVE_INT8, &fill);

    hid_t access_id = H5Pcreate(H5P_DATASET_ACCESS);
    size_t rdcc_nslots = 101;
    size_t rdcc_nbytes = chunk_size * 24;
    double rdcc_w0 = 0.75;
    H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0);
    
    avail_dset = H5Dcreate2(fid, "avail", H5T_NATIVE_INT8, avail_space, 
                            H5P_DEFAULT, plist_id, access_id);
    H5Pclose(access_id);
    H5Pclose(plist_id);
  }

  H5Fstart_swmr_write(fid);
  
  std::vector<hid_t> writer_fids(num_writers);
  std::vector<hid_t> writer_dsets(num_writers);
  std::vector<hsize_t> writer_dims(num_writers);
  std::vector<hsize_t> writer_dims_in_round_robin(num_writers);

  for (size_t writer = 0; writer < num_writers; ++writer) {
    writer_fids.at(writer) = H5Fopen_with_polling(writer_files.at(writer).c_str(), 
                                                  H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, 
                                                  H5P_DEFAULT);
    writer_dsets.at(writer) = H5Dopen2(writer_fids.at(writer), "data", H5P_DEFAULT);
  }
  
  hsize_t len_where_no_missing = 0;
  while (len_where_no_missing < hsize_t(master_len)) {
    get_current_lens(writer_dsets, writer_dims);
    for (size_t idx = 0; idx < num_writers; ++idx) {
      writer_dims_in_round_robin.at(idx) = num_writers * writer_dims.at(idx)  + idx;
    }
    hsize_t previous = len_where_no_missing;
    len_where_no_missing = *std::min_element(writer_dims_in_round_robin.begin(), writer_dims_in_round_robin.end());
    if (len_where_no_missing > previous) {
      H5Dset_extent(avail_dset, &len_where_no_missing);
      H5Dflush(avail_dset);
      std::cout << "master: " << len_where_no_missing << std::endl;
    }
    usleep(microseconds_between_master_wait);
  }
  
  for (size_t writer = 0; writer < num_writers; ++writer) {
    H5Dclose(writer_dsets.at(writer));
    H5Fclose(writer_fids.at(writer));
  }
  H5Dclose(avail_dset);  
  H5Dclose(dset);
  H5Pclose(access_id);
  H5Sclose(src_space);
  H5Sclose(vds_space);
  H5Pclose(dcpl);
  H5Fclose(fid);
  H5Pclose(fapl);

  std::cout << "master done" << std::endl;
  return 0;
}

