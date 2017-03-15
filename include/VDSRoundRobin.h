#ifndef VDSROUNDROBIN_HH
#define VDSROUNDROBIN_HH

#include <vector>
#include <string>

#include "hdf5.h"

class VDSRoundRobin {

  const std::vector<std::string> m_src_filenames, m_src_dset_paths;

  std::vector<hid_t> m_src_fids;
  std::vector<hid_t> m_src_dsets;
  std::vector<hid_t> m_src_spaces;

  int m_rank;
  hid_t m_h5type;

  // for a 2D N x M detector, one_block will be [1,N,M]
  std::vector<hsize_t> m_one_block;

  hid_t m_vds_dcpl;
  hid_t m_vds_dset;
  
  void check_srcs() const;
  hid_t get_h5type_and_cache_files_dsets();
  int get_rank_and_cache_spaces();
  std::vector<hsize_t> get_one_block();

  void set_vds_fill_value();
  hid_t create_vds_space();
  hid_t select_all_of_any_src_countOne_blockUnlimited();
  void select_unlimited_count_of_vds(hid_t space, hsize_t start, hsize_t stride);
  void add_to_virtual_mapping(hid_t vds_src, hid_t src_space, size_t which_src);
  void cleanup();

 public:
  /**
   * create a VDS that is a round robin of the source datasets, over the first
   slow dimension.
  */
  VDSRoundRobin(hid_t vds_location,
                const char * vds_dset_name,
                std::vector<std::string> src_filenames,
                std::vector<std::string> src_dset_paths);
  ~VDSRoundRobin();
  hid_t get_and_transfer_ownership_of_VDS();
};

#endif
