#ifndef LC2_DSET_HH
#define LC2_DSET_HH

#include <vector>
#include "hdf5.h"


class Dset {
  // wrap pieces used for appending/reading N events to datasets. 
  // Cache some things for these operations

  hid_t m_id;
  hid_t m_type;
  hid_t m_file_space;
  hid_t m_mem_space;

  std::vector<hsize_t> m_dim;

  std::vector<hsize_t> m_file_select_start;
  std::vector<hsize_t> m_file_select_count;
  std::vector<hsize_t> m_file_select_stride;
  std::vector<hsize_t> m_file_select_block;

  // will always be maintained for a contigous block
  std::vector<hsize_t> m_mem_select_start;
  std::vector<hsize_t> m_mem_select_count;
  std::vector<hsize_t> m_mem_select_stride;
  std::vector<hsize_t> m_mem_select_block;

 public:
  Dset() = default;
  Dset(const Dset &o) = default;
  Dset &operator=(const Dset &o) = default;

  // custom constructors
  Dset(hid_t _id, hsize_t extent);
  Dset(hid_t _id, const std::vector<hsize_t> & _dim);

  // accessors
  hid_t id() const { return m_id; }
  hid_t mem_space() const { return m_mem_space; }
  hid_t file_space() const { return m_file_space; }
  const std::vector<hsize_t> & dim() const { return m_dim; }

  // reset the dimensions, updates the filespace
  void dim(const std::vector<hsize_t> &_dim);

  // these will trigger resetting the memspace as well, for 1 or count
  void file_space_select(int64_t event_index);
  void file_space_select(int64_t event_index_start, int64_t count);

  // close/cleanup
  void close();
  ~Dset() {};

  void append(const std::vector<int64_t> &data);
  void append(const std::vector<int16_t> &data);
  
  static Dset create(hid_t parent, const char *name, hid_t h5type, const std::vector<hsize_t> &chunk);
};

#endif
