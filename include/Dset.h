#ifndef LC2_DSET_HH
#define LC2_DSET_HH

#include <vector>
#include "hdf5.h"

// utility functions:
template <class T>
std::ostream & operator<<(std::ostream &ostr, const std::vector<T> &vec) {
  for (auto iter=vec.begin(); iter != vec.end(); ++iter) {
    ostr << *iter <<',';
  }
  return ostr;
}

void print_h5space(std::ostream &ostr, const char *header, hid_t space);

// main class
class Dset {
  // wrap pieces used for appending/reading N events to datasets. 
  // Cache some things for these operations

  hid_t m_id;
  hid_t m_type;
  std::vector<hsize_t> m_dims;

protected:
  void check_append(hid_t type, hsize_t start, hsize_t count, size_t data_len);
  void check_read(hid_t type, hsize_t start, hsize_t count);
  void file_space_select(hid_t file_space, hsize_t start, hsize_t count);
  void generic_append(hsize_t count, const void *data);
  void generic_read(hsize_t start, hsize_t count, void *data);
  std::ostream & dbgInfo(std::ostream &o);

public:
  Dset() = default;
  Dset(const Dset &o) = default;
  Dset &operator=(const Dset &o) = default;
  ~Dset() {};

  // accessors
  hid_t id() const { return m_id; }
  const std::vector<hsize_t> & dim() const { return m_dims; }

  // close/cleanup
  void close();

  void append(hsize_t start, hsize_t count, const std::vector<int64_t> &data);
  void append(hsize_t start, hsize_t count, const std::vector<int16_t> &data);

	void read(hsize_t start, hsize_t count, std::vector<int64_t> &data);
	void read(hsize_t start, hsize_t count, std::vector<int16_t> &data);

  bool wait(hsize_t len_to_grow_to, int microseconds_to_pause, int timeout_seconds, bool verbose);

  static Dset create(hid_t parent, const char *name, hid_t h5type, const std::vector<hsize_t> &chunk);
  static Dset open(hid_t parent, const char *name);
  static std::vector<hsize_t> get_chunk(const std::string & fname, const std::string &dset);
  static std::vector<hsize_t> get_chunk(hid_t parent, const std::string &dset);
  static std::vector<hsize_t> get_chunk(hid_t parent, hid_t dset);
 
};

#endif
