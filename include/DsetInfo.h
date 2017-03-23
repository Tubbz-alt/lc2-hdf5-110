#ifndef LC2_DSETINFO_HH
#define LC2_DSETINFO_HH

#include <vector>
#include "hdf5.h"


class DsetInfo {
 private:
  hid_t m_dset_id;
  std::vector<hsize_t> m_dim;

 public:
  hid_t dset_id() const { return m_dset_id; }
  const std::vector<hsize_t> & const_dim() const { return m_dim; }
  std::vector<hsize_t> & dim() { return m_dim; }

  void dset_id(hid_t new_dset) { m_dset_id = new_dset; }
  virtual void dim(const std::vector<hsize_t> &new_dim) { m_dim = new_dim; }
 
  virtual void close();
  virtual ~DsetInfo() {};

  DsetInfo() : m_dset_id(-1) {};
  DsetInfo(const DsetInfo &o) : m_dset_id(o.dset_id()), m_dim(o.const_dim()) {};
  DsetInfo &operator=(const DsetInfo &o) { 
    dset_id(o.dset_id()); 
    dim(o.const_dim()); 
    return *this; 
  }
  DsetInfo(hid_t _dset_id, hsize_t extent) : m_dset_id(_dset_id) { 
    m_dim.push_back(extent); 
  };
  DsetInfo(hid_t _dset_id, std::vector<hsize_t> _dim) : m_dset_id(_dset_id), m_dim(_dim) {};
};


class DsetWriterInfo : public DsetInfo {
 public:
  
  virtual void close();
  virtual ~DsetWriterInfo() {};
  
  DsetWriterInfo() : DsetInfo() {};
  DsetWriterInfo(const DsetWriterInfo &o) : DsetInfo(o) {};
  DsetWriterInfo &operator=(DsetWriterInfo &o);

  DsetWriterInfo(hid_t _dset_id, std::vector<hsize_t> _dim);
  DsetWriterInfo(hid_t _dset_id, hsize_t _extent);
};

class DsetReaderInfo : public DsetInfo {
  hid_t m_mem_space_one_event;
  hid_t m_file_space_id;

 public:
  
  virtual void close();
  virtual ~DsetReaderInfo() {};
  virtual void dim(const std::vector<hsize_t> &new_dim);
  std::vector<hsize_t> & dim() { return DsetINfo::dim(); }
  
  DsetReaderInfo() : DsetInfo(), m_mem_space_one_event(-1), m_file_space_id(-1) {};
  DsetReaderInfo(const DsetReaderInfo &o) : DsetInfo(o), m_mem_space_one_event(o.m_mem_space_one_event), m_file_space_id(o.m_file_space_id) {};
  DsetReaderInfo &operator=(DsetReaderInfo &o);

  hid_t mem_space_one_event() const { return m_mem_space_one_event; }
  hid_t file_space_id() const { return m_file_space_id; }

  void mem_space_one_event(hid_t space)  { m_mem_space_one_event = space; }
  void file_space_id(hid_t space) { m_file_space_id = space; }

  void select_file_space_id(int64_t event_index);

};


#endif
