#include "Dset.h"
#include "check_macros.h"


Dset::Dset(hid_t _id, hsize_t extent) : 
  m_id(_id), 
  m_file_space(-1),
  m_mem_space(-1)
{
  std::vector<hsize_t> _dim(extent);
  dim(_dim);
}

Dset::Dset(hid_t _id, const std::vector<hsize_t> & _dim) : 
  m_id(_id), 
  m_file_space(-1),
  m_mem_space(-1)
{
  dim(_dim);
}

void Dset::dim(const std::vector<hsize_t> & _dim) 
{
}

Dset Dset::create(hid_t parent, const char *name, hid_t h5type, const std::vector<hsize_t> & chunk) {
  
}

void Dset::append(const std::vector<int64_t> & data) {
}

void Dset::append(const std::vector<int16_t> & data) {
}


/*
// -------------------------------------------------------------------
// Dset
void Dset::close() {
 // don't own id
}

// -------------------------------------------------------------------
// DsetWriterInfo
void DsetWriterInfo::close() {
  Dset::close();
  if (m_filespace > -1) {
    NONNEG( H5Sclose( m_filespace ) );
  }
  if (m_mem_space > -1) {
    NONNEG( H5Sclose( m_mem_space) );
  }
}

DsetWriterInfo &DsetWriterInfo::operator=( DsetWriterInfo &o) {
  Dset::operator=(o);
  m_filespace = o.m_filespace;
  m_mem_space = o.m_mem_space;
  return *this;
}

DsetWriterInfo::DsetWriterInfo(hid_t _dset_i; std::vector<hsize_t> _dim) :
  Dset(_dset_id, _dim)
{
  dim( _dim );
}

DsetWriterInfo::DsetWriterInfo(hid_t _dset_id, hsize_t _extent) :
  Dset(_dset_id, _extent) {
}

// -------------------------------------------------------------------
// DsetReaderInfo
void DsetReaderInfo::close() {
  if (m_mem_space_id > -1) {
    NONNEG( H5Sclose( m_mem_space_id) );
  }
  if (file_space_id() > -1) {
    NONNEG( H5Sclose( file_space_id() ) );
  }
  Dset::close();
}

DsetReaderInfo::DsetReaderInfo( const DsetReaderInfo &o) :
  Dset(o),
  m_mem_space_id(o.m_mem_space_id),
  m_file_space_id(o.m_file_space_id),
  m_file_select_start(o.m_file_select_start),
  m_file_select_count(o.m_file_select_count),
  m_file_select_stride(o.m_file_select_stride),
  m_file_select_block(o.m_file_select_block)
{
}

DsetReaderInfo &DsetReaderInfo::operator=( DsetReaderInfo &o) {
  Dset::operator=(o);
  m_mem_space_id = o.m_mem_space_id;
  m_file_space_id = o.m_file_space_id;

  m_file_select_start = o.m_file_select_start;
  m_file_select_count = o.m_file_select_count;
  m_file_select_stride = o.m_file_select_stride;
  m_file_select_block = o.m_file_select_block;
  
  return *this;
}

void DsetReaderInfo::dim(const std::vector<hsize_t> &new_dim) {
  if (file_space_id() < 0) {
    file_space_id(  NONNEG( H5Screate( H5S_SIMPLE ) ) );
  }

  if (mem_space_id() < 0) {
    mem_space_id(  NONNEG( H5Screate( H5S_SIMPLE ) ) );
  }

  Dset::dim(new_dim);

  m_file_select_start = std::vector<hsize_t>(new_dim.size(), 0);

  m_file_select_count = std::vector<hsize_t>(new_dim.size(), 1);
  
  m_file_select_stride = std::vector<hsize_t>(new_dim.size(), 1);

  m_file_select_block = new_dim;
  m_file_select_block.at(0)=1;
  
  NONNEG( H5Sset_extent_simple( file_space_id(), dim().size(), &dim().at(0), NULL) );

  NONNEG( H5Sset_extent_simple( mem_space_id(), dim().size(), &m_file_select_block.at(0), NULL) );
}

void DsetReaderInfo::file_space_select(int64_t event_index) {
  m_file_select_start.at(0)=event_index;
  NONNEG( H5Sselect_hyperslab( file_space_id(),
                               H5S_SELECT_SET,
                               &m_file_select_start.at(0),
                               &m_file_select_count.at(0),
                               &m_file_select_stride.at(0),
                               &m_file_select_block.at(0) ) );
}
*/

