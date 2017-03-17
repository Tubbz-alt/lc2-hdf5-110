#ifndef ANA_DAQ_UTIL_HH
#define ANA_DAQ_UTIL_HH

#include <cstdio>
#include <vector>
#include <chrono>

#include "hdf5.h"

#define NONNEG(arg) check_nonneg(arg, #arg, __LINE__, __FILE__)
template<class T>
T check_nonneg(T val, const char *expression, int lineno, const char *fname) {
  if (val < 0) {
    static char msg[1024]; 
    sprintf(msg, "ERROR: %lld = %s line=%d  file=%s\n", (long long int)val, expression, lineno, fname);
    throw std::runtime_error(msg);
  }
  //  printf("%s\n" , expression );
  //  fflush(stdout);
  return val;
}

#define POS(arg) check_pos(arg, #arg, __LINE__, __FILE__)
template<class  T>
T check_pos(T val, const char *expression, int lineno, const char *fname) {
  if (val <= 0) {
    static char msg[1024]; 
    sprintf(msg, "ERROR: %lld = %s line=%d  file=%s\n", (long long int)val, expression, lineno, fname);
    throw std::runtime_error(msg);
  } 
  return val;
}

typedef std::chrono::high_resolution_clock Clock;
typedef std::map<int, hid_t>::const_iterator CMapIter;

const int CSPadDim1 = 32;
const int CSPadDim2 = 185;
const int CSPadDim3 = 388;
const int CSPadNumElem = CSPadDim1*CSPadDim2*CSPadDim3;


struct DsetInfo {
  hid_t dset_id;
  hsize_t extent;
  DsetInfo(hid_t _dset_id, hsize_t _extent) : dset_id(_dset_id), extent(_extent) {}
  DsetInfo() : dset_id(-1), extent(-1) {}
  DsetInfo(const DsetInfo &o) :dset_id(o.dset_id), extent(o.extent) {}
  DsetInfo &operator=(const DsetInfo &o) { dset_id = o.dset_id; extent = o.extent; return *this; }
};

DsetInfo create_1d_dataset(hid_t, const char *, hid_t, hsize_t, size_t);
DsetInfo create_4d_short_dataset(hid_t parent, const char * dset, int dim1, int dim2, int dim3, int chunk_size);

void append_to_1d_dset(DsetInfo &, long);
void append_to_4d_short_dset(DsetInfo &, int, int, int, short *);

hsize_t append_many_to_1d_dset(DsetInfo &, hsize_t, long *);

bool wait_for_dataset_to_grow(hid_t dset_id, hsize_t * dims, hsize_t len_to_grow_to, int microseconds_to_pause=0, int timeout_seconds=-1.0);

long read_long_from_1d(hid_t dset_id, long event_index);

int foo();

#endif // ANA_DAQ_UTIL_HH
