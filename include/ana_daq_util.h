#ifndef LC2DAQ_HH
#define LC2DAQ_HH

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
  printf("%s\n" , expression );
  fflush(stdout);
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

void divide_evenly(int total, int splits, std::vector<int> &offsets, std::vector<int> &counts);

struct DsetInfo {
  hid_t dset_id;
  hsize_t extent;
  DsetInfo(hid_t _dset_id, hsize_t _extent) : dset_id(_dset_id), extent(_extent) {}
  DsetInfo() : dset_id(-1), extent(-1) {}
  DsetInfo(const DsetInfo &o) :dset_id(o.dset_id), extent(o.extent) {}
  DsetInfo &operator=(const DsetInfo &o) { dset_id = o.dset_id; extent = o.extent; return *this; }
};

DsetInfo create_1d_dataset(hid_t, const char *, hid_t, hsize_t, size_t);
DsetInfo create_3d_dataset(hid_t, const char *, hid_t, int, int, int, size_t);
void append_to_1d_dset(DsetInfo &, long);
hsize_t append_many_to_1d_dset(DsetInfo &, hsize_t, long *);
void append_to_3d_dset(DsetInfo &, int, int, short *);

int foo();

void linedump_vector(FILE *fout, const char *hdr, const std::vector<int> &data);
int read_args(std::vector<int> &to_fill, int num, char *argv[], int start_at);
#endif // LC2DAQ_HH
