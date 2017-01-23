#include <cstdio>
#include <stdexcept>
#include "hdf5.h"

void check_nonneg(int err, const char *msg, int lineno, const char *fname) {
  if (err < 0) {
    fprintf(::stderr, "ERROR: %s line=%d  file=%s\n", msg, lineno, fname);
    throw std::runtime_error("FATAL: check_nonneg");
  } 
}


int foo() {
  return 3;
}


