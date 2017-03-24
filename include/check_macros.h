#ifndef CHECK_MACROS
#define CHECK_MACROS

#include <cstdio>
#include <stdexcept>

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


template<class  T>
T check_pos(T val, const char *expression, int lineno, const char *fname) {
  if (val <= 0) {
    static char msg[1024]; 
    sprintf(msg, "ERROR: %lld = %s line=%d  file=%s\n", (long long int)val, expression, lineno, fname);
    throw std::runtime_error(msg);
  } 
  return val;
}

#define POS(arg) check_pos(arg, #arg, __LINE__, __FILE__)
#define NONNEG(arg) check_nonneg(arg, #arg, __LINE__, __FILE__)

#endif // CHECK_MACROS
