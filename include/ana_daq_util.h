#ifndef LC2DAQ_HH
#define LC2DAQ_HH

#define CHECK_NONNEG(x , msg) check_nonneg(x, msg, __LINE__, __FILE__)

int foo();
void check_nonneg(int, const char *, int, const char *);

#endif // LC2DAQ_HH
