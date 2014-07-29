#ifdef __APPLE__
#include "platform.darwin.hpp"
#elif defined(__linux__)
#include "platform.linux.hpp"
#else
#error "Please generate a platform.h for your platform."
#endif
