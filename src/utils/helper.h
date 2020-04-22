#pragma once
//lang::Cpp

#include <cstdlib>
#include <cstring>
#include <cmath>
#include <iostream>
#include <string>

/** Helper class providing some C++ functionality and convenience
 *  functions. This class has no data, constructors, destructors or
 *  virtual functions. Inheriting from it is zero cost.
 */
class Sys {
 public:

  // Printing functions
  Sys& p(char* c) { std::cout << c; return *this; }
  Sys& p(bool c) { std::cout << c; return *this; }
  Sys& p(float c) { std::cout << c; return *this; }  
  Sys& p(int i) { std::cout << i;  return *this; }
  Sys& p(size_t i) { std::cout << i;  return *this; }
  Sys& p(const char* c) { std::cout << c;  return *this; }
  Sys& p(char c) { std::cout << c;  return *this; }
  Sys& p(double d) { std::cout << d; return *this; }
  Sys& pln() { std::cout << "\n";  return *this; }
  Sys& pln(int i) { std::cout << i << "\n";  return *this; }
  Sys& pln(char* c) { std::cout << c << "\n";  return *this; }
  Sys& pln(bool c) { std::cout << c << "\n";  return *this; }  
  Sys& pln(char c) { std::cout << c << "\n";  return *this; }
  Sys& pln(float x) { std::cout << x << "\n";  return *this; }
  Sys& pln(size_t x) { std::cout << x << "\n";  return *this; }
  Sys& pln(const char* c) { std::cout << c << "\n";  return *this; }
  Sys& pln(double d) { std::cout << d << "\n"; return *this; }

  // Copying strings
  char* duplicate(const char* s) {
    if(s == nullptr) pln("help!");
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
  }
  char* duplicate(char* s) {
    if(s == nullptr) pln("help!");
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
  }

  bool stringEqual(char* str1, char* str2) {
      return strcmp(str1, str2) == 0;
  }

  bool doubleAlmostEqual(double d1, double d2, size_t sig_figs) {
      return fabs(d1 - d2) < pow(10, -1 * sig_figs);
  }

  // Function to terminate execution with a message
  void exit_if_not(bool b, char* c) {
    if (b) return;
    p("Exit message: ").pln(c);
    exit(-1);
  }
  
  // Definitely fail
//  void FAIL() {
  void myfail(){
    pln("Failing");
    exit(1);
  }

  // Some utilities for lightweight testing
  void OK(const char* m) { pln(m); }
  void t_true(bool p) { if (!p) myfail(); }
  void t_false(bool p) { if (p) myfail(); }
};

template<typename T> static char* to_str(T v) {
  std::string str = std::to_string(v);
  const char* c_str = str.c_str();
  return strdup(c_str);
}
