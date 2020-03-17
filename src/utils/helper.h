#pragma once
//lang::Cpp

#include <cstdlib>
#include <cstring>
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
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
  }
  char* duplicate(char* s) {
    char* res = new char[strlen(s) + 1];
    strcpy(res, s);
    return res;
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

  // conversion of numerical types to char* arrays
  char* size_t_to_str(size_t d) {
    std::string str = std::to_string(d);
    return strdup(str.c_str());
  }

  char* double_to_str(double d) {
    std::string str = std::to_string(d);
    return strdup(str.c_str());
  }

  char* short_to_str(short s) {
    std::string str = std::to_string(s);
    return strdup(str.c_str());
  }

  char* long_to_str(long l) {
    std::string str = std::to_string(l);
    return strdup(str.c_str());
  }
};
