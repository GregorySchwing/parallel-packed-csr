/**
 * Created by Eleni Alevra
 * modified by Christian Menges
 */

#include <bfs.h>
#include <pagerank.h>

#include <chrono>
#include <cmath>
#include <ctime>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "thread_pool/thread_pool.h"
#include "thread_pool_pppcsr/thread_pool_pppcsr.h"

#include "helpers.h"


using namespace std;


int main(int argc, char *argv[]) {
  int threads = 8;
  int size = 1000000;
  int num_nodes = 0;
  bool lock_search = true;
  bool insert = true;
  PCSRVersion v = PCSRVersion::PPPCSRNUMA;
  int partitions_per_domain = 1;
  vector<tuple<Operation, int, int>> core_graph;
  vector<tuple<Operation, int, int>> updates;
  for (int i = 1; i < argc; i++) {
    string s = string(argv[i]);
    if (s.rfind("-threads=", 0) == 0) {
      threads = stoi(s.substr(string("-threads=").length(), s.length()));
    } else if (s.rfind("-size=", 0) == 0) {
      size = stoi(s.substr(string("-size=").length(), s.length()));
    } else if (s.rfind("-lock_free", 0) == 0) {
      lock_search = false;
    } else if (s.rfind("-insert", 0) == 0) {
      insert = true;
    } else if (s.rfind("-delete", 0) == 0) {
      insert = false;
    } else if (s.rfind("-pppcsrnuma", 0) == 0) {
      v = PCSRVersion::PPPCSRNUMA;
    } else if (s.rfind("-pppcsr", 0) == 0) {
      v = PCSRVersion::PPPCSR;
    } else if (s.rfind("-ppcsr", 0) == 0) {
      v = PCSRVersion::PPCSR;
    } else if (s.rfind("-partitions_per_domain=", 0) == 0) {
      partitions_per_domain = stoi(s.substr(string("-partitions_per_domain=").length(), s.length()));
    } else if (s.rfind("-core_graph=", 0) == 0) {
      string core_graph_filename = s.substr(string("-core_graph=").length(), s.length());
      int temp = 0;
      std::tie(core_graph, temp) = read_input(core_graph_filename, Operation::ADD);
      num_nodes = std::max(num_nodes, temp);
    } else if (s.rfind("-update_file=", 0) == 0) {
      string update_filename = s.substr(string("-update_file=").length(), s.length());
      cout << update_filename << endl;
      int temp = 0;
      Operation defaultOp = Operation::ADD;
      if (!insert) {
        defaultOp = Operation::DELETE;
      }
      std::tie(updates, temp) = read_input(update_filename, defaultOp);
      num_nodes = std::max(num_nodes, temp);
      size = std::min((size_t)size, updates.size());
    }
  }
  if (core_graph.empty()) {
    cout << "Core graph file not specified" << endl;
    exit(EXIT_FAILURE);
  }
  if (updates.empty()) {
    cout << "Updates file not specified" << endl;
    exit(EXIT_FAILURE);
  }
  cout << "Core graph size: " << core_graph.size() << endl;
  //   sort(core_graph.begin(), core_graph.end());
  switch (v) {
    case PCSRVersion::PPCSR: {
      auto thread_pool = make_unique<ThreadPool>(threads, lock_search, num_nodes + 1, partitions_per_domain);
      execute(threads, size, core_graph, updates, thread_pool);
      break;
    }
    case PCSRVersion::PPPCSR: {
      auto thread_pool =
          make_unique<ThreadPoolPPPCSR>(threads, lock_search, num_nodes + 1, partitions_per_domain, false);
      execute(threads, size, core_graph, updates, thread_pool);
      break;
    }
    default: {
      auto thread_pool =
          make_unique<ThreadPoolPPPCSR>(threads, lock_search, num_nodes + 1, partitions_per_domain, true);
      execute(threads, size, core_graph, updates, thread_pool);
    }
  }

  return 0;
}
