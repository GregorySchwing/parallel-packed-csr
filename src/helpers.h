#ifndef HELPERS_PCSR_H
#define HELPERS_PCSR_H

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

using namespace std;

enum class Operation { READ, ADD, DELETE };

constexpr const char* OperationToString(Operation o) noexcept
{
    switch (o)
    {
        case Operation::READ: return "READ";
        case Operation::ADD: return "ADD";
        case Operation::DELETE: return "DELETE";
    }
}
// Reads edge list with separator
pair<vector<tuple<Operation, int, int>>, int> read_input(string filename, Operation defaultOp, int & num_nodes, int & num_edges) {
  ifstream f;
  string line;
  f.open(filename);
  if (!f.good()) {
    std::cerr << "Invalid file" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::size_t pos, pos2;

  // First line is num_nodes & num_edges
  getline(f, line);
  num_nodes = stoi(line, &pos);
  num_edges = stoi(line.substr(pos + 1), &pos2);

  vector<tuple<Operation, int, int>> edges;
  while (getline(f, line)) {
    int src = stoi(line, &pos);
    int target = stoi(line.substr(pos + 1), &pos2);

    num_nodes = std::max(num_nodes, std::max(src, target));

    Operation op = defaultOp;
    if (pos + 1 + pos2 + 1 < line.length()) {
      switch (line[pos + 1 + pos2 + 1]) {
        case '1':
          op = Operation::ADD;
          break;
        case '0':
          op = Operation::DELETE;
          break;
        default:
          cerr << "Invalid operation";
      }
    }
    edges.emplace_back(op, src, target);
  }
  return make_pair(edges, num_nodes);
}

// Reads edge list with separator
pair<vector<tuple<Operation, int, int>>, int> read_input(string filename, Operation defaultOp) {
  ifstream f;
  string line;
  f.open(filename);
  if (!f.good()) {
    std::cerr << "Invalid file" << std::endl;
    exit(EXIT_FAILURE);
  }
  vector<tuple<Operation, int, int>> edges;
  int num_nodes = 0;
  std::size_t pos, pos2;
  while (getline(f, line)) {
    int src = stoi(line, &pos);
    int target = stoi(line.substr(pos + 1), &pos2);

    num_nodes = std::max(num_nodes, std::max(src, target));

    Operation op = defaultOp;
    if (pos + 1 + pos2 + 1 < line.length()) {
      switch (line[pos + 1 + pos2 + 1]) {
        case '1':
          op = Operation::ADD;
          break;
        case '0':
          op = Operation::DELETE;
          break;
        default:
          cerr << "Invalid operation";
      }
    }
    edges.emplace_back(op, src, target);
  }
  return make_pair(edges, num_nodes);
}



// Does insertions
template <typename ThreadPool_t>
void update_existing_graph(const vector<tuple<Operation, int, int>> &input, ThreadPool_t *thread_pool, int threads,
                           int size) {
  for (int i = 0; i < size; i++) {
    switch (get<0>(input[i])) {
      case Operation::ADD:
        thread_pool->submit_add(i % threads, get<1>(input[i]), get<2>(input[i]));
        break;
      case Operation::DELETE:
        thread_pool->submit_delete(i % threads, get<1>(input[i]), get<2>(input[i]));
        break;
      case Operation::READ:
        cerr << "Not implemented\n";
        break;
    }
  }
  thread_pool->start(threads);
  thread_pool->stop();
}

template <typename ThreadPool_t>
void execute(int threads, int size, const vector<tuple<Operation, int, int>> &core_graph,
             const vector<tuple<Operation, int, int>> &updates, std::unique_ptr<ThreadPool_t> &thread_pool) {
  // Load core graph
  update_existing_graph(core_graph, thread_pool.get(), threads, core_graph.size());
  // Do updates
  update_existing_graph(updates, thread_pool.get(), threads, size);

  //    DEBUGGING CODE
  //    Check that all edges are there and in sorted order
  //    for (int i = 0; i < core_graph.size(); i++) {
  //        if (!thread_pool->pcsr->edge_exists(std::get<1>(core_graph[i]),std::get<2>(core_graph[i]))) {
  //            cout << "Not there " <<  std::get<1>(core_graph[i]) << " " <<
  //                 std::get<2>(core_graph[i]) << endl;
  //        }
  //    }
  //    for (int i = 0; i < size; i++) {
  //        if (!thread_pool->pcsr->edge_exists(std::get<1>(updates[i]), std::get<2>(updates[i]))) {
  //            cout << "Update not there " << std::get<1>(updates[i]) << " " <<
  //                 std::get<2>(updates[i]) << endl;
  //        }
  //    }
}


template <typename ThreadPool_t>
void executeInitial(int threads, int size, const vector<tuple<Operation, int, int>> &core_graph,
                    std::unique_ptr<ThreadPool_t> &thread_pool) {
  // Load core graph
  update_existing_graph(core_graph, thread_pool.get(), threads, core_graph.size());


  //    DEBUGGING CODE
  //    Check that all edges are there and in sorted order
  //    for (int i = 0; i < core_graph.size(); i++) {
  //        if (!thread_pool->pcsr->edge_exists(std::get<1>(core_graph[i]),std::get<2>(core_graph[i]))) {
  //            cout << "Not there " <<  std::get<1>(core_graph[i]) << " " <<
  //                 std::get<2>(core_graph[i]) << endl;
  //        }
  //    }
  //    for (int i = 0; i < size; i++) {
  //        if (!thread_pool->pcsr->edge_exists(std::get<1>(updates[i]), std::get<2>(updates[i]))) {
  //            cout << "Update not there " << std::get<1>(updates[i]) << " " <<
  //                 std::get<2>(updates[i]) << endl;
  //        }
  //    }
}


template <typename ThreadPool_t>
void executeUpdates(int threads, int size, const vector<tuple<Operation, int, int>> &updates,
                    std::unique_ptr<ThreadPool_t> &thread_pool) {
  // Load core graph
  update_existing_graph(updates, thread_pool.get(), threads, size);


  //    DEBUGGING CODE
  //    Check that all edges are there and in sorted order
  //    for (int i = 0; i < core_graph.size(); i++) {
  //        if (!thread_pool->pcsr->edge_exists(std::get<1>(core_graph[i]),std::get<2>(core_graph[i]))) {
  //            cout << "Not there " <<  std::get<1>(core_graph[i]) << " " <<
  //                 std::get<2>(core_graph[i]) << endl;
  //        }
  //    }
  //    for (int i = 0; i < size; i++) {
  //        if (!thread_pool->pcsr->edge_exists(std::get<1>(updates[i]), std::get<2>(updates[i]))) {
  //            cout << "Update not there " << std::get<1>(updates[i]) << " " <<
  //                 std::get<2>(updates[i]) << endl;
  //        }
  //    }
}

enum class PCSRVersion { PPCSR, PPPCSR, PPPCSRNUMA };

#endif