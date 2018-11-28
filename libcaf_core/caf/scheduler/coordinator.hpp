/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2016                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#ifndef CAF_SCHEDULER_COORDINATOR_HPP
#define CAF_SCHEDULER_COORDINATOR_HPP

#include "caf/config.hpp"

#include <thread>
#include <limits>
#include <memory>
#include <iostream>
#include <condition_variable>

#include "caf/scheduler/worker.hpp"
#include "caf/scheduler/abstract_coordinator.hpp"

namespace caf {
namespace scheduler {

class worker_group {
public:
    using queue_type = std::list<resumable*>;


	worker_group* parent;
	size_t id;
	std::vector<size_t> worker_ids;
	std::vector<worker_group*> wg_children;
    std::uniform_int_distribution<size_t> uniform;
    std::default_random_engine rengine;
    size_t repeat;
    size_t size_;
    size_t base_;


    size_t next_wg_;
//work sharing
    queue_type queue;
    std::mutex lock;
    std::condition_variable cv;
    std::atomic<int> pollers;


	worker_group(worker_group* p, size_t i, size_t size, size_t r, size_t base): parent(p), id(i), uniform(0, size-2), rengine(std::random_device{}()), repeat(r), size_(size), next_wg_(0), base_(base), pollers(0){};

    int get_no(size_t sid){
        if(size_ < 3) {
            return (sid+1)%2;
        }
        auto victim =  uniform(rengine);
        victim = worker_ids[victim];
    	if (victim == sid)
    	   victim = worker_ids[worker_ids.size()-1];
        return victim;
    }


};
/// Policy-based implementation of the abstract coordinator base class.
template <class Policy>
class coordinator : public abstract_coordinator {
public:
  using super = abstract_coordinator;

  using policy_data = typename Policy::coordinator_data;

  coordinator(actor_system& sys) : super(sys), data_(this), wg_root_(nullptr, 0, 64, 24, 0) {
    // nop
  }

  using worker_type = worker<Policy>;

  worker_type* worker_by_id(size_t x) {
    return workers_[x].get();
  }

  policy_data& data() {
    return data_;
  }

protected:
  void start() override {
    // initialize workers vector
    auto num = num_workers();
    workers_.reserve(num);
	std::vector<worker_group*> leafs;
    leafs.reserve(64);
    int id = 1;

#if 1
    wg_root_.wg_children.reserve(8);
    for(size_t i = 0; i < 8; i++){
        //root has id 0
        worker_group* wg_tmp = new worker_group(&wg_root_, id++, 8, 12, i*8);
        wg_root_.wg_children.push_back(wg_tmp);
        //std::cout << i << ":" << id << std::endl;
        for(size_t j = 0; j < 4; j++){
            worker_group* nwg = new worker_group(wg_tmp, id++, 2, 2, i*8+j);
            wg_tmp->wg_children.push_back(nwg);
            leafs.push_back(nwg);
            //std::cout << "\t" << j << ":" << id << std::endl;
        }
        // direct children of wg_root [0-7][8-15]...
        for(size_t j =0; j < 8; j++) {
            wg_tmp->worker_ids.push_back(i*8+j);
        }
    }

    for (size_t i = 0; i < num; ++i){
      //wg=wg_root_.wg_children[(i/8)]->wg_children[(i%4)];
      size_t min = (i/8)*8;
      size_t max = min+7;
      workers_.emplace_back(new worker_type(i, this, max_throughput_, leafs[i/2], min, max));
      wg_root_.worker_ids.push_back(i);
      leafs[i/2]->worker_ids.push_back(i);

    }
#else
    wg_root_.wg_children.reserve(2);
    for(size_t i = 0; i < 2; i++){
        //root has id 0
        worker_group* r1 = new worker_group(&wg_root_, id++, 32, 20, i*32);
        wg_root_.wg_children.push_back(r1);
        // direct children of wg_root [0-31][32-63]...
        for(size_t j =0; j < 32; j++){
            r1->worker_ids.push_back(i*32+j);
        }

        for(size_t i2 =0 ; i2 < 2; i2++) {
            worker_group* r2 = new worker_group(r1, id++, 16, 16, i*32+i2*16);
            r1->wg_children.push_back(r2);

            for(size_t j =0; j < 16; j++){
                r2->worker_ids.push_back(i*32+i2*16+j);
            }

            for(size_t i3 =0 ; i3 < 2; i3++) {
                worker_group* r3 = new worker_group(r2, id++, 8, 12, i*32+i2*16+i3*4);
                r2->wg_children.push_back(r3);

                for(size_t j =0; j < 8; j++){
                    r3->worker_ids.push_back(i*32+i2*16+i3*4+j);

                }

                for(size_t j = 0; j < 4; j++){
                    worker_group* nwg = new worker_group(r3, id++, 2, 2, i*32+i2*16+i3*4+j);
                    r3->wg_children.push_back(nwg);
                    leafs.push_back(nwg);
                    //std::cout << "\t" << j << ":" << id << std::endl;
                }

            }


        }

    }

    for (size_t i = 0; i < num; ++i){
      //wg=wg_root_.wg_children[(i/8)]->wg_children[(i%4)];
      size_t min = (i/8)*8;
      size_t max = min+7;
      workers_.emplace_back(new worker_type(i, this, max_throughput_, leafs[i/2], min, max));
      wg_root_.worker_ids.push_back(i);
      leafs[i/2]->worker_ids.push_back(i);

    }

#endif
    // start all workers now that all workers have been initialized
    for (auto& w : workers_)
      w->start();
    // run remaining startup code
    super::start();
  }

  void stop() override {
    // shutdown workers
    class shutdown_helper : public resumable, public ref_counted {
    public:
      resumable::resume_result resume(execution_unit* ptr, size_t) override {
        CAF_ASSERT(ptr != nullptr);
        std::unique_lock<std::mutex> guard(mtx);
        last_worker = ptr;
        cv.notify_all();
        return resumable::shutdown_execution_unit;
      }
      void intrusive_ptr_add_ref_impl() override {
        intrusive_ptr_add_ref(this);
      }

      void intrusive_ptr_release_impl() override {
        intrusive_ptr_release(this);
      }
      shutdown_helper() : last_worker(nullptr) {
        // nop
      }
      std::mutex mtx;
      std::condition_variable cv;
      execution_unit* last_worker;
    };
    // use a set to keep track of remaining workers
    shutdown_helper sh;
    std::set<worker_type*> alive_workers;
    auto num = num_workers();
    for (size_t i = 0; i < num; ++i) {
      alive_workers.insert(worker_by_id(i));
      sh.ref(); // make sure reference count is high enough
    }
    while (!alive_workers.empty()) {
      (*alive_workers.begin())->external_enqueue(&sh);
      // since jobs can be stolen, we cannot assume that we have
      // actually shut down the worker we've enqueued sh to
      { // lifetime scope of guard
        std::unique_lock<std::mutex> guard(sh.mtx);
        sh.cv.wait(guard, [&] { return sh.last_worker != nullptr; });
      }
      alive_workers.erase(static_cast<worker_type*>(sh.last_worker));
      sh.last_worker = nullptr;
    }
    // shutdown utility actors
    stop_actors();
    // wait until all workers are done
    for (auto& w : workers_) {
      w->get_thread().join();
    }
    // run cleanup code for each resumable
    auto f = &abstract_coordinator::cleanup_and_release;
    for (auto& w : workers_)
      policy_.foreach_resumable(w.get(), f);
    policy_.foreach_central_resumable(this, f);
  }

  void enqueue(resumable* ptr) override {
    policy_.central_enqueue(this, ptr);
  }


public:
  worker_group& get_root(){
      return wg_root_;
  }
private:
  // usually of size std::thread::hardware_concurrency()
  std::vector<std::unique_ptr<worker_type>> workers_;
  // policy-specific data
  policy_data data_;
  // instance of our policy object
  Policy policy_;

  worker_group wg_root_;
};

} // namespace scheduler
} // namespace caf

#endif // CAF_SCHEDULER_COORDINATOR_HPP
