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

#ifndef CAF_POLICY_WORK_STEALING_HIER_HPP
#define CAF_POLICY_WORK_STEALING_HIER_HPP

#include <deque>
#include <chrono>
#include <thread>
#include <random>
#include <cstddef>

#include "caf/resumable.hpp"
#include "caf/actor_system_config.hpp"

#include "caf/policy/unprofiled.hpp"

#include "caf/detail/double_ended_queue.hpp"

namespace caf {
namespace policy {
/// Implements scheduling of actors via work stealing.
/// @extends scheduler_policy
class work_shier : public unprofiled {
public:
  ~work_shier() override;

  // A thread-safe queue implementation.
  using queue_type = detail::double_ended_queue<resumable>;

  using usec = std::chrono::microseconds;

  // configuration for aggressive/moderate/relaxed poll strategies.
  struct poll_strategy {
    size_t attempts;
    size_t step_size;
    size_t steal_interval;
    usec sleep_duration;
  };

  // The coordinator has only a counter for round-robin enqueue to its workers.
  struct coordinator_data {
    inline explicit coordinator_data(scheduler::abstract_coordinator*)
        : next_worker(0) {
      // nop
    }

    std::atomic<size_t> next_worker;
  };

  // Holds job job queue of a worker and a random number generator.
  struct worker_data {
    inline explicit worker_data(scheduler::abstract_coordinator* p)
        : rengine(std::random_device{}()),
          // no need to worry about wrap-around; if `p->num_workers() < 2`,
          // `uniform` will not be used anyway
          uniform(0, p->num_workers() - 2),
          strategies{
            {p->system().config().work_stealing_aggressive_poll_attempts, 1,
             p->system().config().work_stealing_aggressive_steal_interval,
             usec{0}},
            {p->system().config().work_stealing_moderate_poll_attempts, 1,
             p->system().config().work_stealing_moderate_steal_interval,
             usec{p->system().config().work_stealing_moderate_sleep_duration_us}},
            {1, 0, p->system().config().work_stealing_relaxed_steal_interval,
            usec{p->system().config().work_stealing_relaxed_sleep_duration_us}}
          } {
      // nop
    }

    // This queue is exposed to other workers that may attempt to steal jobs
    // from it and the central scheduling unit can push new jobs to the queue.
    queue_type queue;
    // needed to generate pseudo random numbers
    std::default_random_engine rengine;
    std::uniform_int_distribution<size_t> uniform;
    poll_strategy strategies[3];
  };

  template<class Worker>
  int64_t get_size(Worker* self) {
    return d(self).queue.get_size();
  }

  // Goes on a raid in quest for a shiny new job.
  template <class Worker>
  resumable* try_steal(Worker* self) {
    auto p = self->parent();
    if (p->num_workers() < 2) {
      // you can't steal from yourself, can you?
      return nullptr;
    }
    // roll the dice to pick a victim other than ourselves
    auto victim = d(self).uniform(d(self).rengine);
    if (victim == self->id())
      victim = p->num_workers() - 1;
    // steal oldest element from the victim's queue
    return d(p->worker_by_id(victim)).queue.take_tail();
  }

  template <class Worker, class WorkerGroup>
  resumable* try_steal_h(Worker* self, size_t &current, WorkerGroup &wg_current, size_t &repeat) {
    auto p = self->parent();
    if (p->num_workers() < 2) {
      // you can't steal from yourself, can you?
      return nullptr;
    }
    if(repeat >= wg_current->repeat) {
        if(wg_current->parent)
            wg_current = wg_current->parent;
        /*else
            wg_current =  self->get_parent();*/

        //if(!wg_current){
        //    wg_current =  self->get_parent();
        //}
        repeat = 0;
    }
    ++repeat;
	auto victim = wg_current->get_no(self->id());
    if(victim < self->numa_min_ || victim > self->numa_max_)
        ++self->chunk_steals;

    queue_type &queue = d(p->worker_by_id(victim)).queue;
/*    if(queue.get_size() == 0)
        return nullptr;*/

	// steal oldest element from the victim's queue
//    if(wg_current->repeat < 9 || queue.get_size() < 16  || (victim > (self->id()/8)*8 && victim < ((self->id()/8)*8)+1))
	    return queue.take_tail();
//    else
//        return d(self).queue.steal_half_from(queue);
  }

  // Goes on a raid in quest for a shiny new job.
  template <class Worker, class WorkerGroup>
  resumable* try_steal_h2(Worker* self, size_t &current, WorkerGroup &wg_current, size_t &repeat) {
    auto p = self->parent();
    if (p->num_workers() < 2) {
      // you can't steal from yourself, can you?
      return nullptr;
    }
    // roll the dice to pick a victim other than ourselves
    /*auto victim = d(self).uniform(d(self).rengine);
    if (victim == self->id())
      victim = p->num_workers() - 1;
    // steal oldest element from the victim's queue
    return d(p->worker_by_id(victim)).queue.take_tail();*/

    auto wp = wg_current->worker_ids;
    //if(current == min) current = max+1;
	/*if(wg_current-> parent == nullptr)
	{
		auto victim = d(self).uniform(d(self).rengine);
		if (victim == self->id())
		  victim = p->num_workers() - 1;
		// steal oldest element from the victim's queue
		return d(p->worker_by_id(victim)).queue.take_tail();

	}*/

	if(wg_current-> parent == nullptr){
        wg_current = nullptr;
        return nullptr;
    }
    if(current > wp[wp.size()-1]){
/*        if(repeat < 4 * log(wp.size())){
            ++repeat;
        }else{
            repeat=0;*/
            wg_current = wg_current->parent;
            if(!wg_current){
            //{
                return nullptr;
            //    wg_current =  self->get_parent();
            }
       /*         min = max = self->id();
            }else{
                min = wp[0];
                max = wp[wp.size()-1];
            }*/
        //}
        current = wg_current->worker_ids[0];
    }
    if(current == self->id()) {
        ++current;
        return nullptr;
    }
//    if(self->id() == 63)
//        std::cout << self->id() << ":" << current << std::endl;
    //std::cout << current << std::endl;
	if(d(p->worker_by_id(current)).queue.get_size() == 0){
		++current;
		return nullptr;
		}
    return d(p->worker_by_id(current++)).queue.take_tail();
  }

  template <class Coordinator>
  void central_enqueue(Coordinator* self, resumable* job) {
    //auto w = self->worker_by_id(d(self).next_worker++ % self->num_workers());
    //w->external_enqueue(job);
    auto wg = self->get_root().wg_children[(self->get_root().next_wg_++)%(self->get_root().wg_children.size())];

    std::unique_lock<std::mutex> guard(wg->lock);
    wg->queue.push_back(job);
  }

  template <class Worker>
  void external_enqueue(Worker* self, resumable* job) {
    auto wg = self->get_parent()->parent;
    //auto wg = self->parent()->get_root().wg_children[(self->parent()->get_root().next_wg_++)%(self->parent()->get_root().wg_children.size())];
    std::unique_lock<std::mutex> guard(wg->lock);
    wg->queue.push_back(job);
  }

  template <class Worker>
  void internal_enqueue(Worker* self, resumable* job) {
    auto wg = self->get_parent()->parent;
    std::unique_lock<std::mutex> guard(wg->lock);
    wg->queue.push_front(job);
  }

  template <class Worker>
  void resume_job_later(Worker* self, resumable* job) {
    // job has voluntarily released the CPU to let others run instead
    // this means we are going to put this job to the very end of our queue
    auto wg = self->get_parent()->parent;
    std::unique_lock<std::mutex> guard(wg->lock);
    wg->queue.push_back(job);

//    d(self).queue.append(job);
  }

  template <class Worker>
  resumable* dequeue(Worker* self) {
    // we wait for new jobs by polling our external queue: first, we
    // assume an active work load on the machine and perform aggresive
    // polling, then we relax our polling a bit and wait 50 us between
    // dequeue attempts, finally we assume pretty much nothing is going
    // on and poll every 10 ms; this strategy strives to minimize the
    // downside of "busy waiting", which still performs much better than a
    // "signalizing" implementation based on mutexes and conition variables
    auto& strategies = d(self).strategies;
    resumable* job = nullptr;
    size_t current = 0;
    auto wg = self->get_parent()->parent;
    auto p = self->parent();
    for (auto& strat : strategies) {
      for (size_t i = 0; i < strat.attempts; i += strat.step_size) {
        {
            std::unique_lock<std::mutex> guard(wg->lock);
            if(!wg->queue.empty()){
                job = wg->queue.front();
                wg->queue.pop_front();
                return job;
            }
        }
        // try to steal every X poll attempts
        if ((i % strat.steal_interval) == 0) {
            ++self->all_steals;
            auto victim = p->get_root().wg_children[current++%8];
            {
                std::unique_lock<std::mutex> guard(victim->lock);
                if(!victim->queue.empty()){
                job = victim->queue.front();
                 victim->queue.pop_front();
                 return job;
                }
            }

          ++self->failed_steals;
        }
        if (strat.sleep_duration.count() > 0)
          std::this_thread::sleep_for(strat.sleep_duration);
      }
      ++self->repeat_steals;
    }
    // unreachable, because the last strategy loops
    // until a job has been dequeued
    return nullptr;
  }

  template <class Worker, class UnaryFunction>
  void foreach_resumable(Worker* self, UnaryFunction f) {
    auto next = [&] { return d(self).queue.take_head(); };
    for (auto job = next(); job != nullptr; job = next()) {
      f(job);
    }
  }

  template <class Coordinator, class UnaryFunction>
  void foreach_central_resumable(Coordinator*, UnaryFunction) {
    // nop
  }
};

} // namespace policy
} // namespace caf

#endif // CAF_POLICY_WORK_STEALING_HPP