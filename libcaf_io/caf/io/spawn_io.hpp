/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2015                                                  *
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

#ifndef CAF_IO_SPAWN_IO_HPP
#define CAF_IO_SPAWN_IO_HPP

#include <tuple>
#include <functional>

#include "caf/spawn.hpp"

#include "caf/io/broker.hpp"
#include "caf/io/middleman.hpp"
#include "caf/io/connection_handle.hpp"

#include "caf/io/network/native_socket.hpp"

namespace caf {
namespace io {

/// Spawns a new functor-based broker.
template <spawn_options Os = no_spawn_options,
     typename F = std::function<void(broker*)>, class... Ts>
actor spawn_io(F fun, Ts&&... xs) {
  return spawn<broker::functor_based>(std::move(fun), std::forward<Ts>(xs)...);
}

/// Spawns a new functor-based broker connecting to `host:port.
template <spawn_options Os = no_spawn_options,
      typename F = std::function<void(broker*)>, class... Ts>
actor spawn_io_client(F fun, const std::string& host,
                      uint16_t port, Ts&&... xs) {
  // provoke compiler error early
  using fun_res = decltype(fun(static_cast<broker*>(nullptr),
                               connection_handle{},
                               std::forward<Ts>(xs)...));
  // prevent warning about unused local type
  static_assert(std::is_same<fun_res, fun_res>::value,
                "your compiler is lying to you");
  // works around an issue with GCC 4.8 that could not handle
  // variadic template parameter packs inside lambdas
  auto args = std::forward_as_tuple(std::forward<Ts>(xs)...);
  return spawn_class<broker::functor_based>(
        nullptr,
        [&](broker::functor_based* ptr) {
          auto mm = middleman::instance();
          auto hdl = mm->backend().add_tcp_scribe(ptr, host, port);
          broker::functor_based::initializer init;
          detail::apply_args_prefixed(init, detail::get_indices(args), args,
                                      ptr, std::move(fun), std::move(hdl));
        });
}

/// Spawns a new broker as server running on given `port`.
template <spawn_options Os = no_spawn_options,
          class F = std::function<void(broker*)>, class... Ts>
actor spawn_io_server(F fun, uint16_t port, Ts&&... xs) {
  // same workaround as above
  auto args = std::forward_as_tuple(std::forward<Ts>(xs)...);
  return spawn_class<broker::functor_based>(
        nullptr,
        [&](broker::functor_based* ptr) {
          auto mm = middleman::instance();
          mm->backend().add_tcp_doorman(ptr, port);
          broker::functor_based::initializer init;
          detail::apply_args_prefixed(init, detail::get_indices(args), args,
                                      ptr, std::move(fun));
        });
}

} // namespace io
} // namespace caf

#endif // CAF_IO_SPAWN_IO_HPP
