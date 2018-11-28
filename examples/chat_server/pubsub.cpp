//
// Created by saman on 14/12/17.
//
#include <set>
#include <map>
#include <vector>
#include <sstream>
#include <iostream>
#include <unordered_map>
#include <chrono>

#include "caf/all.hpp"
#include "caf/io/all.hpp"


using namespace std;
using namespace caf;

uint64_t TOTAL_MESSAGES = 120000;
uint64_t TOTAL_SESSIONS = 0;
uint64_t CREATORS = 64;
uint64_t NUM_SENDERS = 8;

std::vector<caf::group> groups;
std::mutex actors_mutex;
std::vector<std::vector<actor>> actors;
std::vector<actor> main_logger;
actor god;
// actor group_registry;
actor printer;


std::chrono::system_clock::time_point begin_time;
std::chrono::system_clock::time_point creation_time;
std::chrono::system_clock::time_point start_time;
std::chrono::system_clock::time_point end_time;

class config : public actor_system_config {
 public:
    uint64_t sessions = 100000;
    uint64_t maxgroups = 1000;
    uint64_t maxgroupmem = 5;
    uint64_t maxfriends = 100;
    uint64_t maxblocked = 5;
    uint64_t totalmessages = 1000;
    uint64_t num_creators = 64;
    uint64_t num_senders = 64;

    config() {
        opt_group{custom_options_, "global"}
                .add(sessions, "sessions,s", "set number of sessions")
                .add(maxgroups, "maxgroups,g", "set how max sessions per group")
                .add(maxgroupmem, "maxgroupmem,m", "set how many groups each session is a member of")
                .add(maxfriends, "maxfriends,f", "set max number of friends each user has")
                .add(maxblocked, "maxblocked,b", "set max number of blocked friends each user has")
                .add(totalmessages, "totalmessages,t", "total messages each actor sends before quit")
                .add(num_creators, "num_creators,c", "number of creator actors")
                .add(num_senders, "num_senders,d", "number of senders");
    }
};

/******* Utils ******************/
static unsigned long x = 123456789, y = 362436069, z = 521288629;

unsigned long myrand(void) {          //period 2^96-1
    unsigned long t;
    x ^= x << 16;
    x ^= x >> 5;
    x ^= x << 1;

    t = x;
    x = y;
    y = z;
    z = t ^ x ^ y;

    return z;
};

/******* Message Types **********/
struct chat_msg {
    uint64_t sender_id;
    uint64_t receiver_id;
    bool is_group;
    std::string msg;
    std::string key;
    std::chrono::high_resolution_clock::time_point sent;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(chat_msg);

struct time_msg {
    std::chrono::high_resolution_clock::time_point sent;
    std::chrono::high_resolution_clock::time_point received;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(time_msg);

template<typename T>
struct write_msg {
    actor sender;
    uint64_t key;
    T value;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(write_msg<actor>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(write_msg<caf::group>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(write_msg<chat_msg>);

struct read_msg {
    actor sender;
    uint64_t key;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(read_msg);

template<typename T>
struct result_msg {
    actor sender;
    uint64_t key;
    T value;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msg<actor>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msg<caf::group>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msg<chat_msg>);

template<typename T>
struct result_msgs {
    actor sender;
    vector<result_msg<T>> log;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msgs<actor>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msgs<caf::group>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msgs<chat_msg>);

enum nonvalid_types {
    ID_DICT,
    LOGGER_DICT
};
struct result_nonvalid_msg {
    actor sender;
    uint64_t key;
    uint64_t type;

};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_nonvalid_msg);

struct get_log_msg {
    actor sender;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(get_log_msg);

using start_work_msg_atom = atom_constant<atom("startwork")>;
using generate_work_msg_atom = atom_constant<atom("genwork")>;
using get_log_msg_atom = atom_constant<atom("getlog")>;
using end_work_msg_atom = atom_constant<atom("endwork")>;
using print_msg_atom = atom_constant<atom("print")>;


vector<std::string> str_messages;
vector<vector<time_msg>> logs;

/******** Actors *******************/

template<typename T>
class DictionaryActor : public caf::event_based_actor {
    std::unordered_map<uint64_t, T> _dict;

 public:
    DictionaryActor(caf::actor_config &cfg) : caf::event_based_actor(cfg) {};

    caf::behavior make_behavior() override {
        return {
                [=](write_msg<T> &write_message) {
                    auto &key = write_message.key;
                    auto &value = write_message.value;
                    _dict[key] = value;
                    // auto &sender = write_message.sender;
                    // self->send(sender, result_msg{actor_cast<actor>(self), value});
                },
                [=](read_msg &read_message) {
                    auto it = _dict.find(read_message.key);
                    auto &sender = read_message.sender;
                    if (it != end(_dict)) {
                        this->send(sender, result_msg<T>{actor_cast<actor>(this), read_message.key, it->second});
                    } else {
                        this->send(sender, result_nonvalid_msg{actor_cast<actor>(this), read_message.key, ID_DICT});
                    }
                },
                [=](get_log_msg &log_msg) {
                    if (_dict.size() == 0) {
                        send(log_msg.sender, result_nonvalid_msg{actor_cast<actor>(this), 0, LOGGER_DICT});
                    } else {
                        std::vector<result_msg<T>> messages;
                        messages.reserve(_dict.size());
                        for (auto it : _dict)
                            messages.emplace_back(result_msg<T>{actor_cast<actor>(this), it.first, it.second});
                        this->send(log_msg.sender, result_msgs<T>{actor_cast<actor>(this), messages});
                    }
                },
                [=](end_work_msg_atom) {
                    this->quit();
                }
        };
    }
};
struct printer_struct{
   uint64_t counter = 0;
};
behavior printer_func(stateful_actor<printer_struct> * self) {
   return {
        [=](generate_work_msg_atom, uint64_t id){
                size_t size = logs[id].size();
                for (size_t i = 0 ; i < size; i++) {
                    auto t = logs[id][i];
                    auto avg = std::chrono::duration_cast<std::chrono::nanoseconds>(t.received - t.sent).count();
                    cout << avg << endl;
                }
            if(++self->state.counter == CREATORS)
                exit(0);
        }
   };
}
atomic<bool> isFinished(false);
atomic<bool> isStarted(false);
behavior logger_func(stateful_actor<uint64_t> *self) {
    return {
            [=](generate_work_msg_atom, uint64_t id){
                self->state = id;
            },
            [=](time_msg &msg) {
                if(!isFinished.load() && isStarted.load())
                    logs[self->state].push_back(msg);
            },
            [=](end_work_msg_atom) {
                isFinished.store(true);
                self->send(printer, generate_work_msg_atom::value, self->state);
               self->quit();
            }
    };
}

class God : public caf::event_based_actor {
    uint64_t numFinshiedActor;
    uint64_t _sessions;
 public:
    God(caf::actor_config &cfg, uint64_t sessions) : event_based_actor(cfg),
                                                     numFinshiedActor(0), _sessions(sessions) {};


    caf::behavior make_behavior() override {
        return {
                [=](end_work_msg_atom) {
                     end_time = std::chrono::system_clock::now();
                     std::cerr << "Finished and took " << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time ).count() << "ms" << std::endl;

                    //send(session_registry, get_log_msg{actor_cast<actor>(this)});
                    for (auto ac : actors) {
                        for(auto a : ac)
                            send(a, end_work_msg_atom::value);
                    }
                    // send(session_registry, end_work_msg_atom::value);
                    quit();
                }
        };
    }
};

class Session : public caf::event_based_actor {
    uint64_t _id;

    // Session registry
    actor _logger;


    uint64_t message_counter;
    std::chrono::high_resolution_clock::time_point start;
    std::string key;

    bool isDone = false;
    void iterateThroughChatMessage(std::string &msg) {
        char tmp, final;
        for (char &c : msg) {
            tmp = c;
            tmp++;
        }
        final = tmp;
    }

 public:
    Session(caf::actor_config &cfg, int id, std::vector<uint64_t> groupsid)
            : caf::event_based_actor(cfg),
              _id(id),
              message_counter(0),
              key(std::to_string(_id)) {
        for (auto &id : groupsid) {
            auto grp = groups[id];
            this->join(grp);
        }
        _logger = main_logger[id%CREATORS];
    };

    caf::behavior make_behavior() override {
        return {
               [=](chat_msg &chat_message) {
                    if(isDone) return;
                    iterateThroughChatMessage(chat_message.msg);
                    send(_logger, time_msg{chat_message.sent, std::chrono::high_resolution_clock::now()});
                },
                [=](end_work_msg_atom) {
                    isDone = true;
                    quit();
                }
        };
    }
};

behavior sender_func(stateful_actor<uint64_t>* self){
   return {
    [=](generate_work_msg_atom, size_t index){
        size_t size = (groups.size()/NUM_SENDERS);
        size_t start = index * size;
        for(size_t i = start; i < start + size; i++){
            auto g = groups[i];
            std::string msg = str_messages[myrand() % str_messages.size()];
            self->send(g, chat_msg{1, 1, false, msg, "",
                                        std::chrono::high_resolution_clock::now()});
        }
        if(!isFinished.load())
            self->delayed_send(self,std::chrono::milliseconds(100), generate_work_msg_atom::value, index);
        else
            self->quit();
    }
   };
}

atomic<uint64_t> actor_counter(0);
class Creator : public event_based_actor {
    actor_system &_system;
    const config &_cfg;
    uint64_t _index;
    vector<actor> _actors;
 public:
    Creator(actor_config &cfg, actor_system &system, const config &ascfg, uint64_t index) : event_based_actor(cfg), _cfg(ascfg), _system(system), _index(index) {};

    behavior make_behavior() override {
        return {
                [=](generate_work_msg_atom) {
                    // Generating session actors, with their friends and groups
                    // Also register the actor with the central registry
                    _actors.reserve(_cfg.sessions / _cfg.num_creators);
                    for (size_t i = 0; i < _cfg.sessions / _cfg.num_creators; i++) {
                        std::vector<uint64_t> groups;
                        size_t size = 0;
                        while (size <= 0)
                            size = myrand() % _cfg.maxgroupmem;
                        groups.reserve(size);
                        for (size_t j = 0; j < size; j++) {
                            uint64_t gid = myrand() % _cfg.maxgroups;
                            groups.push_back(gid);
                        }

                        auto a = _system.spawn<Session>((_cfg.sessions / _cfg.num_creators) * _index + i,
                                                        groups);
                        //send(session_registry,
                        //     write_msg<actor>{this, (_cfg.sessions / _cfg.num_creators) * _index + i, a});
                        //_actors.push_back(a);
                    }
                    /*{
                        std::lock_guard<std::mutex> lock(actors_mutex);
                        actors.push_back(_actors);
                    }*/
                    if (++actor_counter == CREATORS) {
                        creation_time = std::chrono::system_clock::now();
                        std::cerr << "Created and took " << std::chrono::duration_cast<std::chrono::milliseconds>(
                                creation_time - begin_time).count() << "ms" << std::endl;
                        for (size_t i = 0 ; i < NUM_SENDERS; i++) {
                            auto sender = _system.spawn(sender_func);
                            send(sender, generate_work_msg_atom::value, i);
                        }
                        /* for (auto ac: actors) {
                            for (auto a: ac)
                                send(a, generate_work_msg_atom::value);
                        }*/
                        // auto sender = spawn(sender_func);
                        // send(sender, generate_work_msg_atom::value);

                        start_time = std::chrono::system_clock::now();
                        std::cerr << "Started and took " << std::chrono::duration_cast<std::chrono::milliseconds>(
                                start_time - creation_time).count() << "ms" << std::endl;
                        isStarted.store(true);
                        delayed_send(god, std::chrono::milliseconds(TOTAL_MESSAGES), end_work_msg_atom::value);
                        for (size_t i = 0; i < CREATORS; i++)
                            delayed_send(main_logger[i], std::chrono::milliseconds(TOTAL_MESSAGES - 5),
                                         end_work_msg_atom::value);
                    }
                }
        };
    };
};

atomic<uint64_t> group_counter(0);
class GroupCreator : public event_based_actor {
    actor_system &_system;
    const config &_cfg;
    uint64_t _index;

 public:
    GroupCreator(actor_config &cfg, actor_system &system, const config &ascfg, uint64_t i):
            event_based_actor(cfg), _system(system), _cfg(ascfg), _index(i){};
    behavior make_behavior() override {
        return{
            [=](generate_work_msg_atom) {
                std::string module = "local";
                for (size_t i = 0; i < _cfg.maxgroups/_cfg.num_creators; i++) {
                    std::string id = "grp" + std::to_string((_cfg.maxgroups / _cfg.num_creators) * _index +i);
                    auto grp = _system.groups().get_local(id);
                    // send(group_registry, write_msg<caf::group>{actor_cast<actor>(this), (_cfg.maxgroups / _cfg.num_creators) * _index +i, grp});
                    groups.push_back(grp);
                }
                group_counter++;
                if(group_counter.load() == _cfg.num_creators){
                    cerr << "Groups Created" << endl;
                    for(uint64_t i = 0 ; i < _cfg.num_creators; i++){
                        auto creator = _system.spawn<Creator>(_system,_cfg, i);
                        send(creator, generate_work_msg_atom::value);
                    }
                }
            }
        };

    };
};

void run_server(actor_system &system, const config &cfg) {
    scoped_actor self{system};
    //session_registry = system.spawn<DictionaryActor<actor>>();
    // group_registry = system.spawn<DictionaryActor<caf::group>>();
    god = system.spawn<God>(cfg.sessions);
    printer = system.spawn(printer_func);


    for(uint64_t i = 0 ; i < cfg.num_creators; i++){
       vector<time_msg> l;
       l.reserve(100000);
       logs.push_back(l);
        auto a = system.spawn(logger_func);
        main_logger.push_back(a);
        self->send(a, generate_work_msg_atom::value, i);
    }

    actors.reserve(cfg.sessions);

    // Generate Messages
    std::string msg = "";
    for (size_t i = 1; i < 1024; i++) {
        msg += 'H';
        str_messages.push_back(msg);
    }

    groups.reserve(cfg.maxgroups);

    for(uint64_t i = 0 ; i < cfg.num_creators; i++){
        auto creator = system.spawn<GroupCreator>(system, cfg, i);
        self->send(creator, generate_work_msg_atom::value);
    };
}

void caf_main(actor_system &system, const config &cfg) {
    TOTAL_MESSAGES = cfg.totalmessages;
    TOTAL_SESSIONS = cfg.sessions;
    CREATORS = cfg.num_creators;
    NUM_SENDERS = cfg.num_creators;
    begin_time = std::chrono::system_clock::now();
    run_server(system, cfg);
}

CAF_MAIN();