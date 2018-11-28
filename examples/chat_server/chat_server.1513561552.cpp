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

std::vector<caf::group> groups;
std::vector<actor> actors;
class config : public actor_system_config {
 public:
    uint64_t sessions = 100000;
    uint64_t maxgroups = 1000;
    uint64_t maxgroupmem = 5;
    uint64_t maxfriends = 100;
    uint64_t maxblocked = 5;
    uint64_t totalmessages = 1000;

    config() {
        opt_group{custom_options_, "global"}
                .add(sessions, "sessions,s", "set number of sessions")
                .add(maxgroups, "maxgroups,g", "set how max sessions per group")
                .add(maxgroupmem, "maxgroupmem,m", "set how many groups each session is a member of")
                .add(maxfriends, "maxfriends,f", "set max number of friends each user has")
                .add(maxblocked, "maxblocked,b", "set max number of blocked friends each user has")
                .add(totalmessages, "totalmessages,t", "total messages each actor sends before quit");

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

template <typename T>
struct result_msg {
    actor sender;
    uint64_t key;
    T value;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msg<actor>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msg<caf::group>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msg<chat_msg>);

template <typename T>
struct result_msgs{
    actor sender;
    vector<result_msg<T>> log;
};
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msgs<actor>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msgs<caf::group>);
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(result_msgs<chat_msg>);

enum nonvalid_types{
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

using generate_work_msg_atom = atom_constant<atom("genwork")>;
using get_log_msg_atom = atom_constant<atom("getlog")>;
using end_work_msg_atom = atom_constant<atom("endwork")>;


vector<std::string> str_messages;
/******** Actors *******************/

template <typename T>
class DictionaryActor : public caf::event_based_actor{
        std::unordered_map<uint64_t, T> _dict;

public:
    DictionaryActor(caf::actor_config &cfg): caf::event_based_actor(cfg){};
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
                    if(_dict.size() == 0){
                        send(log_msg.sender, result_nonvalid_msg{actor_cast<actor>(this), 0, LOGGER_DICT});
                    }else{
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

behavior logger_func(stateful_actor<vector<time_msg>>* self){
    return {
        [=](time_msg &msg){
            self->state.push_back(msg);
        },
        [=](end_work_msg_atom){
            float sum = 0;
            for(auto t: self->state){
                auto avg =  std::chrono::duration_cast<std::chrono::nanoseconds>(t.received-t.sent).count();
                cout << avg << endl;
                sum += avg;
            }
            cout << endl;
            cout << sum/self->state.size() << endl;
            self->quit();
            exit(0);
        }
    };
}

class God : public caf::event_based_actor{
    uint64_t numFinshiedActor;
    uint64_t _sessions;

    actor session_registry;
    actor group_registry;
    actor logger;

 public:
    God(caf::actor_config &cfg, uint64_t sessions, actor session_reg, actor loger, actor group_reg) : event_based_actor(cfg),
                              numFinshiedActor(0), _sessions(sessions), session_registry(session_reg), group_registry(group_reg), logger(loger){};


    caf::behavior make_behavior() override {
        return {
                [=](end_work_msg_atom){
                    std::cerr << "Finished" << std::endl;
                    send(logger, end_work_msg_atom::value);
                    //send(session_registry, get_log_msg{actor_cast<actor>(this)});
                    for(auto a : actors){
                        send(a, end_work_msg_atom::value);
                    }
                    send(session_registry, end_work_msg_atom::value);
                    quit();
                }
        };
    }
};

class Session : public caf::event_based_actor {
    uint64_t _id;

    std::vector<uint64_t> _friends;
    std::vector<uint64_t> _groups;
    std::vector<uint64_t> _blocked;

    // local cache to map ids to actors
    std::unordered_map<uint64_t, actor> _cache;
    std::unordered_map<uint64_t, caf::group> _cache_group;

   // Session registry
    actor session_registry;
    actor group_registry;
    actor local_logger;
    actor logger;


    uint64_t message_counter;
    std::chrono::high_resolution_clock::time_point start;
    std::string key;

    void iterateThroughChatMessage(std::string &msg){
        char tmp, final;
        for(char& c : msg) {
            tmp = c;
            tmp++;
        }
        final = tmp;
    }
    // key is now const&
    std::string encrypt(std::string msg, std::string const& key)
    {
        // Side effects if the following is not written:
        // In my case, division by 0.
        // In the other case, stuck in an infinite loop.
        if(key.empty())
            return msg;

        for (std::string::size_type i = 0; i < msg.size(); ++i)
            msg[i] ^= key[i%key.size()];
        return msg;
    }

// Rewritten to use const& on both parameters
    std::string decrypt(std::string const& msg, std::string const& key)
    {
        return encrypt(msg, key); // lol
    }

 public:
    Session(caf::actor_config &cfg, int id, std::vector<uint64_t> friends, std::vector<uint64_t> groupsid, vector<uint64_t> blocked, actor session_reg, actor loger, actor group_reg)
            : caf::event_based_actor(cfg),
              _id(id), _friends(friends), _groups(groupsid), _blocked(blocked), session_registry(session_reg), group_registry(group_reg),
              logger(loger), message_counter(0),
            key(std::to_string(_id)){
            local_logger = spawn<DictionaryActor<chat_msg>>();
            for(auto &id : groupsid){
                auto grp = groups[id];
                 this->join(grp);
            }
    };

    caf::behavior make_behavior() override {
        return {
                [=](result_msg<actor> &res_msg){
                    if(!res_msg.value) {
                        std::cout << "ERRROR" << std::endl;
                    }else {
                        _cache[res_msg.key] = res_msg.value;
                        send(this, generate_work_msg_atom::value);
                    }
                },
                [=](result_msg<caf::group> &res_msg){
                    if(!res_msg.value) {
                        std::cout << "ERRROR" << std::endl;
                    }else {
                        _cache_group[res_msg.key] = res_msg.value;
                        send(this, generate_work_msg_atom::value);
                    }
                },
                [=](result_nonvalid_msg &res_msg){
                    if(res_msg.type == ID_DICT) {
                        cout << "NONVALID USER ID !!!!!!!!!!!!!!!!!!!!!!!!!" << endl;
                        send(this, generate_work_msg_atom::value);
                    }
                },
                [=](get_log_msg_atom) {
                    send(local_logger, get_log_msg{actor_cast<actor>(this)});
                    delayed_send(this, std::chrono::milliseconds(myrand()%5000), get_log_msg_atom::value);
                },
                [=](generate_work_msg_atom) {
                   int coin = myrand()%20;
                    if(coin) {
                        uint64_t dst_id = _friends[myrand() % _friends.size()];
                        auto it = _cache.find(dst_id);

                        if (it == end(_cache)) {
                            this->send(session_registry, read_msg{this, dst_id});
                            return;
                        } else {
                            std::string msg = str_messages[myrand() % str_messages.size()];
                            std::string nkey(key);
                            while (nkey.size() < msg.size())
                                nkey += key;

                            msg = encrypt(msg, nkey);
                            this->send(it->second, chat_msg{_id, dst_id, false, msg, nkey,
                                                            std::chrono::high_resolution_clock::now()});
                            this->send(local_logger, write_msg<chat_msg>{actor_cast<actor>(this), _id,
                                                                         chat_msg{_id, dst_id, false, msg, nkey,
                                                                                  std::chrono::high_resolution_clock::now()}});
                       }

                    }else{
                        uint64_t dst_id = _groups[myrand() % _groups.size()];
                        auto it = _cache_group.find(dst_id);

                        if (it == end(_cache_group)) {
                            this->send(group_registry, read_msg{this, dst_id});
                            return;
                        } else {
                            std::string msg = str_messages[myrand() % str_messages.size()];
                            std::string nkey(key);
                            while (nkey.size() < msg.size())
                                nkey += key;

                            msg = encrypt(msg, nkey);
                            this->send(it->second, chat_msg{_id, dst_id, false, msg, nkey,
                                                            std::chrono::high_resolution_clock::now()});
                            this->send(local_logger, write_msg<chat_msg>{actor_cast<actor>(this), _id,
                                                                         chat_msg{_id, dst_id, false, msg, nkey,
                                                                                  std::chrono::high_resolution_clock::now()}});
                        }
                    }
                    delayed_send(this, std::chrono::milliseconds(myrand() % 1000), generate_work_msg_atom::value);
                },
                [=](result_msgs<chat_msg> res_msgs){
                    for( auto msg : res_msgs.log) {
                        iterateThroughChatMessage(msg.value.msg);
                    }
                },
                [=](chat_msg &chat_message) {
                    auto b_it = find(_blocked.begin(), _blocked.end(), chat_message.sender_id);
                    if(b_it == end(_blocked)){
                        std::string msg = decrypt(chat_message.msg, chat_message.key);
                        iterateThroughChatMessage(msg);
                        send(logger, time_msg{chat_message.sent, std::chrono::high_resolution_clock::now()});
                   }

                },
                [=](end_work_msg_atom) {
                    send(local_logger, end_work_msg_atom::value);
                    quit();
                }
        };
    }
};

void run_server(actor_system &system, const config &cfg) {
    scoped_actor self{system};
    auto session_registry = system.spawn<DictionaryActor<actor>>();
    auto group_registry = system.spawn<DictionaryActor<caf::group>>();
    auto logger = system.spawn(logger_func);
    auto god = system.spawn<God>(cfg.sessions, session_registry, logger, group_registry);

    actors.reserve(cfg.sessions);

    // Generate Messages
    std::string msg = "";
    for(size_t i = 1 ; i < 1024; i++){
        msg += 'H';
        str_messages.push_back(msg);
    }

    groups.reserve(cfg.maxgroups);
    std::string module = "local";
    for(size_t i = 0 ; i < cfg.maxgroups; i++){
        std::string id = "grp" + std::to_string(i);
        auto grp = system.groups().get_local(id);
        self->send(group_registry, write_msg<caf::group>{self, i, grp});
        groups.push_back(grp);
    }

    // Generating session actors, with their friends and groups
    // Also register the actor with the central registry
    for (size_t i = 0; i < cfg.sessions; i++) {

        std::vector<uint64_t> friends;
        size_t size = 0;
        while(size <= 0)
            size = myrand() % cfg.maxfriends;
        friends.reserve(size);
        for (size_t j = 0; j < size; j++) {
            uint64_t fid = myrand() % cfg.sessions;
            if (fid != i) friends.push_back(fid);
            else j--;
        }

        std::vector<uint64_t> groups;
        while(size <= 0)
            size = myrand() % cfg.maxgroupmem;
        groups.reserve(size);
        for (size_t j = 0; j < size; j++) {
            uint64_t gid = myrand() % cfg.maxgroups;
            groups.push_back(gid);
        }

        std::vector<uint64_t> blocked;
        while(size <= 0)
            size = myrand() % cfg.maxblocked;
        blocked.reserve(size);
        for(size_t j = 0 ; j < size; j++){
            uint64_t bid = myrand() % cfg.sessions;
            if(bid != i) blocked.push_back(bid);
            else j--;
        }

        auto a = system.spawn<Session>(i, friends, groups, blocked, session_registry, logger, group_registry);
        self->send(session_registry, write_msg<actor>{self, i, a});

        actors.push_back(a);
    }


    // now tell the actors to
    for (auto a : actors) {
       self->send(a, generate_work_msg_atom::value);
        self->send(a, get_log_msg_atom::value);
    }
    std::cerr << "Started" << std::endl;
    self->delayed_send(god, std::chrono::milliseconds(TOTAL_MESSAGES), end_work_msg_atom::value);

}

void caf_main(actor_system &system, const config &cfg) {
    TOTAL_MESSAGES = cfg.totalmessages;
    run_server(system, cfg);
}
CAF_MAIN();