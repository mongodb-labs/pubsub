/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <zmq.hpp>

#include "mongo/bson/oid.h"
#include "mongo/util/concurrency/mutex.h"
#include "mongo/util/net/hostandport.h"


namespace mongo {

    class PubSub {
    public:

        // outwards-facing interface for pubsub communication across replsets and clusters
        static bool publish(const string& channel, const BSONObj& message);
        static OID subscribe(const string& channel);
        static void poll(std::set<OID>& oids, long timeout,
                         BSONObjBuilder& result, BSONObjBuilder& errors);
        static void unsubscribe(const OID& oid, BSONObjBuilder& errors, bool force=false);
        static void viewSubscriptions(BSONObj& obj);

        // to be included in all files using the client's sub sockets
        static const char* const INT_PUBSUB_ENDPOINT;

        // process-specific (mongod or mongos) initialization of internal communication sockets
        static zmq::socket_t* initSendSocket();
        static zmq::socket_t* initRecvSocket();
        static void proxy(zmq::socket_t *subscriber, zmq::socket_t *publisher);
        static void subscription_cleanup();

        // zmq sockets for internal communication
        static zmq::context_t zmqContext;
        static zmq::socket_t intPubSocket;
        static zmq::socket_t* extSendSocket;
        static zmq::socket_t* extRecvSocket;

        // connections modifiers
        static void updateReplSetMember(HostAndPort hp);
        static void pruneReplSetMembers();


    private:

        // contains information about a single subscription
        struct subInfo {
            zmq::socket_t* sock;
            // if currently polling, all other polls return error
            int activePoll;// : 1;
            // if currently polling, this bit signifies unsub has been called
            int shouldUnsub;// : 1;
            // if no activity on subscription recently, remove subscription
            int polledRecently;// : 1;
        };

        // max poll length so we can check if unsubscribe has been called
        static const long maxPollInterval;

        // data structure mapping sub_id to subscription info
        static std::map<OID, subInfo*> subscriptions;

        // for locking around the subscriptions map in subscribe, poll, and unsubscribe
        static SimpleMutex mapMutex;

        // for locking around publish, which does not affect the subscriptions map
        static SimpleMutex sendMutex;

        // list of other replica set members we are connected to for pubsub
        // only used by mongods in a replica set (not mongods as configs)
        static std::map<HostAndPort, bool> rsMembers;

        static void endCurrentPolls(std::vector<std::pair<OID, subInfo*> >& subs);

        static void getSubscriptions(std::set<OID>& oids,
                                     std::vector<zmq::pollitem_t>& items,
                                     std::vector<std::pair<OID, subInfo*> >& subs,
                                     BSONObjBuilder& errors);

        static BSONObj recvMessages(std::vector<std::pair<OID, subInfo*> >& subs);
    };

}  // namespace mongo
