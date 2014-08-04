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

#include <queue>
#include <zmq.hpp>

#include "mongo/bson/oid.h"
#include "mongo/util/concurrency/mutex.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/db/matcher/matcher.h"
#include "mongo/db/projection.h"

namespace mongo {

    typedef OID SubscriptionId;

    // contains information about a message
    class SubscriptionMessage {
    public:
        SubscriptionId subscriptionId;
        std::string channel;
        BSONObj message;
        unsigned long long timestamp;
        SubscriptionMessage(SubscriptionId _subscriptionId,
                            std::string _channel,
                            BSONObj _message,
                            unsigned long long _timestamp) {
            subscriptionId = _subscriptionId;
            channel = _channel;
            message = _message;
            timestamp = _timestamp;
        }

        friend bool operator<(const SubscriptionMessage& m1, const SubscriptionMessage& m2) {
            if (m1.subscriptionId < m2.subscriptionId)
                return true;
            if (m1.subscriptionId == m2.subscriptionId && m1.channel < m2.channel)
                return true;
            if (m1.subscriptionId == m2.subscriptionId &&
                m1.channel == m2.channel &&
                m1.timestamp > m2.timestamp)
                return true;
            return false;
        }
    };

    class PubSub {
    public:

        // outwards-facing interface for pubsub communication across replsets and clusters
        static bool publish(const string& channel, const BSONObj& message);
        static SubscriptionId subscribe(const string& channel, const BSONObj& filter,
                                        const BSONObj& projection);
        static std::priority_queue<SubscriptionMessage>
            poll(std::set<SubscriptionId>& subscriptionIds, long timeout, long long& millisPolled,
                 bool& pollAgain, std::map<SubscriptionId, std::string>& errors);
        // force is an option used internally if a poll is interrupted by an unsubscribe to 
        // allow the unsubscribe to happen without having to check the subscription in and
        // back out. it is always false if this method is called from the unsubscribe command.
        static void unsubscribe(const SubscriptionId& subscriptionId,
                                std::map<SubscriptionId, std::string>& errors,
                                bool force=false);

        // to be included in all files using the client's sub sockets
        static const char* const kIntPubSubEndpoint;

        // process-specific (mongod or mongos) initialization of internal communication sockets
        static zmq::socket_t* initSendSocket();
        static zmq::socket_t* initRecvSocket();
        static void proxy(zmq::socket_t* subscriber, zmq::socket_t* publisher);
        static void subscriptionCleanup();

        // zmq sockets for internal communication
        static zmq::context_t zmqContext;
        static zmq::socket_t intPubSocket;
        static zmq::socket_t* extRecvSocket;

    private:

        // contains information about a single subscription
        struct SubscriptionInfo {
            scoped_ptr<zmq::socket_t> sock;

            // If currently polling, all other polls return error. Set in checkoutSocket
            // (which locks the map of all subscriptions) to ensure that sockets are only
            // used by one thread at a time.
            int inUse : 1;

            // Set to indicate the subscription is invalid, and should be disposed of at the
            // next opportune time. This is needed while the subscription is being used and the
            // cleanup must wait.
            int shouldUnsub : 1;

            // Signifies that the subscription has been polled recently and is therefore
            // still alive. Used to clean up subscriptions that are abandoned.
            int polledRecently : 1;

            // Only return documents for this subscription that match this filter
            scoped_ptr<Matcher2> filter;

            // Only return the fields in each document that match the projection
            scoped_ptr<Projection> projection;
        };

        // max poll length so we can check if unsubscribe has been called
        static const long maxPollInterval;

        // data structure mapping SubscriptionId to subscription info
        static std::map<SubscriptionId, SubscriptionInfo*> subscriptions;

        // for locking around the subscriptions map in subscribe, poll, and unsubscribe
        static SimpleMutex mapMutex;

        // for locking around publish, because it uses a non-thread-safe zmq socket
        static SimpleMutex sendMutex;

        // Helper method to end all polls on subscriptions passed in. This is used in the case
        // that poll() gets cut off by an error or by hitting the max poll timeout.
        static void endCurrentPolls(std::vector<std::pair<SubscriptionId,
                                                          SubscriptionInfo*> >& subs);

        // Gets the SubscriptionInfo object for each SubscriptionId passed in. Fills in the
        // subs vector with the subscription info and the items vector with zmq::pollitem_t's
        // that correspond to those subscriptions for polling. In the event of an error finding
        // subscriptions, this method inserts an error message in the errors map for the given
        // SubscriptionId.
        static void getSubscriptions(std::set<SubscriptionId>& subscriptionIds,
                                     std::vector<zmq::pollitem_t>& items,
                                     std::vector<std::pair<SubscriptionId,
                                                           SubscriptionInfo*> >& subs,
                                     std::map<SubscriptionId, std::string>& errors);

        // Methods to check subscriptions and their corresponding sockets in and out to ensure
        // thread safe use. If you check out a socket, you are guaranteed that no other threads
        // can check out the socket until you check it back in. This is acheived by setting
        // and checkingthe bits in the SubscriptionInfo struct. If there is an error
        // checking a socket out, checkoutSocket returns NULL and sets the error message.
        static SubscriptionInfo* checkoutSocket(SubscriptionId subscriptionId, std::string& errmsg);
        static void checkinSocket(SubscriptionInfo* s);

        // This method receives messages on all subscriptions passed in. In the event of an error,
        // this method inserts an error message in the errors map for the given SubscriptionId.
        static std::priority_queue<SubscriptionMessage>
                recvMessages(std::vector<std::pair<SubscriptionId, SubscriptionInfo*> >& subs,
                             std::map<SubscriptionId, std::string>& errors);
    };

}  // namespace mongo
