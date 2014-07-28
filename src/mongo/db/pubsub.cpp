/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
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

#include "mongo/pch.h"

#include "mongo/db/pubsub.h"

#include <zmq.hpp>

#include "mongo/bson/oid.h"
#include "mongo/db/instance.h"
#include "mongo/db/server_options_helpers.h"
#include "mongo/db/server_parameters.h"

namespace mongo {

    MONGO_EXPORT_SERVER_PARAMETER(useDebugTimeout, bool, false);

    namespace {
        // used as a timeout for polling and cleaning up inactive subscriptions
        long maxTimeoutMillis = 1000 * 60 * 10;
    }

    /**
     * Sockets for internal communication across replsets and clusters.
     *
     * Mongods in a replica set communicate directly, while mongoses in a cluster
     * communicate through the config server.
     * Further, mongod's "publish" information to each other, whereas mongoses
     * "push" information to a queue shared between the 3 config servers.
     */

    const char* const PubSub::kIntPubSubEndpoint = "inproc://pubsub";

    zmq::context_t PubSub::zmqContext(1);
    zmq::socket_t PubSub::intPubSocket(zmqContext, ZMQ_PUB);
    zmq::socket_t* PubSub::extSendSocket;
    zmq::socket_t* PubSub::extRecvSocket;

    zmq::socket_t* PubSub::initSendSocket() {
        zmq::socket_t* sendSocket;
        try {
            sendSocket = new zmq::socket_t(zmqContext, isMongos() ? ZMQ_PUSH : ZMQ_PUB);
        } catch (zmq::error_t& e) {
            // TODO: turn off pubsub if initialization here fails
            log() << "Error initializing zmq send socket." << causedBy(e) << endl;
        }
        return sendSocket;
    }

    zmq::socket_t* PubSub::initRecvSocket() {
        zmq::socket_t* recvSocket;
        try {
            recvSocket =
                new zmq::socket_t(zmqContext, serverGlobalParams.configsvr ? ZMQ_PULL : ZMQ_SUB);
            if (!serverGlobalParams.configsvr) {
                recvSocket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
            }
        } catch (zmq::error_t& e) {
            // TODO: turn off pubsub if initialization here fails
            log() << "Error initializing zmq recv socket." << causedBy(e) << endl;
        }
        return recvSocket;
    }

    void PubSub::proxy(zmq::socket_t* subscriber, zmq::socket_t* publisher) {
        try {
            zmq::proxy(*subscriber, *publisher, NULL);
        } catch (zmq::error_t& e) {
            // TODO: turn off pubsub if proxy here fails
            log() << "Error starting zmq proxy." << causedBy(e) << endl;
        }
    }

    // runs in a background thread and cleans up subscriptions
    // that have not been polled in at least 10 minutes
    void PubSub::subscriptionCleanup() {
        if (useDebugTimeout)
            // change timeout to 100 millis for testing
            maxTimeoutMillis = 100;
        while (true) {
            {
                SimpleMutex::scoped_lock lk(mapMutex);
                for (std::map<SubscriptionId, SubscriptionInfo*>::iterator it =
                                                                            subscriptions.begin();
                     it != subscriptions.end();
                     it++) {
                    SubscriptionInfo* s = it->second;
                    if (s->polledRecently) {
                        s->polledRecently = 0;
                    } else {
                        try {
                            s->sock->close();
                        } catch (zmq::error_t& e) {
                            log() << "Error closing zmq socket." << causedBy(e) << endl;
                        }
                        delete s->sock;
                        delete s;
                        subscriptions.erase(it);
                    }
                }
            }
            sleepmillis(maxTimeoutMillis);
        }
    }

    /**
     * In-memory data structures for pubsub.
     * Subscribers can poll for more messages on their subscribed channels, and the class
     * keeps an in-memory map of the id (cursor) they are polling on to the actual
     * poller instance used to retrieve their messages.
     *
     * The map is wrapped in a class to facilitate clean (multi-threaded) access
     * to the table from subscribe (to add entries), unsubscribe (to remove entries),
     * and poll (to access entries) without exposing any locking mechanisms
     * to the user.
     */

    std::map<SubscriptionId, PubSub::SubscriptionInfo*> PubSub::subscriptions =
                                            std::map<SubscriptionId, PubSub::SubscriptionInfo*>();

    std::map<HostAndPort, bool> PubSub::rsMembers;

    const long PubSub::maxPollInterval = 100; // milliseconds

    SimpleMutex PubSub::mapMutex("subsmap");
    SimpleMutex PubSub::sendMutex("zmqsend");

    // Outwards-facing interface for PubSub across replica sets and sharded clusters

    bool PubSub::publish(const std::string& channel, const BSONObj& message) {
        try {
            // zmq sockets are not thread-safe
            SimpleMutex::scoped_lock lk(sendMutex);
            PubSub::extSendSocket->send(channel.c_str(), channel.size() + 1, ZMQ_SNDMORE);
            PubSub::extSendSocket->send(message.objdata(), message.objsize());
        } catch (zmq::error_t& e) {
            // can't uassert here - this method is used for database events.
            // don't want a db event command to fail because pubsub doesn't work
            log() << "ZeroMQ failed to publish to pub socket." << causedBy(e) << endl;
            return false;
        }

        return true;
    }

    // TODO: add secure access to this channel?
    // perhaps return an <oid, key> pair?
    SubscriptionId PubSub::subscribe(const std::string& channel) {
        SubscriptionId subscriptionId;
        subscriptionId.init();

        zmq::socket_t* subSocket;
        try {
            subSocket = new zmq::socket_t(zmqContext, ZMQ_SUB);
            subSocket->connect(PubSub::kIntPubSubEndpoint);
            subSocket->setsockopt(ZMQ_SUBSCRIBE, channel.c_str(), channel.length());
        } catch (zmq::error_t& e) {
            uassert(18539, e.what(), false);
        }

        SubscriptionInfo* s = new SubscriptionInfo();
        s->sock = subSocket;
        s->inUse = 0;
        s->shouldUnsub = 0;
        s->polledRecently = 1;

        SimpleMutex::scoped_lock lk(mapMutex);
        subscriptions.insert(std::make_pair(subscriptionId, s));

        return subscriptionId;
    }

    std::priority_queue<SubscriptionMessage> PubSub::poll(std::set<SubscriptionId>& subscriptionIds, long timeout, long long& millisPolled, bool& pollAgain, std::map<SubscriptionId, std::string>& errors) {

        std::priority_queue<SubscriptionMessage> messages;
        std::vector<std::pair<SubscriptionId, SubscriptionInfo*> > subs;
        std::vector<zmq::pollitem_t> items;

        PubSub::getSubscriptions(subscriptionIds, items, subs, errors);

        // if there are no valid subscriptions to check, return. there may have
        // been errors during getSubscriptions which will be returned.
        if (items.size() == 0)
            return messages;

        // limit time polled to ten minutes.
        // poll for max poll interval or timeout, whichever is shorter, until
        // client timeout has passed
        if (timeout > maxTimeoutMillis || timeout < 0)
             timeout = maxTimeoutMillis;
        long long pollRuntime = 0LL;
        long currPollInterval = std::min(PubSub::maxPollInterval, timeout);

        try {
            // while no messages have been received on any of the subscriptions,
            // continue polling coming up for air in intervals to check if any of the
            // subscriptions have been canceled
            while (timeout > 0 && !zmq::poll(&items[0], items.size(), currPollInterval)) {

                for (size_t i = 0; i < subs.size(); i++) {
                    if (subs[i].second->shouldUnsub) {
                        SubscriptionId subscriptionId = subs[i].first;
                        errors.insert(std::make_pair(subscriptionId, "Poll interrupted by unsubscribe."));
                        items.erase(items.begin() + i);
                        subs.erase(subs.begin() + i);
                        PubSub::unsubscribe(subscriptionId, errors, true);
                        i--;
                    }
                }

                // If all sockets that were polling are unsubscribed, return
                if (items.size() == 0) {
                    millisPolled = pollRuntime;
                    return messages;
                }

                timeout -= currPollInterval;
                pollRuntime += currPollInterval;

                // stop polling if poll has run longer than the max timeout (default ten minutes,
                // or 100 millis if debug flag is set)
                if (pollRuntime >= maxTimeoutMillis) {
                    millisPolled = pollRuntime;
                    pollAgain = true;
                    endCurrentPolls(subs);
                    return messages;
                }

                currPollInterval = std::min(PubSub::maxPollInterval, timeout);
            }
        } catch (zmq::error_t& e) {
            log() << "Error polling on zmq socket." << causedBy(e) << endl;
            endCurrentPolls(subs);
            uassert(18547, e.what(), false);
        }

        // if we reach this point, then we know at least 1 message
        // has been received on some subscription
        messages = PubSub::recvMessages(subs, errors);

        millisPolled = pollRuntime;
        return messages;
    }

    void PubSub::endCurrentPolls(std::vector<std::pair<SubscriptionId, SubscriptionInfo*> >& subs) {
        for (std::vector<std::pair<SubscriptionId, SubscriptionInfo*> >::iterator subIt =
                                                                                    subs.begin();
             subIt != subs.end();
             ++subIt) {
            SubscriptionInfo* s = subIt->second;
            PubSub::checkinSocket(s);
        }
    }

    void PubSub::getSubscriptions(std::set<SubscriptionId>& subscriptionIds,
                                  std::vector<zmq::pollitem_t>& items,
                                  std::vector<std::pair<OID, SubscriptionInfo*> >& subs,
                                  std::map<SubscriptionId, std::string>& errors) {
        // check if each oid is for a valid subscription.
        // for each oid already in an active poll, set an error message
        // for each non-active oid, set active poll and create a poller
        for (std::set<SubscriptionId>::iterator it = subscriptionIds.begin();
             it != subscriptionIds.end();
             it++) {

            SubscriptionId subscriptionId = *it;
            std::string errmsg;
            SubscriptionInfo* s = PubSub::checkoutSocket(subscriptionId, errmsg);

            if (s == NULL) {
                errors.insert(std::make_pair(subscriptionId, errmsg));
                continue;
            }

            // create subscription info and poll item for socket.
            // items and subs have corresponding info at the same indexes
            zmq::pollitem_t pollItem = { *(s->sock), 0, ZMQ_POLLIN, 0 };
            items.push_back(pollItem);
            subs.push_back(std::make_pair(subscriptionId, s));
        }
    }

    PubSub::SubscriptionInfo* PubSub::checkoutSocket(SubscriptionId subscriptionId, std::string& errmsg) {
        SimpleMutex::scoped_lock lk(mapMutex);
        std::map<SubscriptionId, SubscriptionInfo*>::iterator subIt =
                                                                subscriptions.find(subscriptionId);

        if (subIt == subscriptions.end() || subIt->second->shouldUnsub) {
            errmsg = "Subscription not found.";
            return NULL;
        } else if (subIt->second->inUse) {
            errmsg = "Poll currently active.";
            return NULL;
        } else {
            SubscriptionInfo* s = subIt->second;
            s->inUse = 1;
            return s;
        }
    }

    void PubSub::checkinSocket(SubscriptionInfo* s) {
        s->polledRecently = 1;
        s->inUse = 0;
    }

    std::priority_queue<SubscriptionMessage> PubSub::recvMessages(std::vector<std::pair<SubscriptionId, SubscriptionInfo*> >& subs, std::map<SubscriptionId, std::string>& errors) {

        std::priority_queue<SubscriptionMessage> outbox;

        for (std::vector<std::pair<SubscriptionId, SubscriptionInfo*> >::iterator subIt =
                                                                                    subs.begin();
             subIt != subs.end();
             ++subIt) {
            SubscriptionId subscriptionId = subIt->first;
            SubscriptionInfo* s = subIt->second;

            try {
                zmq::message_t msg;
                while (s->sock->recv(&msg, ZMQ_DONTWAIT)) {
                    // receive channel
                    std::string channel = std::string(static_cast<const char*>(msg.data()));
                    msg.rebuild();

                    // receive message body
                    s->sock->recv(&msg);

                    BSONObj message(static_cast<const char*>(msg.data()));
                    message = message.getOwned();
                    msg.rebuild();

                    SubscriptionMessage m(subscriptionId, channel, message);
                    outbox.push(m);
                }
            } catch (zmq::error_t& e) {
                errors.insert(std::make_pair(subscriptionId, "Error receiving messages from zmq socket."));
            }

            // done receiving from ZMQ socket
            PubSub::checkinSocket(s);
        }

        return outbox;
    }

    void PubSub::unsubscribe(const SubscriptionId& subscriptionId,
                             std::map<SubscriptionId, std::string>& errors, bool force) {
        SimpleMutex::scoped_lock lk(mapMutex);
        std::map<SubscriptionId, SubscriptionInfo*>::iterator it =
                                                                subscriptions.find(subscriptionId);

        if (it == subscriptions.end()) {
            errors.insert(std::make_pair(subscriptionId, "Subscription not found."));
            return;
        }

        // if force unsubscribe not specified, set flag to unsubscribe when poll checks
        // active polls check frequently if unsubscribe has been noted
        SubscriptionInfo* s = it->second;
        if (!force && s->inUse) {
            s->shouldUnsub = 1;
        } else {
            try {
                s->sock->close();
            } catch (zmq::error_t& e) {
                errors.insert(std::make_pair(subscriptionId, "Error closing zmq socket."));
            }

            delete s->sock;
            delete s;
            subscriptions.erase(it);
        }
    }


    // update connections to replica set/cluster members on configuration change

    void PubSub::updateReplSetMember(HostAndPort hp) {
        std::map<HostAndPort, bool>::iterator member = rsMembers.find(hp);
        if (!hp.isSelf() && member == rsMembers.end()) {
            std::string endpoint = str::stream() << "tcp://" << hp.host()
                                                 << ":" << (hp.port() + 1234);

            try {
                SimpleMutex::scoped_lock lk(sendMutex);
                PubSub::extSendSocket->connect(endpoint.c_str());
                log() << "PubSub connected to new replica set member." << endl;
            } catch (zmq::error_t& e) {
                log() << "Error connecting to replica set member." << causedBy(e) << endl;
            }

            rsMembers.insert(std::make_pair(hp, true));
        } else {
            member->second = true;
        }
    }

    void PubSub::pruneReplSetMembers() {
        for (std::map<HostAndPort, bool>::iterator it = rsMembers.begin();
             it != rsMembers.end();
             it++) {
            if (it->second == false) {
                std::string endpoint = str::stream() << "tcp://" << it->first.host()
                                                     << ":" << (it->first.port() + 1234);

                try {
                    SimpleMutex::scoped_lock lk(sendMutex);
                    PubSub::extSendSocket->disconnect(endpoint.c_str());
                    log() << "PubSub disconnected from replica set member." << endl;
                } catch (zmq::error_t& e) {
                    log() << "Error disconnecting from replica set member." << causedBy(e) << endl;
                }

                rsMembers.erase(it);
            } else {
                it->second = false;
            }
        }
    }

}  // namespace mongo
