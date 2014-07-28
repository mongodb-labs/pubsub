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
#include "mongo/db/pubsub_sendsock.h"

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

        // constants for field names
        const std::string kMillisPolledField = "millisPolled";
        const std::string kPollAgainField = "pollAgain";
        const std::string kMessagesField = "messages";
    }

    /**
     * Sockets for internal communication across replsets and clusters.
     *
     * Mongods in a replica set communicate directly, while mongoses in a cluster
     * communicate through the config server.
     * Further, mongod's "publish" information to each other, whereas mongoses
     * "push" information to a queue shared between the 3 config servers.
     */

    const char* const PubSub::kIntPubsubEndpoint = "inproc://pubsub";

    zmq::context_t PubSub::zmqContext(1);
    zmq::socket_t PubSub::intPubSocket(zmqContext, ZMQ_PUB);
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

    const long PubSub::maxPollInterval = 100; // milliseconds

    SimpleMutex PubSub::mapMutex("subsmap");
    SimpleMutex PubSub::sendMutex("zmqsend");

    // Outwards-facing interface for PubSub across replica sets and sharded clusters

    bool PubSub::publish(const std::string& channel, const BSONObj& message) {

        try {
            // zmq sockets are not thread-safe
            SimpleMutex::scoped_lock lk(sendMutex);
            PubSubSendSocket::extSendSocket->send(channel.c_str(), channel.size() + 1, ZMQ_SNDMORE);
            PubSubSendSocket::extSendSocket->send(message.objdata(), message.objsize());
        } catch (zmq::error_t& e) {
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
            subSocket->connect(PubSub::kIntPubsubEndpoint);
            subSocket->setsockopt(ZMQ_SUBSCRIBE, channel.c_str(), channel.length());
        } catch (zmq::error_t& e) {
            log() << "Error subscribing to channel." << causedBy(e) << endl;
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

    void PubSub::poll(std::set<SubscriptionId>& subscriptionIds, long timeout,
                      BSONObjBuilder& result, BSONObjBuilder& errors) {

        std::vector<std::pair<SubscriptionId, SubscriptionInfo*> > subs;
        std::vector<zmq::pollitem_t> items;

        PubSub::getSubscriptions(subscriptionIds, items, subs, errors);

        // if there are no valid subscriptions to check, return. there may have
        // been errors during getSubscriptions which will be returned.
        if (items.size() == 0)
            return;

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
                        errors.append(subscriptionId.toString(),
                                      "Poll interrupted by unsubscribe.");
                        items.erase(items.begin() + i);
                        subs.erase(subs.begin() + i);
                        PubSub::unsubscribe(subscriptionId, errors, true);
                        i--;
                    }
                }

                if (items.size() == 0)
                    return;

                timeout -= currPollInterval;
                pollRuntime += currPollInterval;

                // stop polling if poll has run longer than the max timeout (default ten minutes,
                // or 100 millis if debug flag is set)
                if (pollRuntime >= maxTimeoutMillis) {
                    result.append(kMillisPolledField, pollRuntime);
                    result.append(kPollAgainField, true);
                    endCurrentPolls(subs);
                    return;
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

        BSONObj messages = PubSub::recvMessages(subs);

        result.append(kMessagesField , messages);
        result.append(kMillisPolledField, pollRuntime);
    }

    void PubSub::endCurrentPolls(std::vector<std::pair<SubscriptionId, SubscriptionInfo*> >& subs) {
        for (std::vector<std::pair<SubscriptionId, SubscriptionInfo*> >::iterator subIt =
                                                                                    subs.begin();
             subIt != subs.end();
             ++subIt) {
            SubscriptionInfo* s = subIt->second;
            s->polledRecently = 1;
            s->inUse = 0;
        }
    }

    void PubSub::getSubscriptions(std::set<SubscriptionId>& subscriptionIds,
                                  std::vector<zmq::pollitem_t>& items,
                                  std::vector<std::pair<OID, SubscriptionInfo*> >& subs,
                                  BSONObjBuilder& errors) {
        SimpleMutex::scoped_lock lk(mapMutex);

        // check if each oid is for a valid subscription.
        // for each oid already in an active poll, set an errmsg
        // for each non-active oid, set active poll and create a poller
        for (std::set<SubscriptionId>::iterator it = subscriptionIds.begin();
             it != subscriptionIds.end();
             it++) {
            SubscriptionId subscriptionId = *it;
            std::map<SubscriptionId, SubscriptionInfo*>::iterator subIt =
                                                                subscriptions.find(subscriptionId);

            if (subIt == subscriptions.end() || subIt->second->shouldUnsub) {
                errors.append(subscriptionId.toString(), "Subscription not found.");
            } else if (subIt->second->inUse) {
                errors.append(subscriptionId.toString(), "Poll currently active.");
            } else {
                SubscriptionInfo* s = subIt->second;

                s->inUse = 1;

                // items and subs have corresponding info at the same indexes
                zmq::pollitem_t pollItem = { *(s->sock), 0, ZMQ_POLLIN, 0 };
                items.push_back(pollItem);
                subs.push_back(std::make_pair(subscriptionId, s));
            }
        }
    }

    BSONObj PubSub::recvMessages(std::vector<std::pair<SubscriptionId, SubscriptionInfo*> >& subs) {
        BSONObjBuilder messages;

        for (std::vector<std::pair<SubscriptionId, SubscriptionInfo*> >::iterator subIt =
                                                                                    subs.begin();
             subIt != subs.end();
             ++subIt) {
            SubscriptionInfo* s = subIt->second;

            // keep an outbox to demultiplex messages before serializing into BSON
            // first part of each message is the channel name,
            // second part is the message body (as a document)
            std::map<std::string, BSONArrayBuilder*> outbox;

            try {
                zmq::message_t msg;
                while (s->sock->recv(&msg, ZMQ_DONTWAIT)) {
                    std::string channelName = std::string(static_cast<const char*>(msg.data()));

                    // if first new message on this channel, add channel to our outbox
                    if (outbox.find(channelName) == outbox.end())
                        outbox.insert(std::make_pair(channelName, new BSONArrayBuilder()));
                    BSONArrayBuilder* arrayBuilder = outbox.find(channelName)->second;

                    // receive and save message body
                    msg.rebuild();
                    s->sock->recv(&msg);
                    BSONObj messageObject(static_cast<const char*>(msg.data()));
                    arrayBuilder->append(messageObject);

                    msg.rebuild();
                }
            } catch (zmq::error_t& e) {
                log() << "Error receiving messages from zmq socket." << causedBy(e) << endl;
                s->polledRecently = 1;
                s->inUse = 0;
                // TODO: don't uassert here or could potentially lose messages
                uassert(18548, e.what(), false);
            }

            // done receiving from ZMQ socket
            s->polledRecently = 1;
            s->inUse = 0;

            if (outbox.size() > 0) {
                // serialize the outbox into BSON
                BSONObjBuilder b;
                for (std::map<std::string, BSONArrayBuilder*>::iterator msgIt = outbox.begin();
                     msgIt != outbox.end();
                     msgIt++) {
                    b.append(msgIt->first, msgIt->second->arr());
                    delete msgIt->second;
                }

                messages.append(subIt->first.toString(), b.obj());
            }
        }

        return messages.obj();
    }

    void PubSub::unsubscribe(const SubscriptionId& subscriptionId,
                             BSONObjBuilder& errors, bool force) {
        SimpleMutex::scoped_lock lk(mapMutex);
        std::map<SubscriptionId, SubscriptionInfo*>::iterator it =
                                                                subscriptions.find(subscriptionId);

        if (it == subscriptions.end()) {
            errors.append(subscriptionId.toString(), "Subscription not found.");
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
                log() << "Error closing zmq socket." << causedBy(e) << endl;
                uassert(18549, e.what(), false);
            }

            delete s->sock;
            delete s;
            subscriptions.erase(it);
        }
    }

}  // namespace mongo
