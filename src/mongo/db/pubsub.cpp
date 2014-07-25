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
#include "mongo/db/curop.h"
#include "mongo/db/instance.h"
#include "mongo/db/server_options_helpers.h"
#include "mongo/util/timer.h"
#include "mongo/db/server_parameters.h"

namespace mongo {

    MONGO_EXPORT_SERVER_PARAMETER(quickPubsubTimeout, bool, false);

    namespace {
        // used as a timeout for polling and cleaning up inactive subscriptions
        long pubsubMaxTimeout = 1000 * 60 * 10; // in milliseconds
    }

    /**
     * Sockets for internal communication across replsets and clusters.
     *
     * Mongods in a replica set communicate directly, while mongoses in a cluster
     * communicate through the config server.
     * Further, mongod's "publish" information to each other, whereas mongoses
     * "push" information to a queue shared between the 3 config servers.
     */

    const char* const PubSub::INT_PUBSUB_ENDPOINT = "inproc://pubsub";

    zmq::context_t PubSub::zmqContext(1);
    zmq::socket_t PubSub::intPubSocket(zmqContext, ZMQ_PUB);
    zmq::socket_t* PubSub::extSendSocket;
    zmq::socket_t* PubSub::extRecvSocket;

    zmq::socket_t* PubSub::initSendSocket() {
        zmq::socket_t* sendSocket;
        try {
            sendSocket = new zmq::socket_t(zmqContext, isMongos() ? ZMQ_PUSH : ZMQ_PUB);
        } catch (zmq::error_t& e) {
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
            log() << "Error initializing zmq recv socket." << causedBy(e) << endl;
        }
        return recvSocket;
    }

    void PubSub::proxy(zmq::socket_t* subscriber, zmq::socket_t* publisher) {
        try {
            zmq::proxy(*subscriber, *publisher, NULL);
        } catch (zmq::error_t& e) {
            log() << "Error starting zmq proxy." << causedBy(e) << endl;
        }
    }

    // runs in a background thread and cleans up subscriptions
    // that have not been polled in at least 10 minutes
    void PubSub::subscription_cleanup() {
        if (quickPubsubTimeout)
            pubsubMaxTimeout = 100;
        while (true) {
            {
                SimpleMutex::scoped_lock lk(mapMutex);
                for (std::map<OID, subInfo*>::iterator it = subscriptions.begin();
                     it != subscriptions.end();
                     it++) {
                    subInfo* s = it->second;
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
            sleepmillis(pubsubMaxTimeout);
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

    std::map<OID, PubSub::subInfo*> PubSub::subscriptions = std::map<OID, PubSub::subInfo*>();

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
            log() << "ZeroMQ failed to publish to pub socket." << causedBy(e) << endl;
            return false;
        }

        return true;
    }

    // TODO: add secure access to this channel?
    // perhaps return an <oid, key> pair?
    OID PubSub::subscribe(const std::string& channel) {
        OID oid;
        oid.init();

        zmq::socket_t* subSocket;
        try {
            subSocket = new zmq::socket_t(zmqContext, ZMQ_SUB);
            subSocket->connect(PubSub::INT_PUBSUB_ENDPOINT);
            subSocket->setsockopt(ZMQ_SUBSCRIBE, channel.c_str(), channel.length());
        } catch (zmq::error_t& e) {
            log() << "Error subscribing to channel." << causedBy(e) << endl;
            uassert(18539, e.what(), false);
        }

        subInfo* s = new subInfo();
        s->sock = subSocket;
        s->activePoll = 0;
        s->shouldUnsub = 0;
        s->polledRecently = 1;

        SimpleMutex::scoped_lock lk(mapMutex);
        subscriptions.insert(std::make_pair(oid, s));

        return oid;
    }

    void PubSub::poll(std::set<OID>& oids, long timeout,
                      BSONObjBuilder& result, BSONObjBuilder& errors) {

        std::vector<std::pair<OID, subInfo*> > subs;
        std::vector<zmq::pollitem_t> items;

        PubSub::getSubscriptions(oids, items, subs, errors);

        // if there are no valid subscriptions to check, return
        if (items.size() == 0)
            return;

        // limit time polled to ten minutes.
        // poll for max poll interval or timeout, whichever is shorter, until
        // client timeout has passed
        if (timeout > pubsubMaxTimeout || timeout < 0)
             timeout = pubsubMaxTimeout;
        long long pollRuntime = 0LL;
        long currPollInterval = (timeout > PubSub::maxPollInterval) ? PubSub::maxPollInterval
                                                                    : timeout;

        try {
            // while no messages have been received on any of the subscriptions, continue polling
            // coming up for air in intervals to check if any of the subscriptions have been canceled
            while (timeout > 0 && !zmq::poll(&items[0], items.size(), currPollInterval)) {

                for (size_t i = 0; i < subs.size(); i++) {
                    if (subs[i].second->shouldUnsub) {
                        OID oid = subs[i].first;
                        errors.append(oid.toString(), "Poll interrupted by unsubscribe.");
                        items.erase(items.begin() + i);
                        subs.erase(subs.begin() + i);
                        PubSub::unsubscribe(oid, errors, true);
                        i--;
                    }
                }

                if (items.size() == 0)
                    return;

                timeout -= currPollInterval;
                pollRuntime += currPollInterval;

                // stop polling if poll has run longer than ten minutes
                if (pollRuntime >= pubsubMaxTimeout) {
                    result.append("millisPolled", pollRuntime);
                    result.append("pollAgain", true);
                    endCurrentPolls(subs);
                    return;
                }

                currPollInterval = (timeout > PubSub::maxPollInterval) ? PubSub::maxPollInterval
                                                                       : timeout;
            }
        } catch (zmq::error_t& e) {
            log() << "Error polling on zmq socket." << causedBy(e) << endl;
            endCurrentPolls(subs);
            uassert(18547, e.what(), false);
        }

        // if we reach this point, then we know at least 1 message
        // has been received on some subscription

        BSONObj messages = PubSub::recvMessages(subs);

        result.append("messages" , messages);
        result.append("millisPolled", pollRuntime);
    }

    void PubSub::endCurrentPolls(std::vector<std::pair<OID, subInfo*> >& subs) {
        for (std::vector<std::pair<OID, subInfo*> >::iterator subIt = subs.begin();
             subIt != subs.end();
             ++subIt) {
            subInfo* s = subIt->second;
            s->polledRecently = 1;
            s->activePoll = 0;
        }
    }

    void PubSub::getSubscriptions(std::set<OID>& oids,
                                  std::vector<zmq::pollitem_t>& items,
                                  std::vector<std::pair<OID, subInfo*> >& subs,
                                  BSONObjBuilder& errors) {
        SimpleMutex::scoped_lock lk(mapMutex);

        // check if each oid is for a valid subscription.
        // for each oid already in an active poll, set an errmsg
        // for each non-active oid, set active poll and create a poller
        for (std::set<OID>::iterator it = oids.begin(); it != oids.end(); it++) {
            OID oid = *it;
            std::map<OID, subInfo*>::iterator subIt = subscriptions.find(oid);

            if (subIt == subscriptions.end() || subIt->second->shouldUnsub) {
                errors.append(oid.toString(), "Subscription not found.");
            } else if (subIt->second->activePoll) {
                errors.append(oid.toString(), "Poll currently active.");
            } else {
                subInfo* s = subIt->second;

                s->activePoll = 1;

                // items and subs have corresponding info at the same indexes
                zmq::pollitem_t pollItem = { *(s->sock), 0, ZMQ_POLLIN, 0 };
                items.push_back(pollItem);
                subs.push_back(std::make_pair(oid, s));
            }
        }
    }

    BSONObj PubSub::recvMessages(std::vector<std::pair<OID, subInfo*> >& subs) {
        BSONObjBuilder messages;

        for (std::vector<std::pair<OID, subInfo*> >::iterator subIt = subs.begin();
             subIt != subs.end();
             ++subIt) {
            subInfo* s = subIt->second;

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
                s->activePoll = 0;
                uassert(18548, e.what(), false);
            }

            // done receiving from ZMQ socket
            s->polledRecently = 1;
            s->activePoll = 0;

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

    void PubSub::unsubscribe(const OID& oid, BSONObjBuilder& errors, bool force) {
        SimpleMutex::scoped_lock lk(mapMutex);
        std::map<OID, subInfo*>::iterator it = subscriptions.find(oid);

        if (it == subscriptions.end()) {
            errors.append(oid.toString(), "Subscription not found.");
            return;
        }

        // if force unsubscribe not specified, set flag to unsubscribe when poll checks
        // active polls check frequently if unsubscribe has been noted
        subInfo* s = it->second;
        if (!force && s->activePoll) {
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

    void PubSub::viewSubscriptions(BSONObj& obj) {
        BSONObjBuilder b;
        SimpleMutex::scoped_lock lk(mapMutex);
        for (std::map<OID, subInfo*>::iterator it = subscriptions.begin();
             it != subscriptions.end();
             ++it) {
            subInfo* s = it->second;
            BSONObjBuilder info;
            info.append("activePoll", s->activePoll);
            info.append("shouldUnsub", s->shouldUnsub);
            info.append("polledRecently", s->polledRecently);
            b.append((it->first).toString(), info.obj());
        }
        obj = b.obj();
    }

    // update connections to replica set/cluster members on configuration change

    void PubSub::updateReplSetMember(HostAndPort hp) {
        std::map<HostAndPort, bool>::iterator member = rsMembers.find(hp);
        if (!hp.isSelf() && member == rsMembers.end()) {
            std::string endpoint = str::stream() << "tcp://" << hp.host()
                                                 << ":" << (hp.port() + 1234);

            sendMutex.lock();

            try {
                PubSub::extSendSocket->connect(endpoint.c_str());
                log() << "Pubsub connected to new replica set member." << endl;
            } catch (zmq::error_t& e) {
                log() << "Error connecting to replica set member." << causedBy(e) << endl;
            }

            sendMutex.unlock();

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

                sendMutex.lock();

                try {
                    PubSub::extSendSocket->disconnect(endpoint.c_str());
                    log() << "Pubsub disconnected from replica set member." << endl;
                } catch (zmq::error_t& e) {
                    log() << "Error disconnecting from replica set member." << causedBy(e) << endl;
                }

                sendMutex.unlock();

                rsMembers.erase(it);
            } else {
                it->second = false;
            }
        }
    }

}  // namespace mongo
