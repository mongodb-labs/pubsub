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

#include "mongo/pch.h"

#include "mongo/db/pubsub_sendsock.h"

#include <zmq.hpp>

#include "mongo/util/stringutils.h"

namespace mongo {

    SimpleMutex PubSubSendSocket::sendMutex("zmqsend");

    zmq::context_t PubSubSendSocket::zmqContext(1);
    zmq::socket_t* PubSubSendSocket::extSendSocket = NULL;
    zmq::socket_t* PubSubSendSocket::dbEventSocket = NULL;
    std::map<HostAndPort, bool> PubSubSendSocket::rsMembers;

    bool PubSubSendSocket::publish(const std::string& channel, const BSONObj& message) {

        unsigned long long timestamp = curTimeMicros64();
        try {
            // zmq sockets are not thread-safe
            SimpleMutex::scoped_lock lk(sendMutex);

            // dbEventSocket is non-null iff mongod is in a sharded environment
            // workaround to compile on mongos without including d_logic.cpp
            if (!serverGlobalParams.configsvr &&
                dbEventSocket != NULL &&
                StringData(channel).startsWith("$events")) {
                // only publish database events to config servers
                dbEventSocket->send(channel.c_str(), channel.size() + 1, ZMQ_SNDMORE);
                dbEventSocket->send(message.objdata(), message.objsize(), ZMQ_SNDMORE);
                dbEventSocket->send(&timestamp, sizeof(timestamp));
            }

            // publications and writes to config servers are published normally
            extSendSocket->send(channel.c_str(), channel.size() + 1, ZMQ_SNDMORE);
            extSendSocket->send(message.objdata(),
                                                  message.objsize(),
                                                  ZMQ_SNDMORE);
            extSendSocket->send(&timestamp, sizeof(timestamp));
        }
        catch (zmq::error_t& e) {
            // can't uassert here - this method is used for database events.
            // don't want a db event command to fail because pubsub doesn't work
            log() << "ZeroMQ failed to publish to pub socket." << causedBy(e);
            return false;
        }

        return true;
    }

    void PubSubSendSocket::initSharding(const std::string configServers) {
        vector<string> configdbs;
        splitStringDelim(configServers, &configdbs, ',');

        // find config db we are using for pubsub
        HostAndPort maxConfigHP;
        maxConfigHP.setPort(0);

        for (vector<string>::iterator it = configdbs.begin(); it != configdbs.end(); it++) {
            HostAndPort configHP = HostAndPort(*it);
            if (configHP.port() > maxConfigHP.port())
                maxConfigHP = configHP;
        }

        HostAndPort configPullEndpoint = HostAndPort(maxConfigHP.host(), maxConfigHP.port() + 1234);

        try {
            dbEventSocket = new zmq::socket_t(zmqContext, ZMQ_PUSH);
            dbEventSocket->connect(("tcp://" + configPullEndpoint.toString()).c_str());
        }
        catch (zmq::error_t& e) {
            // TODO: something more drastic than logging
            log() << "Could not connect to config server." << causedBy(e) << endl;
        }

    }

    void PubSubSendSocket::updateReplSetMember(HostAndPort hp) {
        std::map<HostAndPort, bool>::iterator member = rsMembers.find(hp);
        if (!hp.isSelf() && member == rsMembers.end()) {
            std::string endpoint = str::stream() << "tcp://" << hp.host()
                                                 << ":" << hp.port() + 1234;

            try {
                extSendSocket->connect(endpoint.c_str());
            }
            catch (zmq::error_t& e) {
                log() << "Error connecting to replica set member." << causedBy(e);
            }

            // don't need to lock around the map because this is called from a locked context
            rsMembers.insert(std::make_pair(hp, true));
        }
        else {
            member->second = true;
        }
    }

    void PubSubSendSocket::pruneReplSetMembers() {
        for (std::map<HostAndPort, bool>::iterator it = rsMembers.begin();
             it != rsMembers.end();
             it++) {
                if (it->second == false) {
                    std::string endpoint = str::stream() << "tcp://" << it->first.host()
                                                         << ":" << it->first.port() + 1234;

                    try {
                        extSendSocket->disconnect(endpoint.c_str());
                    }
                    catch (zmq::error_t& e) {
                        log() << "Error disconnecting from replica set member." << causedBy(e);
                    }

                    // don't need to lock around the map because
                    // this is called from a locked context
                    rsMembers.erase(it);
                }
                else {
                    it->second = false;
                }
        }
    }

}  // namespace mongo
