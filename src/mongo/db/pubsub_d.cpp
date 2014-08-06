// ttl.cpp

/**
*    Copyright (C) 2008 10gen Inc.
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

#include <zmq.hpp>
#include <boost/thread.hpp>
#include "mongo/db/pubsub.h"
#include "mongo/db/pubsub_sendsock.h"
#include "mongo/db/server_options.h"
#include "mongo/util/background.h"

namespace mongo {

    class PubSubCleanup: public BackgroundJob {
    public:
        PubSubCleanup(){}
        virtual ~PubSubCleanup(){}

        virtual string name() const { return "PubSubCleanup"; }
        
        static string secondsExpireField;

        virtual void run() {

            // TODO: allow users to set pubsub ports on startup                                     
            const int port = serverGlobalParams.port;                                               
                                                                                                    
            // is publish socket regardless of if config or mongod                                  
            PubSubSendSocket::extSendSocket = PubSub::initSendSocket();                             
                                                                                                    
            // is pull socket if config, sub socket if mongod                                       
            PubSub::extRecvSocket = PubSub::initRecvSocket();                                       
                                                                                                    
            // config servers must be started with --configsvr to                                   
            // use pubsub within a sharded cluster                                                  
            if (serverGlobalParams.configsvr) {                                                     
                // each mongos pushes its messages to a queue shared between the config servers     
                // (shared queue handled automatically by zmq's push/pull sockets);                 
                // each config server atomically pulls messages from the queue and                  
                // broadcasts them to all mongoses through a zmq.publish socket.                    
                                                                                                    
                try {                                                                               
                    // listen (pull) from each mongos in the cluster                                
                    const std::string kExtPullEndpoint = str::stream() << "tcp://*:"                
                                                                       << (port + 1234);            
                    PubSub::extRecvSocket->bind(kExtPullEndpoint.c_str());                          
                                                                                                    
                    // publish to all mongoses in the cluster                                       
                    const std::string kExtPubEndpoint = str::stream() << "tcp://*:"                 
                                                                      << (port + 2345);             
                    PubSubSendSocket::extSendSocket->bind(kExtPubEndpoint.c_str());                 
                } catch (zmq::error_t& e) {                                                         
                    // TODO: turn off pubsub if connection here fails                               
                    log() << "Error initializing pubsub sockets." << causedBy(e) << endl;           
                }                                                                                   
                                                                                                    
                // automatically proxy messages from PULL endpoint to PUB endpoint                  
                boost::thread internalProxy(PubSub::proxy,                                        
                                            PubSub::extRecvSocket,                                  
                                            PubSubSendSocket::extSendSocket);                       
            } else {                                                                                                                                        
                // each mongod in a replica set publishes its messages                              
                // to all other mongods in its replica set                                          
                                                                                                    
                try {
                    // listen (subscribe) to all mongods in the replset
                    const std::string kExtSubEndpoint = str::stream() << "tcp://*:"
                                                                      << (port + 1234);
                    PubSub::extRecvSocket->bind(kExtSubEndpoint.c_str());

                    // connect to own sub socket to publish messages to self
                    // (other mongods in replset connect to our sub socket when they join the set)
                    const std::string kExtPubEndpoint = str::stream() << "tcp://localhost:"
                                                                      << (port + 1234);
                    PubSubSendSocket::extSendSocket->connect(kExtPubEndpoint.c_str());

                    // automatically proxy messages from SUB endpoint to client sub sockets
                    PubSub::intPubSocket.bind(PubSub::kIntPubSubEndpoint);
                } catch (zmq::error_t& e) {
                    // TODO: turn off pubsub if connection here fails
                    log() << "Could not initialize PubSub sockets: " << causedBy(e) << endl;
                }

                // proxy incoming messages to internal publisher to be received by clients
                boost::thread internalProxy(PubSub::proxy,
                                            PubSub::extRecvSocket,
                                            &PubSub::intPubSocket);

                // clean up subscriptions that have been inactive for at least 10 minutes
                boost::thread subscriptionCleanup(PubSub::subscriptionCleanup);

            }

        }

    };

    void startPubsubBackgroundJob() {
        PubSubCleanup* pubSubCleanup = new PubSubCleanup();
        pubSubCleanup->go();
    }    
    
}
