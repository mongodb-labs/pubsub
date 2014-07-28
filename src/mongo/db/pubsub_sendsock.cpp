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

#include <zmq.hpp>

#include "mongo/db/pubsub_sendsock.h"
#include "mongo/base/init.h"

namespace mongo {

    zmq::socket_t* PubSubSendSocket::extSendSocket; 
    std::map<HostAndPort, bool> PubSubSendSocket::rsMembers; 

    void PubSubSendSocket::updateReplSetMember(HostAndPort hp) {
        std::map<HostAndPort, bool>::iterator member = rsMembers.find(hp);                          
        if (!hp.isSelf() && member == rsMembers.end()) {                                            
            std::string endpoint = str::stream() << "tcp://" << hp.host()                           
                                                 << ":" << (hp.port() + 1234);                      
                                                                                                    
            try {                                                                                   
                extSendSocket->connect(endpoint.c_str());                                   
                log() << "Pubsub connected to new replica set member." << endl;                     
            } catch (zmq::error_t& e) {                                                             
                log() << "Error connecting to replica set member." << causedBy(e) << endl;          
            }                                                                                       

            // don't need to lock around the map because this is called from a locked context  
            rsMembers.insert(std::make_pair(hp, true));                                             
        } else {                                                                                    
            member->second = true;                                                                  
        }                                                                                           
    }      

    void PubSubSendSocket::pruneReplSetMembers() {
        for (std::map<HostAndPort, bool>::iterator it = rsMembers.begin();                          
             it != rsMembers.end();                                                                 
             it++) {                                                                                
            if (it->second == false) {                                                              
                std::string endpoint = str::stream() << "tcp://" << it->first.host()                
                                                     << ":" << (it->first.port() + 1234);           
                                                                                                    
                try {                                                                               
                    extSendSocket->disconnect(endpoint.c_str());                            
                    log() << "Pubsub disconnected from replica set member." << endl;                
                } catch (zmq::error_t& e) {                                                         
                    log() << "Error disconnecting from replica set member." << causedBy(e) << endl; 
                }                                                                                   
                                                                                                    
                // don't need to lock around the map because this is called from a locked context
                rsMembers.erase(it);                                                                
            } else {                                                                                
                it->second = false;                                                                 
            }                                                                                       
        }                                                                                           
    }                                                                                               
                                                                                                
}  // namespace mongo
