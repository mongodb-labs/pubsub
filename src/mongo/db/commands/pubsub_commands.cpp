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

#include <boost/thread.hpp>
#include <string>
#include <vector>

#include "mongo/bson/oid.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/db/pubsub.h"

namespace mongo {

    namespace {

        // constants for field names
        const std::string kSubscriptionId = "subscriptionId";
        const std::string kPublishField = "publish";
        const std::string kMessageField = "message";
        const std::string kSubscribeField = "subscribe";
        const std::string kQueryField = "query";
        const std::string kPollField = "poll";
        const std::string kTimeoutField = "timeout";
        const std::string kMillisPolledField = "millisPolled";
        const std::string kPollAgainField = "pollAgain";
        const std::string kMessagesField = "messages";
        const std::string kErrorField = "errors";
        const std::string kUnsubscribeField = "unsubscribe";

        // Helper method to validate single or array of SubscriptionId arguments
        void validate(BSONElement& element, std::set<OID>& oids) {
            // ensure that the subscriptionId argument is a SubscriptionId or array
            uassert(18543,
                    mongoutils::str::stream() << "The subscriptionId argument must be "
                                              << "an ObjectID or Array but was a "
                                              << typeName(element.type()),
                    element.type() == jstOID || element.type() == mongo::Array);

            if (element.type() == jstOID) {
                oids.insert(element.OID());
            } else {
                std::vector<BSONElement> elements = element.Array();
                for (std::vector<BSONElement>::iterator it = elements.begin();
                     it != elements.end();
                     it++) {
                    // ensure that each member of the array is a SubscriptionId
                    uassert(18544,
                            mongoutils::str::stream() << "Each subscriptionId in the "
                                                      << "subscriptionId array must be an "
                                                      << "ObjectID but found a "
                                                      << typeName(it->type()),
                            it->type() == jstOID);
                    oids.insert(it->OID());
                }
            }
        }

    }


    /**
     * Command for publishing to self and other nodes in your replica set or cluster.
     *
     * Format:
     * {
     *    publish: <string>, // name of channel to publish to.
     *    message: <Object>  // the body of the message to publish. Can have any format desired.
     * }
     */
    class PublishCommand : public Command {
    public:
        PublishCommand() : Command("publish") {}

        virtual bool slaveOk() const { return true; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual bool isWriteCommandForConfigServer() const { return false; }

        virtual LockType locktype() const { return NONE; }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            // TODO: get a real action type
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help(stringstream &help) const {
            help << "{ publish : <channel>, message : {} }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl) {

            BSONElement channelElem = cmdObj[kPublishField];

            // ensure that the channel is a string
            uassert(18527,
                    mongoutils::str::stream() << "The channel passed to the publish "
                                              << "command must be a string but was a "
                                              << typeName(channelElem.type()),
                    channelElem.type() == mongo::String);


            // ensure that message argument exists
            uassert(18552,
                    mongoutils::str::stream() << "The publish command requires a message argument.",
                    cmdObj.hasField(kMessageField));

            BSONElement messageElem = cmdObj[kMessageField];

            // ensure that the message is a document
            uassert(18528,
                    mongoutils::str::stream() << "The message for the publish command must be a "
                                              << "document but was a "
                                              << typeName(messageElem.type()),
                    messageElem.type() == mongo::Object);

            string channel = channelElem.String();
            BSONObj message = messageElem.Obj();

            bool success = PubSub::publish(channel, message);

            uassert(18538, "Failed to publish message.", success);

            return true;
        }

    } publishCmd;


    /**
     * Command for subscribing to messages on a given channel.
     *
     * Format:
     * {
     *    subscribe: <string> // name of channel to subscribe to.
     * }
     *
     * Return value:
     * {
     *    subscriptionId: <ObjectId> // ID of subscription created
     * }
     *
     */
    class SubscribeCommand : public Command {
    public:
        SubscribeCommand() : Command("subscribe") {}

        virtual bool slaveOk() const { return true; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual bool isWriteCommandForConfigServer() const { return false; }

        virtual LockType locktype() const { return NONE; }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            // TODO: get a real action type
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help(stringstream &help) const {
            help << "{ subscribe : <channel>, filter : <BSONObj>, projection : <BSONObj> }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl) {

            BSONElement channelElem = cmdObj[kSubscribeField];

            // ensure that the channel is a string
            uassert(18531, mongoutils::str::stream() << "The channel passed to the subscribe "
                                                     << "command must be a string but was a "
                                                     << typeName(channelElem.type()),
                    channelElem.type() == mongo::String);

            string channel = channelElem.String();

            // TODO: validate filter format (look at find command?)
            BSONObj filter;
            if (cmdObj.hasField("filter")){
                BSONElement filterElem = cmdObj["filter"];
                // ensure that the filter is a BSON object
                uassert(18553, mongoutils::str::stream() << "The filter passed to the subscribe "
                                                         << "command must be an object but was a "
                                                         << typeName(filterElem.type()),
                        filterElem.type() == mongo::Object);
                filter = filterElem.Obj();
            }

            // TODO: validate projection format
            BSONObj projection;
            if (cmdObj.hasField("projection")){
                BSONElement projectionElem = cmdObj["projection"];
                // ensure that the projection is a BSON object
                uassert(18554, mongoutils::str::stream() << "The projection passed to the "
                                                         << "subscribe command must be an object "
                                                         << "but was a "
                                                         << typeName(projectionElem.type()),
                        projectionElem.type() == mongo::Object);
                projection = projectionElem.Obj();
            }


            // TODO: add secure access to this channel?
            // perhaps return an <oid, key> pair?
            OID oid = PubSub::subscribe(channel, filter, projection);
            result.append(kSubscriptionId, oid);

            return true;
        }

    } subscribeCmd;


    /**
     * Command for polling on a single or multiple subscriptions.
     *
     * Format:
     * {
     *    subscriptionId: <ObjectId | Array>, // ID or IDs of subscriptions to poll on
     *    [timeout]: <Number>  // number of milliseconds to wait if there are no new messages.
     * }
     *
     * Return value:
     * {
     *    messages: <Object>, // messages found. Always returned, even if empty. Has format:
     *        {
     *           subscriptionId: <Array>, // key is ID, value is array of message objects
     *           subscriptionId2: <Array>,
     *           ...
     *        }
     *    errors: <Object>, // returned if and only if any errors occurred. Has format:
     *        {
     *           subscriptionId: <string>, // key is ID of channel, value is error string
     *           subscriptionId2: <string>,
     *           ...
     *        }
     *    millisPolled: <Integer>, // number of milliseconds command waited before finding messages.
     *    [pollAgain]: <Bool> // returned as true only if poll gets no messages and times out.
     * }
     */
    class PollCommand : public Command {
    public:
        PollCommand() : Command("poll") {}

        virtual bool slaveOk() const { return true; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual bool isWriteCommandForConfigServer() const { return false; }

        virtual LockType locktype() const { return NONE; }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            // TODO: get a real action type
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help(stringstream &help) const {
            help << "{ poll : <subscriptionId(s)>, timeout : <integer milliseconds> }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl) {

            BSONElement oidElement = cmdObj[kPollField];

            std::set<OID> oids;
            validate(oidElement, oids);

            // if no timeout is specified, default is to return from the poll without waiting
            long timeout = 0L;
            BSONElement timeoutElem = cmdObj[kTimeoutField];
            if (!timeoutElem.eoo()) {
                uassert(18535,
                        mongoutils::str::stream() << "The timeout argument must be an integer "
                                                  << "but was a "
                                                  << typeName(timeoutElem.type()),
                        timeoutElem.type() == NumberDouble ||
                        timeoutElem.type() == NumberLong ||
                        timeoutElem.type() == NumberInt);

                if (timeoutElem.type() == NumberDouble) {
                    timeout = static_cast<long>(std::floor(timeoutElem.numberDouble()));
                } else if (timeoutElem.type() == NumberLong) {
                    timeout = timeoutElem.numberLong();
                } else {
                    timeout = timeoutElem.numberInt();
                }
            }

            long long millisPolled = 0;
            bool pollAgain = false;
            std::map<SubscriptionId, std::string> errors;
            std::priority_queue<SubscriptionMessage> messages = PubSub::poll(oids,
                                                                             timeout,
                                                                             millisPolled,
                                                                             pollAgain,
                                                                             errors);

            // serialize messages into BSON
            BSONObjBuilder messagesBuilder;
            while (!messages.empty()) {
                SubscriptionMessage sm = messages.top();
                SubscriptionId currId = sm.subscriptionId;
                BSONObjBuilder channelBuilder;
                while (!messages.empty() && sm.subscriptionId == currId) {
                    std::string currChannel = sm.channel;
                    BSONArrayBuilder arrayBuilder;
                    while (sm.subscriptionId == currId && sm.channel == currChannel) {
                        arrayBuilder.append(sm.message);
                        messages.pop();
                        if (messages.empty())
                            break;
                        sm = messages.top();
                    }
                    channelBuilder.append(currChannel, arrayBuilder.arr());
                }
                messagesBuilder.append(currId.toString(), channelBuilder.obj());
            }

            result.append(kMessagesField, messagesBuilder.obj());
            result.append(kMillisPolledField, millisPolled);
            if (pollAgain)
                result.append(kPollAgainField, true);

            if (errors.size() > 0) {
                BSONObjBuilder errorBuilder;
                for (std::map<SubscriptionId, std::string>::iterator it = errors.begin();
                     it != errors.end();
                     it++) {
                    errorBuilder.append(it->first.toString(), it->second);
                }
                result.append(kErrorField, errorBuilder.obj());
            }

            return true;
        }

    } pollCmd;


    /**
     * Command for unsubscribing from a previously registered subscription.
     *
     * Format:
     * {
     *    unsubscribe: <ObjectId | Array>, // ID(s) of channel(s) to unsubscribe from.
     * }
     *
     * Return value:
     * {
     *    [errors]: <Object> // if any subscriptions can't be found, returns Object with format:
     *        {
     *           subscriptionId: <string>, // key is subscription ID, value is error message
     *           subscriptionId2: <string>,
     *           ...
     *        }
     * }
     */
    class UnsubscribeCommand : public Command {
    public:
        UnsubscribeCommand() : Command("unsubscribe") {}

        virtual bool slaveOk() const { return true; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual bool isWriteCommandForConfigServer() const { return false; }

        virtual LockType locktype() const { return NONE; }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            // TODO: get a real action type
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help(stringstream &help) const {
            help << "{ unsubscribe : <subscriptionId(s)> }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl) {

            BSONElement oidElement = cmdObj[kUnsubscribeField];
            std::set<OID> oids;
            validate(oidElement, oids);

            std::map<SubscriptionId, std::string> errors;
            for (std::set<OID>::iterator it = oids.begin(); it != oids.end(); it++) {
                OID oid = *it;
                PubSub::unsubscribe(oid, errors);
            }

            if (errors.size() > 0) {
                BSONObjBuilder errorBuilder;
                for (std::map<SubscriptionId, std::string>::iterator it = errors.begin();
                     it != errors.end();
                     it++) {
                    errorBuilder.append(it->first.toString(), it->second);
                }
                result.append(kErrorField, errorBuilder.obj());
            }

            return true;
        }

    } unsubscribeCmd;

}  // namespace mongo
