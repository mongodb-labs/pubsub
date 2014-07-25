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
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help(stringstream &help) const {
            help << "{ publish : 1 , channel : 'string', message : {} }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl) {

            // ensure that channel argument exists
            uassert(18551,
                    mongoutils::str::stream() << "The publish command requires a channel argument.",
                    cmdObj.hasField("channel"));

            BSONElement channelElem = cmdObj["channel"];

            // ensure that the channel is a string
            uassert(18527,
                    mongoutils::str::stream() << "The channel argument to the publish "
                                              << "command must be a string but was a "
                                              << typeName(channelElem.type()),
                    channelElem.type() == mongo::String);


            // ensure that message argument exists
            uassert(18552,
                    mongoutils::str::stream() << "The publish command requires a message argument.",
                    cmdObj.hasField("message"));

            BSONElement messageElem = cmdObj["message"];

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
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help(stringstream &help) const {
            help << "{ subscribe : 1, channel : 'string' }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl) {

            // ensure that channel argument exists
            uassert(18553,
                    mongoutils::str::stream() << "The subscribe command requires "
                                              << "a channel argument.",
                    cmdObj.hasField("channel"));

            BSONElement channelElem = cmdObj["channel"];

            // ensure that the channel is a string
            uassert(18531, mongoutils::str::stream() << "The channel argument to the subscribe "
                                                     << "command must be a string but was a "
                                                     << typeName(channelElem.type()),
                    channelElem.type() == mongo::String);

            string channel = channelElem.String();

            // TODO: add secure access to this channel?
            // perhaps return an <oid, key> pair?
            OID oid = PubSub::subscribe(channel);
            result.append("sub_id" , oid);

            return true;
        }

    } subscribeCmd;


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
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help(stringstream &help) const {
            help << "{ poll : 1 , sub_id : ObjectID, timeout : <integer milliseconds> }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl) {

            // ensure that sub_id argument exists
            uassert(18550,
                    mongoutils::str::stream() << "The poll command requires either an ObjectID "
                                              << "or array of ObjectIDs as the sub_id argument.",
                    cmdObj.hasField("sub_id"));

            BSONElement oidElement = cmdObj["sub_id"];

            std::set<OID> oids;
            validate(oidElement, oids);

            // if no timeout is specified, default is to return from the poll without waiting
            long timeout = 0L;
            if (cmdObj.hasField("timeout")) {
                BSONElement timeoutElem = cmdObj["timeout"];
                uassert(18535,
                        mongoutils::str::stream() << "The timeout argument must be an integer "
                                                  << "but was a "
                                                  << typeName(timeoutElem.type()),
                        timeoutElem.type() == NumberDouble || timeoutElem.type() == NumberLong);
                timeout = timeoutElem.numberLong();
            }

            BSONObjBuilder errors;
            PubSub::poll(oids, timeout, result, errors);

            if (errors.asTempObj().nFields() > 0)
                result.append("errors", errors.obj());

            return true;
        }

    private:
        void validate(BSONElement& element, std::set<OID>& oids) {

            // ensure that the sub_id argument is an ObjectID or array
            uassert(18543,
                    mongoutils::str::stream() << "The sub_id argument to the poll command "
                                              << "must be an ObjectID or Array but was a "
                                              << typeName(element.type()),
                    element.type() == jstOID || element.type() == mongo::Array);

            if (element.type() == jstOID) {
                oids.insert(element.OID());
            } else {
                std::vector<BSONElement> elements = element.Array();
                for (std::vector<BSONElement>::iterator it = elements.begin();
                     it != elements.end();
                     it++) {
                    // ensure that each member of the array is an ObjectID
                    uassert(18544,
                            mongoutils::str::stream() << "Each sub_id in the sub_id array must be "
                                                      << "an ObjectID but found a "
                                                      << typeName(it->type()),
                            it->type() == jstOID);
                    oids.insert(it->OID());
                }
            }
        }

    } pollCmd;


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
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help(stringstream &help) const {
            help << "{ unsubscribe : 1, sub_id : ObjectId }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl) {

            // ensure that sub_id argument exists
            uassert(18554,
                    mongoutils::str::stream() << "The unsubcribe command requires either an "
                                              << "ObjectID or array of ObjectIDs as the sub_id "
                                              << "argument.",
                                              cmdObj.hasField("sub_id"));

            BSONElement oidElement = cmdObj["sub_id"];
            std::set<OID> oids;
            validate(oidElement, oids);

            BSONObjBuilder errors;
            for (std::set<OID>::iterator it = oids.begin(); it != oids.end(); it++) {
                OID oid = *it;
                PubSub::unsubscribe(oid, errors);
            }

            if (errors.asTempObj().nFields() > 0)
                result.append("errors", errors.obj());

            return true;
        }

    private:
        void validate(BSONElement& element, std::set<OID>& oids) {
            // ensure that the sub_id argument is an ObjectID or array
            uassert(18532,
                    mongoutils::str::stream() << "The sub_id argument to the unsubscribe command "
                                              << "must be an ObjectID or Array but was a "
                                              << typeName(element.type()),
                    element.type() == jstOID || element.type() == mongo::Array);

            if (element.type() == jstOID) {
                oids.insert(element.OID());
            } else {
                std::vector<BSONElement> elements = element.Array();
                for (std::vector<BSONElement>::iterator it = elements.begin();
                     it != elements.end();
                     it++) {
                    // ensure that each member of the array is an ObjectID
                    uassert(18545,
                            mongoutils::str::stream() << "Each array member must be an "
                                                      << "ObjectID but found a "
                                                      << typeName(it->type()),
                            it->type() == jstOID);
                    oids.insert(it->OID());
                }
            }
        }

    } unsubscribeCmd;

    class ViewSubscriptionsCommand : public Command {
    public:
        ViewSubscriptionsCommand() : Command("viewSubscriptions") {}

        virtual bool slaveOk() const { return true; }
        virtual bool slaveOverrideOk() const { return true; }
        virtual bool isWriteCommandForConfigServer() const { return false; }

        virtual LockType locktype() const { return NONE; }

        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::find);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        virtual void help(stringstream &help) const {
            help << "{ viewSubscriptions : 1 }";
        }

        bool run(const string& dbname, BSONObj& cmdObj, int, string& errmsg,
                 BSONObjBuilder& result, bool fromRepl) {

            BSONObj subs;
            PubSub::viewSubscriptions(subs);
            result.append("subs", subs);
            return true;
        }

    } viewSubscriptionsCmd;

}  // namespace mongo
