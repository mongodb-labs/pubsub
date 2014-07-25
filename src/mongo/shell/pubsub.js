ps = function() { return "try ps.help()"; }

ps._allSubscriptions = [];

ps.help = function() {
    print("\tps.publish(channel, message)    publishes message to given channel");
    print("\tps.subscribe(channel)           <ObjectId> subscribes to channel");
    print("\tps.poll(id, [timeout])          checks for messages on the subscription id " +
                                             "given, waiting for <timeout> msecs if specified");
    print("\tps.pollAll([timeout])           polls for messages on all subscriptions issed by " +
                                             "this shell");
    print("\tps.unsubscribe(id)              unsubscribes from subscription id given");
    print("\tps.unsubscribeAll()             unsubscribes from all subscriptions issued by " +
                                             "this shell");
}

ps.publish = function(channel, message) {
    channelType = typeof channel;
    if (channelType != "string")
        throw Error("The channel argument to the publish command must be a string but was a " +
                     channelType);
    messageType = typeof message;
    if (messageType != "object")
        throw Error("The message argument to the publish command must be a document but was a " +
                     messageType);
    return assert.commandWorked(db.runCommand({ publish: 1, channel: channel, message: message }));
}

ps.subscribe = function(channel) {
    channelType = typeof channel;
    if (channelType != "string")
        throw Error("The channel argument to the subscribe command must be a string but was a " +
                     channelType);
    var sub_id = assert.commandWorked(db.runCommand({ subscribe: 1, channel: channel }))['sub_id'];
    this._allSubscriptions.push(sub_id);
    return sub_id;
}

ps.poll = function(id, timeout) {
    timeoutType = typeof timeout;
    if (timeoutType != "undefined" && timeoutType != "number")
        throw Error("The timeout argument to the poll command must be " +
                    "a number but was a " + timeoutType);
    var dbCommand = { poll: 1, sub_id: id };
    if (timeout) dbCommand.timeout = timeout;
    return assert.commandWorked(db.runCommand(dbCommand));
}

ps.pollAll = function(timeout) {
    timeoutType = typeof timeout;
    if (timeoutType != "undefined" && timeoutType != "number")
        throw Error("The timeout argument to the poll command must be a " +
                    "number but was a " + timeoutType);
    var dbCommand = { poll: 1, sub_id: this._allSubscriptions };
    if (timeout) dbCommand.timeout = timeout;
    return assert.commandWorked(db.runCommand(dbCommand));
}

ps.unsubscribe = function(id) {
    idType = typeof id;
    if (idType != "object" && idType != "array")
        throw Error("The sub_id argument to the unsubscribe command must be " +
                    "an object or array but was a " + idType);
    var res = assert.commandWorked(db.runCommand({ unsubscribe: 1, sub_id: id }));
    var idx = this._allSubscriptions.indexOf(id);
    this._allSubscriptions.splice(idx, 1);
    return res;
}

ps.unsubscribeAll = function() {
    var res = assert.commandWorked(db.runCommand({ unsubscribe: 1,
                                                   sub_id: this._allSubscriptions }));
    this._allSubscriptions = [];
    return res;
}
