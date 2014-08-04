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
    var res = db.runCommand({ publish: channel, message: message });
    assert.commandWorked(res);
    return res;
}

ps.subscribe = function(channel, filter, projection) {
    channelType = typeof channel;
    if (channelType != "string")
        throw Error("The channel argument to the subscribe command must be a string but was a " +
                     channelType);
    filterType = typeof filter;
    if (filterType != "undefined" && filterType != "object")
        throw Error("The filter argument to the subscribe command must be an object but was a " +
                     filterType);
    projectionType = typeof projection;
    if (projectionType != "undefined" && projectionType != "object")
        throw Error("The projection argument to the subscribe command must be an object " +
                    "but was a " +
                    projectionType);

    var cmdObj = {subscribe: channel};
    if (filter)
        cmdObj["filter"] = filter;
    if (projection)
        cmdObj["projection"] = projection;
    var res = db.runCommand(cmdObj) ;
    assert.commandWorked(res)
    var subscriptionId = res['subscriptionId'];
    this._allSubscriptions.push(subscriptionId);
    return subscriptionId;
}

ps.poll = function(id, timeout) {
    timeoutType = typeof timeout;
    if (timeoutType != "undefined" && timeoutType != "number")
        throw Error("The timeout argument to the poll command must be " +
                    "a number but was a " + timeoutType);
    var dbCommand = { poll: id };
    if (timeout) dbCommand.timeout = timeout;
    var res = db.runCommand(dbCommand);
    assert.commandWorked(res);
    return res;
}

ps.pollAll = function(timeout) {
    timeoutType = typeof timeout;
    if (timeoutType != "undefined" && timeoutType != "number")
        throw Error("The timeout argument to the poll command must be a " +
                    "number but was a " + timeoutType);
    var dbCommand = { poll: this._allSubscriptions };
    if (timeout) dbCommand.timeout = timeout;
    var res = db.runCommand(dbCommand);
    assert.commandWorked(res);
    return res;
}

ps.unsubscribe = function(id) {
    idType = typeof id;
    if (idType != "object" && idType != "array")
        throw Error("The subscriptionId argument to the unsubscribe command must be " +
                    "an object or array but was a " + idType);
    var res = db.runCommand({ unsubscribe: id });
    assert.commandWorked(res);
    var idx = this._allSubscriptions.indexOf(id);
    this._allSubscriptions.splice(idx, 1);
    return res;
}

ps.unsubscribeAll = function() {
    var res = db.runCommand({ unsubscribe: this._allSubscriptions });
    assert.commandWorked(res);
    this._allSubscriptions = [];
    return res
}
