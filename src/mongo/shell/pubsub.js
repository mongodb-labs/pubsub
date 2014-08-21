var PS, Subscription;

(function() {

if (PS === undefined) {
    PS = function(db) {
        if (db === undefined) {
            print("Publish/Subscribe takes a database parameter.");
            return;
        }
        this._db = db;
    }
}

PS.prototype.help = function() {
    print("\tps.publish(channel, message)    publishes message to given channel");
    print("\tps.subscribe(channel)           <ObjectId> subscribes to channel");
    print("\tps.poll(id, [timeout])          checks for messages on the subscription id " +
                                             "given, waiting for <timeout> msecs if specified");
    print("\tps.pollAll([timeout])           polls for messages on all subscriptions issed by " +
                                             "this instance of PS");
    print("\tps.unsubscribe(id)              unsubscribes from subscription id given");
    print("\tps.unsubscribeAll()             unsubscribes from all subscriptions issued by " +
                                             "this instance of PS");
}

PS.prototype.publish = function(channel, message) {
    channelType = typeof channel;
    if (channelType != "string")
        throw Error("The channel argument to the publish command must be a string but was a " +
                     channelType);
    messageType = typeof message;
    if (messageType != "object")
        throw Error("The message argument to the publish command must be a document but was a " +
                     messageType);
    var res = this._db.runCommand({ publish: channel, message: message });
    assert.commandWorked(res);
    return res;
}

PS.prototype.subscribe = function(channel, filter, projection) {
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
        cmdObj.filter = filter;
    if (projection)
        cmdObj.projection = projection;
    var res = this._db.runCommand(cmdObj) ;
    assert.commandWorked(res)
    return new Subscription(res.subscriptionId, this);
}

PS.prototype.poll = function(id, timeout) {
    timeoutType = typeof timeout;
    if (timeoutType != "undefined" && timeoutType != "number")
        throw Error("The timeout argument to the poll command must be " +
                    "a number but was a " + timeoutType);
    var dbCommand = { poll: id };
    if (timeout) dbCommand.timeout = timeout;
    var res = this._db.runCommand(dbCommand);
    assert.commandWorked(res);
    return res;
}

PS.prototype.unsubscribe = function(id) {
    idType = typeof id;
    if (idType != "object" && idType != "array")
        throw Error("The subscriptionId argument to the unsubscribe command must be " +
                    "an object or array but was a " + idType);
    var res = this._db.runCommand({ unsubscribe: id });
    assert.commandWorked(res);
    return res;
}

if (Subscription === undefined) {
    Subscription = function(id, ps) {
        if (id === undefined) {
            throw Error("The Subscription constructor takes an id");
        }
        this._id = id;
        this._ps = ps;
    }
}

Subscription.prototype.poll = function(timeout) {
    return this._ps.poll(this._id, timeout);
}

Subscription.prototype.getId = function() {
    return this._id;
}

Subscription.prototype.forEach = function(callback) {
    while (true) {
        var res = this.poll(10000); // 10 second timeout by default
        if (Object.keys(res.messages).length !== 0) callback(res.messages[this._id.str]);
    }
}

Subscription.prototype.unsubscribe = function() {
    return this._ps.unsubscribe(this._id);
}

}());
