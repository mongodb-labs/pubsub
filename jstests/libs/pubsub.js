/******************** TODO: CHANGE CODES HERE BEFORE PUSHING TO MASTER *********************/
var codes = {
    kPubBadChannel: 18527,
    kPubNoMessage: 18552,
    kPubBadMessage: 18528,
    kPubFailed: 18538, // TODO not sure how to test? requires zmq to fail

    kSubBadChannel: 18531,
    kSubFailed: 18539, // TODO not sure how to test? requires zmq to fail

    kPollBadTimeout: 18535,
    kPollActive: 'Poll currently active.',
    kPollFailed: 18547, // TODO not sure how to test? requires zmq to fail

    kUnsubFailed: 18549, // TODO not sure how to test? requires zmq to fail

    kBadSubscriptionIdType: 18543,
    kBadSubscriptionIdArrayType: 18544,
    kInvalidSubscriptionId: 'Subscription not found.',
};

// NOTE: this function is specific to the messages used in this test
// It is not a generic deep-compare of two javascript objects
var gotMessage = function(res, subscriptionId, channel, msg) {
    if (res.messages[subscriptionId.str] === undefined)
        return false;
    var stream = res.messages[subscriptionId.str][channel];
    for (var i = 0; i < stream.length; i++) {
        if (friendlyEqual(stream[i], msg))
            return true;
    }
    return false;
}

var gotNoMessage = function(res, subscriptionId) {
    return res.messages[subscriptionId.str] === undefined;
}

var onlyGotMessage = function(res, subscriptionId, channel, msg) {
    if (res.messages[subscriptionId.str] === undefined)
        return false;
    var stream = res.messages[subscriptionId.str][channel];
    for (var i = 0; i < stream.length; i++) {
        if (!friendlyEqual(stream[i], msg))
            return false;
    }
    return gotMessage(res, subscriptionId, channel, msg);
}

var msg1 = {a:1};
var msg2 = {a:2};


var selfWorks = function(db) {

    var ps = db.PS();

    // Validate types and existence of arguments. db.runCommand is used here to bypass the 
    // shell-level type checks.

    // publish
    assert.commandFailedWithCode(db.runCommand({ publish: { a: 1 }, message: { a: 1 } }),
                                 codes.kPubBadChannel);
    assert.commandFailedWithCode(db.runCommand({ publish: 'a' }),
                                 codes.kPubNoMessage);
    assert.commandFailedWithCode(db.runCommand({ publish: 'a', message: 'hi' }),
                                 codes.kPubBadMessage);

    // subscribe
    assert.commandFailedWithCode(db.runCommand({ subscribe: { a: 1 } }),
                                 codes.kSubBadChannel);

    // poll
    assert.commandFailedWithCode(db.runCommand({ poll: 'a' }),
                                 codes.kBadSubscriptionIdType);
    assert.commandFailedWithCode(db.runCommand({ poll: [new ObjectId(), 'a'] }),
                                 codes.kBadSubscriptionIdArrayType);
    assert.commandFailedWithCode(db.runCommand({ poll: new ObjectId(), timeout: '1' }),
                                 codes.kPollBadTimeout);

    // unsubscribe
    assert.commandFailedWithCode(db.runCommand({ unsubscribe: 'a' }),
                                 codes.kBadSubscriptionIdType);
    assert.commandFailedWithCode(db.runCommand({ unsubscribe: [new ObjectId(), 'a'] }),
                                 codes.kBadSubscriptionIdArrayType);



    /***** NORMAL OPERATION *****/

    var res, gotFirst = false;

    // node A subscribes to 'A' and polls BEFORE publications on A
    // should get no messages
    var subA = ps.subscribe('A');
    res = ps.poll(subA);
    assert(gotNoMessage(res, subA));

    // node A subscribes to 'A' and polls AFTER publications on A
    // should get one message on 'A'
    assert.commandWorked(ps.publish('A', msg1));
    assert.soon(function() {
        res = ps.poll(subA);
        return gotMessage(res, subA, 'A', msg1);
    });

    // node A should be able to receive multiple messages on 'A'
    assert.commandWorked(ps.publish('A', msg1));
    assert.commandWorked(ps.publish('A', msg2));
    gotFirst = false;
    assert.soon(function() {
        res = ps.poll(subA);
        if (gotMessage(res, subA, 'A', msg1)) gotFirst = true;
        return gotFirst && gotMessage(res, subA, 'A', msg2);
    });

    // node B subscribes to 'B' and polls AFTER publications on *A* (not B)
    // should get no messages
    var subB = ps.subscribe('B');
    res = ps.poll(subB);
    assert(gotNoMessage(res, subB));

    // poll message from A to remove it
    ps.poll(subA);

    // after publications on 'A' and 'B',
    // A only gets messages on 'A'
    // B only gets messages on 'B'
    assert.commandWorked(ps.publish('A', msg1));
    assert.commandWorked(ps.publish('B', msg2));
    assert.soon(function() {
        res = ps.poll(subA);
        return onlyGotMessage(res, subA, 'A', msg1);
    });
    assert.soon(function() {
        res = ps.poll(subB);
        return onlyGotMessage(res, subB, 'B', msg2);
    });

    // nodes should be able to unsubscribe successfully
    assert.commandWorked(ps.unsubscribe(subA));
    assert.commandWorked(ps.unsubscribe(subB));


    /***** LONG RUNNING COMMANDS *****/

    // start a parallel shell to issue a long running poll
    // poll should fail with message 'Poll interrupted by unsubscribe.'
    var subC = ps.subscribe('C');
    var shell1 = startParallelShell('db.runCommand({ poll: ObjectId(\'' + subC + '\'), ' +
                                                    'timeout: 10000 });',
                                    db.getMongo().port);

    // not totally necessary but may need it to wait for parallel shell to start?
    sleep(500);

    // a node cannot poll against an active poll
    assert.soon(function() {
        var res = ps.poll(subC);
        return res.errors !== undefined && res.errors[subC.str] === codes.kPollActive;
    });

    // a node is able issue unsubscribe to interrupt a poll
    ps.unsubscribe(subC);
    assert.soon(function() {
        return ps.poll(subC).errors[subC.str] === codes.kInvalidSubscriptionId;
    });

    // wait for parallel shell to terminate
    shell1();

    // start a new mongod with useDebugTimeout to test errors that occur after long timeouts
    var conn = MongoRunner.runMongod({ setParameter: 'useDebugTimeout=1' });
    var qdb = conn.getDB('test');

    // a subscription that is not polled on for a long time is removed
    var subF = qdb.runCommand({ subscribe: 'F' }).subscriptionId;

    // do not poll for a pseudo-long time
    sleep(500);

    assert.soon(function() {
        return qdb.runCommand({ poll: subF }).errors[subF.str] === codes.kInvalidSubscriptionId;
    });

    // poll that does not receive any messages for a long time returns pollAgain
    var subG = qdb.runCommand({ subscribe: 'F' }).subscriptionId;
    assert.soon(function() {
        return qdb.runCommand({ poll: subG, timeout: 10000 }).pollAgain;
    });

    // close external mongod instance
    MongoRunner.stopMongod(conn);


    /******* NORMAL OPERATION WITH ARRAYS *******/

    // subscribe to a set of channels
    var subH = ps.subscribe('H');
    var subI = ps.subscribe('I');
    var subJ = ps.subscribe('J');
    var arr = [subH, subI, subJ];

    // no messages received on any channel yet - poll as array
    res = ps.poll(arr);
    assert(gotNoMessage(res, subH));
    assert(gotNoMessage(res, subI));
    assert(gotNoMessage(res, subJ));

    // publish to 2 out of the 3 channels
    ps.publish('H', msg1);
    ps.publish('I', msg2);

    // messages received only on those channels -- poll as array
    gotFirst = false;
    assert.soon(function() {
        res = ps.poll(arr);
        if (onlyGotMessage(res, subH, 'H', msg1)) gotFirst = true;
        return gotFirst && onlyGotMessage(res, subI, 'I', msg2) && gotNoMessage(res, subJ);
    });

    // unsubscribe as array
    assert.commandWorked(ps.unsubscribe(arr));


    /******* ERROR CASES ******/

    // cannot poll without subscribing
    var invalidId = new ObjectId();
    assert.eq(ps.poll(invalidId).errors[invalidId.str], codes.kInvalidSubscriptionId);

    // cannot poll on an old subscription
    var subD = ps.subscribe('D');
    ps.unsubscribe(subD);
    assert.eq(ps.poll(subD).errors[subD.str], codes.kInvalidSubscriptionId);

    // cannot unsubscribe without subscribing
    invalidId = new ObjectId();
    assert.eq(ps.unsubscribe(invalidId).errors[invalidId.str], codes.kInvalidSubscriptionId);

    // cannot unsubscribe from old subscription
    var subD = ps.subscribe('D');
    ps.unsubscribe(subD);
    assert.eq(ps.poll(subD).errors[subD.str], codes.kInvalidSubscriptionId);

    return true;
}


var pairWorks = function(db1, db2) {

    var _pairWorks = function(subdb, pubdb) {

        var subscriber = subdb.PS();
        var publisher = pubdb.PS();
        var res;

        // node A subscribes to 'A' and polls BEFORE publications on A
        // should get no messages
        var subA = subscriber.subscribe('A');
        res = subscriber.poll(subA);
        return gotNoMessage(res, subA);

        // node A subscribes to 'A' and polls AFTER publications on A
        // should get one message on 'A'
        assert.commandWorked(publisher.publish('A', msg1));
        assert.soon(function() {
            res = subscriber.poll(subA);
            return onlyGotMessage(res, subA, 'A', msg1);
        });

        // node A should be able to receive multiple messages on 'A'
        assert.commandWorked(publisher.publish('A', msg1));
        assert.commandWorked(publisher.publish('A', msg2));

        var gotFirst = false;
        assert.soon(function() {
            res = subscriber.poll(subA);
            if (gotMessage(res, subA, 'A', msg1)) gotFirst = true;
            return gotFirst && gotMessage(res, subA, 'A', msg2);
        });

        // node B subscribes to 'B' and polls AFTER publications on *A* (not B)
        // should get no messages
        var subB = subscriber.subscribe('B');
        assert.commandWorked(publisher.publish('A', { text: 'hello' }));

        res = subscriber.poll(subB);
        assert(gotNoMessage(res, subB));

        // poll message from A to remove it
        // TODO: need to wrap this in an assert.soon?
        subscriber.poll(subA);

        // node B subscribes to 'B' and polls after publications on B
        // should only get messages on B
        assert.commandWorked(publisher.publish('B', msg2));
        assert.soon(function() {
            res = subscriber.poll(subB);
            return onlyGotMessage(res, subB, 'B', msg2);
        });

        // after publications on B *AND* A
        // A should only get messages on A
        // B should only get messages on B
        assert.commandWorked(publisher.publish('A', msg1));
        assert.commandWorked(publisher.publish('B', msg2));
        assert.soon(function() {
            res = subscriber.poll(subB);
            return onlyGotMessage(res, subB, 'B', msg2);
        });
        assert.soon(function() {
            res = subscriber.poll(subA);
            return onlyGotMessage(res, subA, 'A', msg1);
        });

        // nodes should be able to unsubscribe successfully
        assert.commandWorked(subscriber.unsubscribe(subA));
        assert.commandWorked(subscriber.unsubscribe(subB));

        return true;
    }

    return _pairWorks(db1, db2) && _pairWorks(db2, db1);
}
