/******************** CHANGE CODES HERE BEFORE PUSHING TO MASTER *********************/
var codes = {
    pub_no_channel: 18551,
    pub_bad_channel: 18527,
    pub_no_message: 18552,
    pub_bad_message: 18528,
    pub_failed: 18538, // TODO not sure how to test? requires zmq to fail

    sub_no_channel: 18553,
    sub_bad_channel: 18531,
    sub_failed: 18539, // TODO not sure how to test? requires zmq to fail

    poll_no_subid: 18550,
    poll_bad_subid: 18543,
    poll_bad_timeout: 18535,
    poll_active: 'Poll currently active.',
    poll_interrupted: 18540,
    poll_failed: 18547, // TODO not sure how to test? requires zmq to fail

    unsub_no_subid: 18554,
    unsub_bad_subid: 18532,
    unsub_failed: 18549, // TODO not sure how to test? requires zmq to fail

    invalid_subid: 'Subscription not found.',
};

// NOTE: this function is specific to the messages used in this test
// It is not a generic deep-compare of two javascript objects
var gotMessage = function(res, sub_id, channel, msg) {
    if (res['messages'][sub_id.str] === undefined)
        return false;
    var stream = res['messages'][sub_id.str][channel];
    for (var i = 0; i < stream.length; i++) {
        if (friendlyEqual(stream[i], msg))
            return true;
    }
    return false;
}

var gotNoMessage = function(res, sub_id) {
    return res['messages'][sub_id.str] === undefined;
}

var onlyGotMessage = function(res, sub_id, channel, msg) {
    if (res['messages'][sub_id.str] === undefined)
        return false;
    var stream = res['messages'][sub_id.str][channel];
    for (var i = 0; i < stream.length; i++) {
        if (!friendlyEqual(stream[i], msg))
            return false;
    }
    return gotMessage(res, sub_id, channel, msg);
}

var msg1 = {a:1};
var msg2 = {a:2};


var selfWorks = function(db) {

    /****** VALIDATE TYPES/EXISTENCE OF ARGUMENTS ******/

    // publish
    assert.commandFailedWithCode(db.runCommand({ publish: 1, message: { a: 1 } }),
                                 codes.pub_no_channel);
    assert.commandFailedWithCode(db.runCommand({ publish: 1,
                                                 channel: { a: 1 },
                                                 message: { a: 1 } }),
                                 codes.pub_bad_channel);
    assert.commandFailedWithCode(db.runCommand({ publish: 1, channel: 'a' }),
                                 codes.pub_no_message);
    assert.commandFailedWithCode(db.runCommand({ publish: 1, channel: 'a', message: 'hi' }),
                                 codes.pub_bad_message);

    // subscribe
    assert.commandFailedWithCode(db.runCommand({ subscribe: 1 }),
                                 codes.sub_no_channel);
    assert.commandFailedWithCode(db.runCommand({ subscribe: 1, channel: { a: 1 } }),
                                 codes.sub_bad_channel);

    // poll
    assert.commandFailedWithCode(db.runCommand({ poll: 1 }),
                                 codes.poll_no_subid);
    assert.commandFailedWithCode(db.runCommand({ poll: 1, sub_id: 'a' }),
                                 codes.poll_bad_subid);
    assert.commandFailedWithCode(db.runCommand({ poll: 1, sub_id: new ObjectId(), timeout: '1' }),
                                 codes.poll_bad_timeout);

    // unsubscribe
    assert.commandFailedWithCode(db.runCommand({ unsubscribe: 1 }),
                                 codes.unsub_no_subid);
    assert.commandFailedWithCode(db.runCommand({ unsubscribe: 1, sub_id: 'a' }),
                                 codes.unsub_bad_subid);



    /***** NORMAL OPERATION *****/

    var res, gotFirst = false;

    // node A subscribes to 'A' and polls BEFORE publications on A
    // should get no messages
    var subA = db.runCommand({ subscribe: 1, channel: 'A' })["sub_id"];
    res = db.runCommand({ poll: 1, sub_id: subA });
    assert(gotNoMessage(res, subA));

    // node A subscribes to 'A' and polls AFTER publications on A
    // should get one message on 'A'
    assert.commandWorked(db.runCommand({ publish: 1, channel: 'A', message: msg1 }));
    assert.soon(function() {
        res = db.runCommand({ poll: 1, sub_id: subA });
        return gotMessage(res, subA, 'A', msg1);
    });

    // node A should be able to receive multiple messages on 'A'
    assert.commandWorked(db.runCommand({ publish: 1, channel: 'A', message: msg1 }));
    assert.commandWorked(db.runCommand({ publish: 1, channel: 'A', message: msg2 }));
    gotFirst = false;
    assert.soon(function() {
        res = db.runCommand({ poll: 1, sub_id: subA });
        if (gotMessage(res, subA, 'A', msg1)) gotFirst = true;
        return gotFirst && gotMessage(res, subA, 'A', msg2);
    });

    // node B subscribes to 'B' and polls AFTER publications on *A* (not B)
    // should get no messages
    var subB = db.runCommand({ subscribe: 1, channel: 'B' })["sub_id"];
    res = db.runCommand({ poll: 1, sub_id: subB });
    assert(gotNoMessage(res, subB));

    // poll message from A to remove it
    db.runCommand({ poll: 1, sub_id: subA });

    // after publications on 'A' and 'B',
    // A only gets messages on 'A'
    // B only gets messages on 'B'
    assert.commandWorked(db.runCommand({ publish: 1, channel: 'A', message: msg1 }));
    assert.commandWorked(db.runCommand({ publish: 1, channel: 'B', message: msg2 }));
    assert.soon(function() {
        res = db.runCommand({ poll: 1, sub_id: subA });
        return onlyGotMessage(res, subA, 'A', msg1);
    });
    assert.soon(function() {
        res = db.runCommand({ poll: 1, sub_id: subB });
        return onlyGotMessage(res, subB, 'B', msg2);
    });

    // nodes should be able to unsubscribe successfully
    assert.commandWorked(db.runCommand({ unsubscribe: 1, sub_id: subA }));
    assert.commandWorked(db.runCommand({ unsubscribe: 1, sub_id: subB }));


    /***** LONG RUNNING COMMANDS *****/

    // start a parallel shell to issue a long running poll
    var subC = db.runCommand({ subscribe: 1, channel: 'C' })["sub_id"];
    var shell1 = startParallelShell('db.runCommand({ poll: 1, ' +
                                                    'sub_id: ObjectId(\'' + subC + '\'), ' +
                                                    'timeout: 10000 });',
                                    db.getMongo().port);
    assert.soon(function() {
        return db.runCommand({ viewSubscriptions: 1 })['subs'][subC.str]['activePoll'] === 1;
    });

    // a node cannot poll against an active poll
    assert.eq(db.runCommand({ poll: 1, sub_id: subC })['errors'][subC.str], codes.poll_active);

    // a node is able issue unsubscribe to interrupt a poll
    db.runCommand({ unsubscribe: 1, sub_id: subC });
    assert.soon(function() {
        return db.runCommand({ poll: 1, sub_id: subC })['errors'][subC.str] ===
               codes.invalid_subid;
    });

    // wait for parallel shell to terminate
    shell1();

    // start a new mongod with quickPubsubTimeout to test errors that occur after long timeouts
    var conn = MongoRunner.runMongod({ setParameter: "quickPubsubTimeout=1" });
    var qdb = conn.getDB('test');

    // a subscription that is not polled on for a long time is removed
    // NOTE: alternative sleep(500) is to use viewSubscriptions within the assert.soon
    var subF = qdb.runCommand({ subscribe: 1, channel: 'F' })["sub_id"];

    // do not poll for a pseudo-long time
    sleep(500);

    assert.soon(function() {
        return qdb.runCommand({ poll: 1, sub_id: subF })['errors'][subF.str] ===
               codes.invalid_subid;
    });

    // poll that does not receive any messages for a long time returns pollAgain
    var subG = qdb.runCommand({ subscribe: 1, channel: 'F' })["sub_id"];
    assert.soon(function() {
        return qdb.runCommand({ poll: 1, sub_id: subG, timeout: 10000 })["pollAgain"];
    });

    // close external mongod instance
    MongoRunner.stopMongod(conn);


    /******* NORMAL OPERATION WITH ARRAYS *******/

    // subscribe to a set of channels
    var subH = db.runCommand({ subscribe: 1, channel: 'H' })["sub_id"];
    var subI = db.runCommand({ subscribe: 1, channel: 'I' })["sub_id"];
    var subJ = db.runCommand({ subscribe: 1, channel: 'J' })["sub_id"];
    var arr = [subH, subI, subJ];

    // no messages received on any channel yet - poll as array
    assert.soon(function() {
        res = db.runCommand({ poll: 1, sub_id: arr });
        return gotNoMessage(res, subH) && gotNoMessage(res, subI) && gotNoMessage(res, subJ);
    });

    // publish to 2 out of the 3 channels
    db.runCommand({ publish: 1, channel: 'H', message: msg1 });
    db.runCommand({ publish: 1, channel: 'I', message: msg2 });

    // messages received only on those channels -- poll as array
    gotFirst = false;
    assert.soon(function() {
        res = db.runCommand({ poll: 1, sub_id: arr });
        if (onlyGotMessage(res, subH, 'H', msg1)) gotFirst = true;
        return gotFirst && onlyGotMessage(res, subI, 'I', msg2) && gotNoMessage(res, subJ);
    });

    // unsubscribe as array
    assert.commandWorked(db.runCommand({ unsubscribe: 1, sub_id: arr }));


    /******* ERROR CASES ******/

    // cannot poll without subscribing
    var invalid_id = new ObjectId();
    assert.soon(function() {
        return db.runCommand({ poll: 1, sub_id: invalid_id })['errors'][invalid_id.str] ===
               codes.invalid_subid;
    });

    // cannot poll on an old subscription
    var subD = db.runCommand({ subscribe: 1, channel: 'D' })["sub_id"];
    db.runCommand({ unsubscribe: 1, sub_id: subD });
    assert.soon(function() {
        return db.runCommand({ poll: 1, sub_id: subD })['errors'][subD.str] === codes.invalid_subid;
    });

    // cannot unsubscribe without subscribing
    var invalid_id = new ObjectId();
    assert.soon(function() {
        return db.runCommand({ unsubscribe: 1, sub_id: invalid_id })['errors'][invalid_id.str] ===
               codes.invalid_subid;
    });

    // cannot unsubscribe from old subscription
    var subD = db.runCommand({ subscribe: 1, channel: 'D' })["sub_id"];
    db.runCommand({ unsubscribe: 1, sub_id: subD });
    assert.soon(function() {
        return db.runCommand({ poll: 1, sub_id: subD })['errors'][subD.str] ===
               codes.invalid_subid;
    });

    return true;
}


var pairWorks = function(db1, db2) {

    var _pairWorks = function(subdb, pubdb) {

        var res;

        // node A subscribes to 'A' and polls BEFORE publications on A
        // should get no messages
        var subA = subdb.runCommand({ subscribe: 1, channel: "A" })["sub_id"];
        res = subdb.runCommand({ poll: 1, sub_id: subA });
        return gotNoMessage(res, subA);

        // node A subscribes to 'A' and polls AFTER publications on A
        // should get one message on 'A'
        assert.commandWorked(pubdb.runCommand({ publish: 1, channel: "A", message: msg1 }));
        assert.soon(function() {
            res = subdb.runCommand({ poll: 1, sub_id: subA });
            return onlyGotMessage(res, subA, 'A', msg1);
        });

        // node A should be able to receive multiple messages on 'A'
        assert.commandWorked(pubdb.runCommand({ publish: 1, channel: 'A', message: msg1 }));
        assert.commandWorked(pubdb.runCommand({ publish: 1, channel: 'A', message: msg2 }));

        var gotFirst = false;
        assert.soon(function() {
            res = subdb.runCommand({ poll: 1, sub_id: subA });
            if (gotMessage(res, subA, 'A', msg1)) gotFirst = true;
            return gotFirst && gotMessage(res, subA, 'A', msg2);
        });

        // node B subscribes to 'B' and polls AFTER publications on *A* (not B)
        // should get no messages
        var subB = subdb.runCommand({ subscribe: 1, channel: "B" })["sub_id"];
        assert.commandWorked(pubdb.runCommand({ publish: 1,
                                                channel: 'A',
                                                message: { text: 'hello' } }));
        res = subdb.runCommand({ poll: 1, sub_id: subB });
        assert(gotNoMessage(res, subB));

        // poll message from A to remove it
        subdb.runCommand({ poll: 1, sub_id: subA });

        // node B subscribes to 'B' and polls after publications on B
        // should only get messages on B
        assert.commandWorked(pubdb.runCommand({ publish: 1, channel: 'B', message: msg2 }));
        assert.soon(function() {
            res = subdb.runCommand({ poll: 1, sub_id: subB });
            return onlyGotMessage(res, subB, 'B', msg2);
        });

        // after publications on B *AND* A
        // A should only get messages on A
        // B should only get messages on B
        assert.commandWorked(pubdb.runCommand({ publish: 1, channel: 'A', message: msg1 }));
        assert.commandWorked(pubdb.runCommand({ publish: 1, channel: 'B', message: msg2 }));
        assert.soon(function() {
            res = subdb.runCommand({ poll: 1, sub_id: subB });
            return onlyGotMessage(res, subB, 'B', msg2);
        });
        assert.soon(function() {
            res = subdb.runCommand({ poll: 1, sub_id: subA });
            return onlyGotMessage(res, subA, 'A', msg1);
        });

        // nodes should be able to unsubscribe successfully
        assert.commandWorked(subdb.runCommand({ unsubscribe: 1, sub_id: subA }));
        assert.commandWorked(subdb.runCommand({ unsubscribe: 1, sub_id: subB }));

        return true;
    }

    return _pairWorks(db1, db2) && _pairWorks(db2, db1);
}
