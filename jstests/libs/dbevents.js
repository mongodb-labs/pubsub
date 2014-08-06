/**
 * Shared functions to use when testing database event notifications
 * using pubsub. Test to make sure that write commands on the publisher
 * are received as events on the appropriate channels on the subscriber.
 */
var testPubSubDataEvents = function(subdb, pubdb) {

    // makeshift function overloading so this
    // method can test a single node or a pair
    if (pubdb === undefined) {
        pubdb = subdb;
    }

    var subscriber = subdb.PS();

    // documents used for this test
    var oldDoc = {_id: 1, text: 'hello'};
    var newDoc = {_id: 1, text: 'goodbye'}

    // clean up collection used for this test
    pubdb.pubsub.remove(oldDoc);
    pubdb.pubsub.remove(newDoc);

    // subscribe to all events on the publisher DB's pubsub collection
    var channel = '$events';
    var namespace = pubdb + ".pubsub"

    // TODO: rewrite shell helpers to take a DB
    // until this is done, shell will crash after completion of tests
    var filter = {namespace: namespace};
    var eventSub = subscriber.subscribe(channel, filter);
    var res, msg;



    // inserts:
    // - do an insert
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the correct document
    assert.writeOK(pubdb.pubsub.save(oldDoc));

    assert.soon(function() {
        res = subscriber.poll(eventSub);
        return res.messages[eventSub.str] !== undefined;
    });

    assertMessageCount(res, eventSub, channel, 1);
    msg = res.messages[eventSub.str][channel][0];
    var insertDoc = {
        namespace: namespace,
        type: "insert",
        doc: oldDoc
    };
    assert.eq(msg, insertDoc);



    // updates:
    // - do an update
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the correct form:
    // {
    //    old: <old document>,
    //    new: <new document>
    // }
    assert.writeOK(pubdb.pubsub.save(newDoc));

    assert.soon(function() {
        res = subscriber.poll(eventSub);
        return res.messages[eventSub.str] !== undefined;
    });

    assertMessageCount(res, eventSub, channel, 1);
    var msg = res.messages[eventSub.str][channel][0];
    var updateDoc = {
        namespace: namespace,
        type: "update",
        doc: {
            old: oldDoc,
            new: newDoc
        }
    };
    assert.eq(msg, updateDoc);



    // removes:
    // - do a remove
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the deleted document
    assert.writeOK(pubdb.pubsub.remove(newDoc));

    assert.soon(function() {
        res = subscriber.poll(eventSub);
        return res.messages[eventSub.str] !== undefined;
    });

    assertMessageCount(res, eventSub, channel, 1);
    msg = res.messages[eventSub.str][channel][0];
    var removeDoc = {
        namespace: namespace,
        type: "remove",
        doc: newDoc
    };
    assert.eq(msg, removeDoc);


    // clean up subscription
    subscriber.unsubscribe(eventSub);
}

var assertMessageCount = function(res, subscriptionId, channel, count) {
    var channelMessages = res.messages[subscriptionId.str][channel];
    assert.neq(channelMessages, undefined);
    assert.eq(channelMessages.length, count);
}
