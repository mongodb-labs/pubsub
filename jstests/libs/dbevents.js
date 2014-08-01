/**
 * Shared functions to use when testing database event notifications
 * using pubsub. Test to make sure that write commands on the publisher
 * are received as events on the appropriate channels on the subscriber.
 */
var testPubSubDataEvents = function(publisher, subscriber) {

    // makeshift function overloading so this
    // method can test a single node or a pair
    if (subscriber === undefined) {
        subscriber = publisher;
    }

    // clean up collection used for this test
    publisher.pubsub.remove({text: 'hello'});
    publisher.pubsub.remove({text: 'goodbye'});

    // subscribe to all events on the publisher DB's pubsub collection
    var channelPrefix = '$event.' + publisher + '.pubsub.'
    var insertChannel = channelPrefix + 'insert';
    var updateChannel = channelPrefix + 'update';
    var removeChannel = channelPrefix + 'remove';
    var eventSub = subscriber.runCommand({subscribe: channelPrefix}).subscriptionId;
    var res, msg;



    // inserts:
    // - do an insert
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the correct document
    var oldDoc = {_id: 1, text: 'hello'};
    assert.writeOK(publisher.pubsub.save(oldDoc));

    assert.soon(function() {
        res = subscriber.runCommand({poll: eventSub});
        return res.messages[eventSub.str] !== undefined;
    });

    assertMessageCount(res, eventSub, insertChannel, 1);
    msg = res.messages[eventSub.str][insertChannel][0];
    assert.eq(msg, oldDoc);



    // updates:
    // - do an update
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the correct form:
    // {
    //    old: <old document>,
    //    new: <new document>
    // }
    var newDoc = {_id: 1, text: 'goodbye'}
    assert.writeOK(publisher.pubsub.save(newDoc));

    assert.soon(function() {
        res = subscriber.runCommand({poll: eventSub});
        return res.messages[eventSub.str] !== undefined;
    });

    assertMessageCount(res, eventSub, updateChannel, 1);
    var msg = res.messages[eventSub.str][updateChannel][0];
    assert(msg.hasOwnProperty('old'));
    assert(msg.hasOwnProperty('new'));
    assert.eq(msg.old, oldDoc);
    assert.eq(msg.new, newDoc);



    // removes:
    // - do a remove
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the deleted document
    assert.writeOK(publisher.pubsub.remove({text: 'goodbye'}));

    assert.soon(function() {
        res = subscriber.runCommand({poll: eventSub});
        return res.messages[eventSub.str] !== undefined;
    });

    assertMessageCount(res, eventSub, removeChannel, 1);
    msg = res.messages[eventSub.str][removeChannel][0];
    assert.eq(msg, newDoc);


    // clean up subscription
    subscriber.runCommand({unsubscribe: eventSub});
}

var assertMessageCount = function(res, subscriptionId, channel, count) {
    var channelMessages = res.messages[subscriptionId.str][channel];
    assert.neq(channelMessages, undefined);
    assert.eq(channelMessages.length, count);
}
