/**
 * Shared functions to use when testing database event notifications
 * using pubsub. Test to make sure that write commands on the publisher
 * are received as events on the appropriate channels on the subscriber.
 */
var testPubSubDataEvents = function(subscriber, publisher) {

    // makeshift function overloading so this
    // method can test a single node or a pair
    if (publisher === undefined) {
        publisher = subscriber;
    }

    assert.eq(publisher.getName(), subscriber.getName(),
              'the parameters to testPubSubDataEvents must point to the same database');

    // documents used for this test
    var oldDoc = {_id: 1, text: 'hello'};
    var newDoc = {_id: 1, text: 'goodbye'}

    // clean up collection used for this test
    publisher.pubsub.remove(oldDoc);
    publisher.pubsub.remove(newDoc);

    // subscribe to all events on the publisher DB's pubsub collection
    var eventSub = subscriber.pubsub.subscribeToChanges();
    var res, msg;



    // inserts:
    // - do an insert
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the correct document
    assert.writeOK(publisher.pubsub.save(oldDoc));

    assert.soon(function() {
        res = eventSub.poll();
        return res.messages[eventSub.getId().str] !== undefined;
    });

    assertMessageCount(res, eventSub, '$events', 1);
    msg = res.messages[eventSub.getId().str]['$events'][0];
    var insertDoc = {
        db: publisher.getName(),
        collection: publisher.pubsub.getName(),
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
    assert.writeOK(publisher.pubsub.save(newDoc));

    assert.soon(function() {
        res = eventSub.poll();
        return res.messages[eventSub.getId().str] !== undefined;
    });

    assertMessageCount(res, eventSub, '$events', 1);
    var msg = res.messages[eventSub.getId().str]['$events'][0];
    var updateDoc = {
        db: publisher.getName(),
        collection: publisher.pubsub.getName(),
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
    assert.writeOK(publisher.pubsub.remove(newDoc));

    assert.soon(function() {
        res = eventSub.poll();
        return res.messages[eventSub.getId().str] !== undefined;
    });

    assertMessageCount(res, eventSub, '$events', 1);
    msg = res.messages[eventSub.getId().str]['$events'][0];
    var removeDoc = {
        db: publisher.getName(),
        collection: publisher.pubsub.getName(),
        type: "remove",
        doc: newDoc
    };
    assert.eq(msg, removeDoc);


    // clean up subscription
    eventSub.unsubscribe();
}

var assertMessageCount = function(res, subscription, channel, count) {
    var subscriptionId = subscription.getId();
    var channelMessages = res.messages[subscriptionId.str][channel];
    assert.neq(channelMessages, undefined);
    assert.eq(channelMessages.length, count);
}
