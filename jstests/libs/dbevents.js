/**
 * Shared function to use when testing database event notifications
 * using pubsub. Tests to make sure that write commands on the publisher
 * are received as events on the appropriate channels on the subscriber.
 */
var receiveDbEvents = function(publisher, subscriber) {

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
    var res;



    // inserts:
    // - do an insert
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the correct document
    assert.writeOK(publisher.pubsub.insert({text: 'hello'}));

    assert.soon(function() {
        res = subscriber.runCommand({poll: eventSub});
        return res.messages[eventSub.str] !== undefined;
    });

    assert.eq(res.messages[eventSub.str][insertChannel].length, 1);
    assert.eq(res.messages[eventSub.str][insertChannel][0].text, 'hello');



    // updates:
    // - do an update
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the correct form:
    // {
    //    old: <old document>,
    //    new: <new document>
    // }
    assert.writeOK(publisher.pubsub.update({text: 'hello'}, {text: 'goodbye'}));

    assert.soon(function() {
        res = subscriber.runCommand({poll: eventSub});
        return res.messages[eventSub.str] !== undefined;
    });

    assert.eq(res.messages[eventSub.str][updateChannel].length, 1);
    assert(res.messages[eventSub.str][updateChannel][0].hasOwnProperty('old'));
    assert(res.messages[eventSub.str][updateChannel][0].hasOwnProperty('new'));
    assert.eq(res.messages[eventSub.str][updateChannel][0].old.text, 'hello');
    assert.eq(res.messages[eventSub.str][updateChannel][0]['new'].text, 'goodbye');



    // removes:
    // - do a remove
    // - assert that the subscriber received a single event of the correct type
    // - ensure that the response body had the deleted document
    assert.writeOK(publisher.pubsub.remove({text: 'goodbye'}));

    assert.soon(function() {
        res = subscriber.runCommand({poll: eventSub});
        return res.messages[eventSub.str] !== undefined;
    });

    assert.eq(res.messages[eventSub.str][removeChannel].length, 1);
    assert.eq(res.messages[eventSub.str][removeChannel][0].text, 'goodbye');


    subscriber.runCommand({unsubscribe: eventSub});

    return true;
}
