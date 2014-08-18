// load pubsub database events functions
assert(load('jstests/libs/dbevents.js'));

var kDBEventsDisabled = 18560;

// enable data events and ensure data events work
db.adminCommand({setParameter: 1, publishDataEvents: true});
testPubSubDataEvents(db);

// subscribe to data events before disabling them
var ps = db.PS();
var channel = '$events';
var namespace = db + ".pubsub"
var filter = {namespace: namespace};
var eventSub = ps.subscribe(channel, filter);

// disable data events
db.adminCommand({setParameter: 1, publishDataEvents: false});

// ensure that subscriptions to data events fail. use db.runCommand here
// to bypass shell-level checks
assert.commandFailedWithCode(db.runCommand({subscribe: channel, filter: filter}),
                             kDBEventsDisabled);

// ensure that there are no data events
var res = ps.poll(eventSub);
assert.eq(res.messages, {});

// insert and remove a document
var doc = {_id: 0, hello: 'world'};
db.pubsub.insert(doc)
db.pubsub.remove(doc);

// ensure that there are no data events
res = ps.poll(eventSub);
assert.eq(res.messages, {});

// clean up subscription
ps.unsubscribe(eventSub);

// enable data events and ensure data events work
db.adminCommand({setParameter: 1, publishDataEvents: true});
testPubSubDataEvents(db);
