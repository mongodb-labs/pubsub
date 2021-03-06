// load pubsub database events functions
assert(load('jstests/libs/dbevents.js'));

var kDBEventsDisabled = 18560;

// enable data events and ensure data events work
db.adminCommand({setParameter: 1, publishDataEvents: true});
testPubSubDataEvents(db);

// subscribe to data events before disabling them
var eventSub = db.pubsub.subscribeToChanges();

// disable data events
db.adminCommand({setParameter: 1, publishDataEvents: false});

// ensure that there are no data events
var res = eventSub.poll();
assert.eq(res.messages, {});

// insert and remove a document
var doc = {_id: 0, hello: 'world'};
db.pubsub.insert(doc)
db.pubsub.remove(doc);

// ensure that there are no data events
res = eventSub.poll();
assert.eq(res.messages, {});

// clean up subscription
eventSub.unsubscribe();

// enable data events and ensure data events work
db.adminCommand({setParameter: 1, publishDataEvents: true});
testPubSubDataEvents(db);
