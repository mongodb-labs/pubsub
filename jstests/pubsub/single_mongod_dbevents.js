// load pubsub database events functions
assert(load('jstests/libs/dbevents.js'));

testPubSubDataEvents(db);
