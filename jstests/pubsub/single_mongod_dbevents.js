// Make sure to start a mongod with --setParameter publishDataEvents=1

// load pubsub database events functions
assert(load('jstests/libs/dbevents.js'));

testPubSubDataEvents(db);
