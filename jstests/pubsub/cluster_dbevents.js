// load pubsub database events functions
assert(load('jstests/libs/dbevents.js'));

// start a cluster with 3 mongoses
var st = new ShardingTest({name: 'pubsubClusterDbEvents', mongos: 3, config: 3});

var db0 = st.s0.getDB('test');
var db1 = st.s1.getDB('test');
var db2 = st.s2.getDB('test');

// each mongos can communicate with itself
testPubSubDataEvents(db0);
testPubSubDataEvents(db1);
testPubSubDataEvents(db2);

// event notifications work between the nodes
testPubSubDataEvents(db0, db1);
testPubSubDataEvents(db1, db0);
testPubSubDataEvents(db0, db2);
testPubSubDataEvents(db2, db0);
testPubSubDataEvents(db1, db2);
testPubSubDataEvents(db2, db1);

st.stop();
