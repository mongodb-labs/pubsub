// load pubsub database events functions
assert(load('jstests/libs/dbevents.js'));

// start a cluster with 2 mongoses
var st = new ShardingTest({name: 'pubsubClusterDbEvents', mongos: 1, config: 1, shards: 1, rs: {nodes: 1}});
// var st = new ShardingTest({name: 'pubsubClusterDbEvents', mongos: 2, rs: {nodes: 1}});

var db0 = st.s0.getDB('test');
// var db1 = st.s1.getDB('test');

// each mongos can communicate with itself
assert(receiveDbEvents(db0));
// assert(receiveDbEvents(db1));

// // event notifications work between the nodes
// assert(receiveDbEvents(db0, db1));
// assert(receiveDbEvents(db1, db0));

st.stop();
