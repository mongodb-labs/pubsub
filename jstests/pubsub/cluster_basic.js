// Load shared pubsub functions
assert(load('jstests/libs/pubsub.js'));

// start a cluster with 3 mongoses
var st = new ShardingTest({name: 'pubsubClusterBasic', mongos: 3, config: 3});

var db0 = st.s0.getDB('test');
var db1 = st.s1.getDB('test');
var db2 = st.s2.getDB('test');

// Each mongos can communicate with itself
assert(selfWorks(db0));
assert(selfWorks(db1));
assert(selfWorks(db2));

// each mongos can publish and receive to each other mongos
assert(pairWorks(db0, db1));
assert(pairWorks(db1, db2));
assert(pairWorks(db2, db0));

st.stop();
