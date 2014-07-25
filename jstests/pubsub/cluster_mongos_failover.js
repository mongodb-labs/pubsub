// Load shared pubsub functions
assert(load('jstests/libs/pubsub.js'));

/**
 * Tests whether pubsub communication works within a sharded cluster
 * when shutting down and restarting mongoses
 **/

// start a cluster with 3 mongoses
var st = new ShardingTest({name : 'pubsub', mongos : 3, config : 3});

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

// stop one mongos
st.stopMongos(0);
assert(selfWorks(db1));
assert(selfWorks(db2));
assert(pairWorks(db1, db2));

// stop second mongos
st.stopMongos(1);
assert(selfWorks(db2));

// restart both mongoses
st.restartMongos(0);
db0 = st.s0.getDB('test');
assert(selfWorks(db0));
assert(selfWorks(db2));
assert(pairWorks(db0, db2));

st.restartMongos(1);
db1 = st.s1.getDB('test');

assert(selfWorks(db0));
assert(selfWorks(db1));
assert(selfWorks(db2));

assert(pairWorks(db0, db1));
assert(pairWorks(db1, db2));
assert(pairWorks(db2, db0));

st.stop();
