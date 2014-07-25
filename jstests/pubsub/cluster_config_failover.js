// Load shared pubsub functions
assert(load('jstests/libs/pubsub.js'));

/**
 * Tests whether pubsub communication works within a sharded cluster
 * when shutting down and restarting config servers
 **/

// start a cluster with 3 mongoses
var st = new ShardingTest({name: 'pubsubClusterConfigFailover', mongos: 3, config: 3});

var db0 = st.s0.getDB('test');
var db1 = st.s1.getDB('test');
var db2 = st.s2.getDB('test');

var testMongoses = function(db0, db1, db2) {
    // Each mongos can communicate with itself
    assert(selfWorks(db0));
    assert(selfWorks(db1));
    assert(selfWorks(db2));

    // each mongos can publish and receive to each other mongos
    assert(pairWorks(db0, db1));
    assert(pairWorks(db1, db2));
    assert(pairWorks(db2, db0));
}

// stop one config server
try {
    st.c1.getDB('admin').runCommand({ shutdown: 1 });
} catch(err) {
    print("\n\n\n\nShut down config0: " + err + "\n\n\n\n");
}

testMongoses(db0, db1, db2);

// // stop second config server
// st.c1.getDB('admin').runCommand({ shutdown: 1 });
// testMongoses(db0, db1, db2);

st.stop();
