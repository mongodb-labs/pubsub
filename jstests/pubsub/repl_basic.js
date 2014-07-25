// Load shared pubsub functions
assert(load('jstests/libs/pubsub.js'));

/**
 * Tests whether pubsub communication works within a replica set works
 * when starting an entire replica set immediately
 **/

// start a replica set with 3 nodes
var rs = new ReplSetTest({name: 'pubsub', nodes: 3});
var nodes = rs.startSet();
rs.initiate();

var db0 = nodes[0].getDB('test');
var db1 = nodes[1].getDB('test');
var db2 = nodes[2].getDB('test');

// each node can publish and receive to itself
assert(selfWorks(db0));
assert(selfWorks(db1));
assert(selfWorks(db2));

// each node can pairwise communicate
assert(pairWorks(db0, db1));
assert(pairWorks(db1, db2));
assert(pairWorks(db2, db0));

rs.stopSet();
