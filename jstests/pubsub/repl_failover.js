// Load shared pubsub functions
assert(load('jstests/libs/pubsub.js'));

/**
 * Tests whether pubsub communication works within a replica set works
 * when adding and removing members of a replica set, starting with 1 node
 **/

// start a replica set with 1 node
var rs = new ReplSetTest({ name: 'pubsub', nodes: 1 });
rs.startSet();
rs.initiate();

var db0 = rs.getMaster().getDB('test');

// node can publish and receive to itself
assert(selfWorks(db0));

// add a second node
var db1 = rs.add().getDB('test');
rs.reInitiate();

// both nodes can still publish and receive to themselves
assert(selfWorks(db0));
assert(selfWorks(db1));

// first and second nodes can publish and receive from each other
assert(pairWorks(db0, db1));

// remove first node
rs.stop(0);

// second node can still publish and receive to itself
assert(selfWorks(db1));

rs.stopSet();
