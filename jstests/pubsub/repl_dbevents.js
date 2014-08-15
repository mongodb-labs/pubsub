/**
 * Tests whether pubsub db event notifications
 * work within a replica set
 **/

// load pubsub database events shared functions
assert(load('jstests/libs/dbevents.js'));

// start a replica set with 3 nodes and DB events enabled
var rs = new ReplSetTest({name: 'pubsubReplDbEvents', nodes: 3,
                          nodeOptions: {setParameter: 'publishDataEvents=1'}});
var nodes = rs.startSet();
rs.initiate();

var primary = rs.getPrimary().getDB('test');
var secondary = rs.getSecondary().getDB('test');

// events work between the two nodes
testPubSubDataEvents(secondary, primary);

rs.stopSet();
