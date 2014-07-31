/**
 * Tests whether pubsub db event notifications
 * work within a replica set
 **/

// load pubsub database events shared functions
assert(load('jstests/libs/dbevents.js'));

// start a replica set with 3 nodes
var rs = new ReplSetTest({name: 'pubsubReplDbEvents', nodes: 3});
var nodes = rs.startSet();
rs.initiate();

var primary = rs.getPrimary().getDB('test');
var secondary = rs.getSecondary().getDB('test');

// events work between the two nodes
assert(receiveDbEvents(primary, secondary));

rs.stopSet();
