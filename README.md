MongoDB + Pub/sub
=================

Welcome to [MongoDB](https://github.com/mongodb/mongo)! This is an implementation of publish/subscribe within MongoDB v2.6.3 using [ZeroMQ](http://zeromq.org). This is a MongoDB summer 2014 intern project by [Alex Grover](https://github.com/ajgrover) and [Esha Maharishi](https://github.com/EshaMaharishi).

Note: this is a prototype and is _not_ production ready.

Building
--------

See docs/building.md or check out the [documentation](http://www.mongodb.org/about/contributors/tutorial/build-mongodb-from-source/).

Drivers
-------

An example node.js driver is available [here](https://github.com/ajgrover/node-mongodb-pubsub). This driver provides access to all the server functionality implemented here, including filters, projections, and database event notifications.

Motivation
----------

Publish/subscribe abstracts the routing and delivery aspects of communication into its own layer, allowing nodes to pass information without the added complexity of initiating and maintaining connections to receivers. A node can simply "publish" information to a channel on MongoDB, another node can "subscribe" to that channel, and MongoDB handles routing and delivering the information.

Using pub/sub within MongoDB has many benefits:

- There are many use cases for both a database and pub/sub, and combining the two reduces the number of components in the application stack. It also speeds up development because the syntax, setup, and maintenance are shared.
- Pub/sub in MongoDB benefits from the existing power of Mongo, such as allowing messages to be structured documents rather than plain strings and using the exact same query syntax to filter messages on channels as to query documents in the database.
- Pub/sub can be used to deliver information about changes to the database to subscribers in real-time. This is something that cannot be accomplished with an external pub/sub system, but is enabled by the implementation of pub/sub within the database.

Design
------

### Considerations

The behavior of the pub/sub system was designed to closely align with existing behaviors of reads (for subscriptions) and writes (for publishes) in MongoDB and to provide a simple and logical interface to application developers.

Additionally, the system is designed to need no stricter requirements on connections between servers than already exist for replication and sharding, allowing pub/sub to be integrated in existing production environments.

### Architecture

On a standalone mongod server, all messages published to the instance will be sent to all subscribers on the instance.

In a replica set, all messages published to any node in the set will be sent to subscribers on all nodes in the set. In this way, a replica set is logically equivalent to a single server. Additionally, interactions with a single server do not need to be changed if and when the server is added to a replica set.

In a sharded cluster, all messages published to any mongos will be sent to subscribers on all mongoses. However, messages published to a mongod in a shard will _not_ be sent to subscribers on mongoses, and vice versa. In this way, the logical entry point for pub/sub within a cluster is _only_ through mongos instances. Therefore, interactions with a replica set do not need to be changed if and when the set is added to a cluster.

### ZeroMQ

[ZeroMQ](http://zeromq.org) is a standalone socket library that provides the tools to implement common messaging patterns across distributed systems. Rather than dictating an architecture, ZeroMQ allows the implementation of a brokerless internal communication system for replica sets on top of ZeroMQ’s socket API, but a centralized architecture for sharded clusters using the same simple API. In addition, ZeroMQ's pub/sub sockets encapsulate well-tested functionality regarding channel matching and message buffering, with well-defined behavior in case of an error.

API + Documentation
===================

Pub/sub is implemented through four database commands: `publish`, `subscribe`, `poll`, and `unsubscribe`.

### Publish

```
{ publish: <channel>, message: <message> }
```

**Arguments:**

- `channel` Required. Must be a string.
- `message` Required. Must be a document.

**Return:**

`{ok: 1}`

**Note:**

The channel `$events` is reserved for database event notifications and will return an error if a user attempts to publish to it.

### Subscribe

```
{ subscribe: <channel>, filter: <document>, projection: <document> }
```

**Arguments:**

- `channel` Required. Must be a string. Channel matching is handled by ZeroMQ's prefix-matching.

See below for detailed documentation on filters and projections.

**Return:**

`{ subscriptionId: <ObjectId> }`

The subscriptionId (of type ObjectId) returned is used to poll or unsubscribe from the subscription.

**Note:**

As of now, subscriptionId's are insecure, meaning any client can poll from a subscriptionId once it is issued. Care should be taken that only one client polls from each subscriptionId (however, any number of clients may be polling from the same channel on different subscriptionId's).

### Poll

```
{ poll: <subscriptionId(s)>, timeout: <timeout> }
```

**Arguments:**

- `subscriptionId` Required. Must be an ObjectId or array of ObjectIds.
- `timeout` Optional. Must be an Int, Long, or Double (Double gets rounded down). Specifies the number of milliseconds to wait on the server if no messages are immediately available. If no timeout is specified, the default is to return immediately.

**Return:**

A document of the form:

```
{
  messages: { 
    <subscriptionId1>: {
        <channelA>: [ <messages> ],
        <channelB>: [ <messages> ]
      },
    <subscriptionId2>: {
        <channelA>: [ <messages> ],
        <channelC>: [ <messages> ]
      }
  }
}
```
Therefore, when passing an array of SubscriptionIds, messages are grouped first by SubscriptionId, then by channel (in case the subscription applies to multiple channels due to prefix-matching).

**Notes:**

In the event that an array is passed and not all array members are ObjectIds, this command will fail and no messages will be received on any subscription.

In the event that an array is passed and an ObjectId is not a valid subscription, an error string will be appended to result.errors[invalid ObjectId].

### Unsubscribe

```
{ unsubscribe: <subscriptionId(s)> }
```

**Arguments:**

`subscriptionId` Required. Must be an ObjectId or array of ObjectIds.

**Return:**
`{ ok: 1 }`

**Notes:**

See the notes for the poll command.

Shell Helper
------------

Four helper commands for the Mongo shell exist to enable usage of pub/sub. These are accessible through the `ps` object in the Mongo shell. This object is initialized by running `var ps = db.PS()` from the shell.

The publish helper is a straightforward wrapper around the server command:

```
ps.publish(channel, message)
```

The subscribe helper returns a Subscription object which is used to manipulate subscriptions, hiding away the actual SubscriptionId:

```
ps.subscribe(channel) // returns a Subscription object
```

This Subscription object can then be used to poll in two ways:

```
subscription.poll([timeout])
ps.poll([subscription.getId(), ...], [timeout]) // can take a single Id or an array of Ids
```

The Subscription object also provides a forEach method that polls continuously with a 10 second timeout and calls a callback with each response:

```
subscription.forEach(function(res) { printjson(res); })
```

Similarly, the Subscription object can be used to unsubscribe in two ways:

```
subscription.unsubscribe()
ps.unsubscribe([subscription.getId(), ...]) // can also take a single Id or an array of Ids
```

All together, a simple script would look like:

```javascript
> var ps = db.PS()
> var subscription = ps.subscribe(channel) // returns a Subscription object
> ps.publish(channel, message)
> subscription.poll([timeout]) // returns message
> subscription.unsubscribe()
```

Features
========

In addition to core pub/sub functionality (publish, subscribe, poll, unsubscribe), this project implements the following features:

### Filters

Normally, it would be up to some filtering system on the client side to only pass along the interesting documents to the rest of the application. This would not only require each application to build its own filtering logic (or use some third-party library), but would also result in a lot of useless data being transmitted over the network. Since MongoDB already has a powerful and well-known query framework, it makes sense utilize it for applying filters within the pub/sub system.

Filters can be applied to messages on subscriptions in the same way that a query is applied to documents in a collection. The filter is designated through a "filter" field in the subscribe command. For example, the following will return only documents with field `a` greater than 10.

```
{ subscribe : <channel>, filter: { $gt : {a : 10} }
```

See [here](http://docs.mongodb.org/manual/tutorial/query-documents/) for full documentation on MongoDB query syntax.

### Projections

Some applications may know before-hand that they only need specific fields in each message. In this case, they can apply a projection to their subscription, specifying which fields to deliver. 

A projection is specified using the same syntax as projections on MongoDB queries. The projection is designated through a `projection` field in the subscribe command. For example, the following will return only the `type` and `author` fields in each document:

```
{ subscribe : <channel>, projection: { type : 1, author : 1 }
```

See [here](http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/) for full documentation on projection syntax.

**Note:**

Filters and projections can be applied simultaneously:

```
{ subscribe : <channel>, filter: <document>, projection: <document> }
```

### Database Event Notifications

As noted above, one major benefit of having a pub/sub system integrated into MongoDB is the ability to publish database change notifications. The channel `$events` is reserved within the system and used to publish database event notifications.

To enable database notifications, start a mongodb server with the command line option `--setParameter publishDataEvents=1`.

While it is possible to subscribe to the events channel manually, the Mongo shell provides convenient helper methods to get notifications on collections or databases:

```
db.subscribeToChanges([type])
db.collection.subscribeToChanges([type])
```

**Arguments:**

`type` Optional. Specifies the type of events to be notified of. Valid options are the strings 'insert', 'update', and 'remove'.

Performance
===========

Since pub/sub is implemented in MongoDB through the Command interface, the throughput of the pub/sub commands is similar to other database commands.

Below are the commands/second processed by the server under an increasing load of clients. "Light" indicates that the messages being published or polled were about 10 bytes; "heavy" messages were about 400 bytes.

![alt tag](https://raw.githubusercontent.com/10gen-interns/pubsub/master/benchmark.png)

These statistics are comparable to other brokered messaging queue systems.

TODO
====

- Use secure connections for internally propagating messages over ZMQ (Curve or SSL)
- Allow for exact matching channels rather than just ZMQ’s prefix matching
- Add secure access to subscriptions
- Internal system to allow synchronized on/off across cluster
- Only propagate messages internally to nodes who are subscribed
- Allow enable/disable of pubsub at runtime

License
=======

Most MongoDB source files (src/mongo folder and below) are made available under the terms of the GNU Affero General Public License (AGPL).  See individual files for details.

As an exception, the files in the client/, debian/, rpm/, utils/mongoutils, and all subdirectories thereof are made available under the terms of the Apache License, version 2.0.
