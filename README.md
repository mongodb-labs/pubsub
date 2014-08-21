MongoDB + Pub/Sub
=================

Welcome to [MongoDB](https://github.com/mongodb/mongo)! This is an implementation of publish/subscribe within MongoDB v2.6.3 using [ZeroMQ](http://zeromq.org), a MongoDB summer 2014 intern project by [Alex Grover](https://github.com/ajgrover) and [Esha Maharishi](https://github.com/EshaMaharishi).

Note: this is a prototype and is _not_ production ready.

##Building

See docs/building.md or navigate to www.mongodb.org and search for "Building".

##Drivers

An example node.js driver is available [here](https://github.com/ajgrover/node-mongodb-pubsub). This driver provides access to all the server functionality implemented here, including filters and projections, and database event notifications.


# Motivation

Publish/subscribe abstracts the routing and delivery aspects of communication into its own layer, allowing nodes to pass information without the added complexity of initiating and maintaining connections to receivers. A node can simply "publish" information to a channel on MongoDB, another node can "subscribe" to that channel, and MongoDB handles routing and delivering the information.

Using pub/sub within MongoDB has many benefits:

- Many use cases call for both a database and pub/sub, and combining the two reduces the number of components in the application stack. It also speeds up developement, since the syntax, setup, and maintenance are shared.
- Pub/sub in MongoDB benefits from the existing power of Mongo, such as allowing messages to be structured documents rather than plain strings, and using the exact same query syntax to filter messages on channels as to query documents in collections.
- Pub/sub can be used to deliver information about changes to the database to subscribers in real-time. This is something that cannot be accomplished with an external pub/sub system, but is an internal implementation built on top of pub/sub.


# Design

###Distributed Design Considerations
We designed the behavior of our pub/sub system to closely align with existing behaviors of writes (for publishes) and reads (for subscriptions) in MongoDB, and to mirror the logical views of replica sets and sharded clusters.

Additionally, our system is designed to need no stricter requirements on connections between servers than already exist for replication and sharding, allowing pub/sub to be simply integrated in existing production environments.

###Distributed Architecture
On a single mongod server, all messages published to the instance will be sent to all subscribers on the instance.

In a replica set, all messages published to any node in the set will be sent to subscribers on all nodes in the set. In this way, a replica set is logically equivalent to a single server. Additionally, interactions with a single server do not need to be changed if and when the server is added to a replica set.

In a sharded cluster, all messages published to any mongos will be sent to subscribers on all mongoses. However, messages published to a mongod in a shard will *NOT* be sent to subscribers on mongoses, and vice versa. In this way, the logical entry point for pub/sub within a cluster is *ONLY* through mongos instances. Therefore, interactions with a replica set do not need to be changed if and when the set is added as a shard in a cluster.

###ZeroMQ
[ZeroMQ](http://zeromq.org) is a standalone socket library that provides the tools to implement common messaging patterns across distributed systems. Rather than dictating a distributed architecture, ZeroMQ allowed us to construct our own communication patterns for different parts of our system, including direct one-to-one communication, pub/sub fan-out across a network, and pub/sub fan-out within a single process.

In particular, we were able to design a broker-less internal communication system for replica sets on top of ZeroMQ’s pub/sub socket API, but a hub-based communication system for sharded clusters using the same simple API. We chose ZeroMQ over related alternatives such as RabbitMQ, because ZMQ is a library rather than a messaging implementation in itself.


# API + Documentation

In addition to MongoDB's basic behavior, we implemented 4 additional database commands: `publish`, `subscribe`, `poll`, and `unsubscribe`.

These are accessible from the Mongo shell through the `ps` variable.

```javascript
> var ps = db.PS()
> var subscription = ps.subscribe(channel, [filter], [projection]) // returns a Subscription object
> ps.publish(channel, message)
> subscription.poll([timeout]) // returns message
> subscription.unsubscribe()
```

The server API for for each of these commands is as follows:

### Publish

Signature:

```
{ publish : <channel>, message : <message> }
```

From the Mongo shell:

```
ps.publish(channel, message)
```

Arguments:

- `channel` Required. Must be a string.
- `message` Required. Must be an object.

Other:

- The channel `$events` is reserved for database event notifications and will return an error if a user attempts to publish to it.

### Subscribe

```
{ subscribe : <channel>, filter : <filter>, projection: <projection> }
```

From the Mongo shell:

```
ps.subscribe(channel, [filter], [projection]) // returns a Subscription
```

Arguments:

- `channel` Required. Must be a string. Channel matching is done by ZeroMQ's prefix matching.
- `filter` Optional. Must be an object. Specifies a filter to apply to incoming messages.
- `projection` Optional. Must be an object. Specifies fields of incoming messages to return.

Filters and projections in pubsub have the same syntax as the query and projection fields of a read command. See [here](http://docs.mongodb.org/manual/tutorial/query-documents/) for documentation on filter syntax and [here](http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/) for documentation on projection syntax.

### Subscriptions

- document subscription object methods
- document shell helper

### Poll

Signature:

```
{ poll : <subscriptionId(s)>, timeout : <timeout> }
```

From the Mongo shell:

```
subscription.poll([timeout])
ps.poll(subscription.getId(), [timeout])
ps.poll([ subscriptionIds ], [timeout])
```

Arguments:

- `subscriptionId` Required. Must be an ObjectId or array of ObjectIds.
- `timeout` Optional. Must be an Int, Long, or Double. Specifies the number of milliseconds to wait on the server if no messeges are available. If the number is a Double, it is rounded down to the nearest integer. If no timeout is specified, the default is to return immediately.

Errors:

- In the event that an array is passed and not all array members are ObjectIds, the command will fail and no messages will be received on any subscription.
- In the event that an array is passed and an ObjectId is not a valid subscription, an error string will be appended to result.errors[invalid ObjectId].

### Unsubscribe

Signature:

```
{ unsubscribe : <subscriptionId(s)> }
```

From the Mongo shell:

```
subscription.unsubscribe()
ps.unsubscribe(subscription.getId())
ps.unsubscribe([subscriptionIds])
```

Arguments:

- `subscriptionId` Required. Must be an ObjectId or array of ObjectIds.

Errors:

- In the event that an array is passed and not all array members are ObjectIds, the command will fail and no subscriptions will be unsubscribed.
- In the event that an array is passed and an ObjectId is not a valid subscription, an error string will be appended to result.errors[invalid ObjectId].


# Features

In addition to core pub/sub functionality (allowing subscribers to subscribe to channels, poll for messages, and unsubscribe; allowing publishers to publish to channels), we implemented the following features:

###Filters and Projections


###Database Event Notifications

- document shell helper

# Performance

- include graphs and numbers here

# TODO

- Use secure connections for internally propagating messages over ZMQ (Curve or SSL)
- Allow for exact matching channels rather than just ZMQ’s prefix matching
- Add secure access to subscriptions
- Internal system to allow synchronized on/off across cluster
- Only propagate messages internally to nodes who are subscribed
- Allow enable/disable of pubsub at runtime

# License
Most MongoDB source files (src/mongo folder and below) are made available under the terms of the GNU Affero General Public License (AGPL).  See individual files for details.

As an exception, the files in the client/, debian/, rpm/, utils/mongoutils, and all subdirectories thereof are made available under the terms of the Apache License, version 2.0.
