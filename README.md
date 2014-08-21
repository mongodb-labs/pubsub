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

- Many use cases call for both a database and pub/sub, and combining the two reduces the number of components in the application stack. It also speeds up development, since the syntax, setup, and maintenance are shared.
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

We implemented four database commands for pub/sub: `publish`, `subscribe`, `poll`, and `unsubscribe`:


### Publish

```
{ publish : <channel>, message : <message> }
```

#####Arguments:

`channel` Required. Must be a string.

`message` Required. Must be an document.

#####Return:

`{ok : 1}`

#####Note:

The channel `$events` is reserved for database event notifications and will return an error if a user attempts to publish to it.



### Subscribe

```
{ subscribe : <channel> }
```

#####Arguments:

`channel` Required. Must be a string. Channel matching is handled by ZeroMQ's prefix-matching.

#####Return:

`{ subscriptionId : <ObjectId> }`

The SubscriptionId (of type ObjectId) returned is used to poll from the subscription or unsubscribe from the subscription.

#####Note:

As of now, SubscriptionId's are insecure, meaning any client can poll from a subscriptionId once it is issued. Care should be taken that only one client polls from each subscriptionId (however, any number of clients may be polling from the same _channel_ on different _subscriptionId's_).



### Poll

```
{ poll : <subscriptionId(s)>, timeout : <timeout> }
```

#####Arguments:

`subscriptionId` Required. Must be an ObjectId or array of ObjectIds.

`timeout` Optional. Must be an Int, Long, or Double (Double gets truncated). Specifies the number of milliseconds to wait on the server if no messeges are immediately available. If no timeout is specified, the default is to return immediately.

#####Return:

A document of the form:

```
{ messages : 
  { <subscriptionId1> : 
      { <channelA> : [ <messages> ],
        <channelB> : [ <messages> ]
      },
    <subscriptionId2> : 
      { <channelA> : [ <messages> ],
        <channelC> : [ <messages> ]
      }
  }
}
```
Therefore, when passing an array of SubscriptionIds, messages are grouped first by SubscriptionId, then by channel (in case the subscription applies to multiple channels due to prefix-matching).

#####Note:

In the event that an array is passed and not all array members are _ObjectIds_, this command will fail and no messages will be received on any subscription.

In the event that an array is passed and an ObjectId is not a _valid subscription_, an error string will be appended to result.errors[invalid ObjectId].



### Unsubscribe

```
{ unsubscribe : <subscriptionId(s)> }
```

#####Arguments:

`subscriptionId` Required. Must be an ObjectId or array of ObjectIds.

#####Return:
`{ ok : 1 }`

#####Note:

Same notes as for poll.


##Shell Helper

We additionally implemented four helper commands for the Mongo shell in javascript. These are accessible through the `ps` object in the Mongo shell.

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
ps.poll(subscription.getId(), [timeout])
```

Further, the ps.poll helper can take an array:
```
ps.poll([ subscriptionId1.getId(), subscriptionId2.get(), ... ], [timeout])
```

Similarly, the Subscription object can be used to unsubscribe in two ways:
```
subscription.unsubscribe()
ps.unsubscribe(subscription.getId())
```

And the ps.unsubscribe helper can also take an array:
```
ps.unsubscribe([ subscriptionId1.getId(), subscriptionId2.get(), ... ], [timeout])
```

All together, a simple script would look like:

```javascript
> var ps = db.PS()
> var subscription = ps.subscribe(channel) // returns a Subscription object
> ps.publish(channel, message)
> subscription.poll([timeout]) // returns message
> subscription.unsubscribe()
```

# Features

In addition to core pub/sub functionality (publish, subscribe, poll, unsubscribe), we implemented the following features:

###Filters and Projections

- `filter` Optional. Must be a document. Specifies a filter to apply to incoming messages.
- `projection` Optional. Must be a document of the form . Specifies fields of incoming messages to return.

Filters and projections in pubsub have the same syntax as the query and projection fields of a read command. See [here](http://docs.mongodb.org/manual/tutorial/query-documents/) for documentation on filter syntax and [here](http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/) for documentation on projection syntax.

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
