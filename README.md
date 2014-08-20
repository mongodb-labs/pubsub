MongoDB + Pub/Sub
=================

Welcome to [MongoDB](https://github.com/mongodb/mongo)! This is an implementation of publish/subscribe on top of MongoDB v2.6.3 using [ZeroMQ](http://zeromq.org). A MongoDB summer 2014 intern project by [Alex Grover](https://github.com/ajgrover) and [Esha Maharishi](https://github.com/EshaMaharishi).

Note: this is a prototype and is _not_ production ready.

# Building

See docs/building.md or navigate to www.mongodb.org and search for "Building".

# Drivers

An example node.js driver is available [here](https://github.com/ajgrover/node-mongodb-pubsub). This driver provides access to all the additional functionality implemented here.

# Design

- why ZeroMQ?
- design considerations
- where we are today

# Features

- regular pubsub
- filters/projections
- database event notifications

# API Documentation

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

## Publish

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

## Subscribe

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

### Database Events

- document channels and behavior, setParameter

## Poll

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

## Unsubscribe

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

## Database Events

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
