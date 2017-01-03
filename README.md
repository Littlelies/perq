perq
=====

Persistent, named queues for Erlang.

## Add queue
```
-spec perq:add_queue(atom()) -> ok | {error, any()}.
```
## Add a binary to a queue
```
-spec enq(atom(), binary()) -> ok | {error, unknown | restarting | any()}.
```
## Get oldest item from the queue
```
-spec deq(atom()) -> binary() | empty | {error, unknown | restarting | any()}.
```
## Get oldest item from the queue BUT don't remove it (convenient to make sure we only remove items from queue when a job is done for example)
```
-spec predeq(atom()) -> binary() | empty | {error, unknown | restarting | any()}.

```
Build
-----

    $ rebar3 compile
