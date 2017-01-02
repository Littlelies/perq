perq
=====

Persistent, named queues for Erlang.
Right now, the file storing a queue keeps growing until 2GB, and then it stops working. The plan is to fix this of course.

## Add queue
```
-spec perq:add_queue(atom()) -> ok | {error, any()}.
```
## Add a binary to a queue
```
-spec perq:enq(atom(), binary()) -> ok.
```
## Get oldest item from the queue
```
-spec perq:deq(atom()) -> binary() | empty.
```

Build
-----

    $ rebar3 compile
