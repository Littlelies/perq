perq
=====

Persistent, named queues for Erlang.

## Add queue
```
-spec perq:add_queue(atom()) -> ok | {error, any()}.
```
## Add a binary to a named queue
```
-spec enq(atom(), binary()) -> ok | {error, unknown | restarting | any()}.
```
## Remove and get oldest binary from the queue
```
-spec deq(atom()) -> binary() | {cut, integer(), binary()} | empty | {error, unknown | restarting | any()}.
```
## If you want to safely complete a task with a binary before dequeuing it, one at a time, use
```
-spec predeq(atom()) -> binary() | {cut, integer(), binary()} | empty | {error, unknown | restarting | any()}.

```
## If you want to safely complete a task with a binary before dequeuing it, multiple ones at a time, use multiple queues and use this to move them safely from one queue to the other
```
-spec move(atom(), atom()) -> binary() | {cut, integer(), binary()} | empty | {error, unknown | restarting | any()}.

```

Build
-----

    $ rebar3 compile

Contribute
-----
    Look at @todo in the code.
    For example:
    - improve the read performance by caching the look ahead data
    - improve tests coverage (94% so far)
