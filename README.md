perq
=====

Persistent, named queues for Erlang.

## Add queue
Queue name is an atom, use of `__` in the name is forbidden
```
-spec perq:add_queue(atom()) -> ok | {error, any()}.
```
## Add a binary to a named queue
Limited in size to 64000 bytes
```
-spec enq(atom(), binary()) -> ok | {error, unknown | restarting | any()}.
```
## Remove and get oldest binary from the queue
```
-spec deq(atom()) -> binary() | {cut, integer(), binary()} | empty | {error, unknown | restarting | any()}.
```
## Look up next binary without dequeuing it
Useful if you want to safely complete a task with a binary before dequeuing it, one at a time, use
```
-spec predeq(atom()) -> binary() | {cut, integer(), binary()} | empty | {error, unknown | restarting | any()}.

```
## Move next binary from one queue to another
Useful if you want to safely complete a task with a binary before dequeuing it, multiple ones at a time, use multiple queues and use this to move them safely from one queue to the other
```
-spec move(atom(), atom()) -> binary() | {cut, integer(), binary()} | empty | {error, unknown | restarting | any()}.

```

Build
-----

    $ rebar3 compile

Contribute
-----
    Look at @todo in the code.
    - improve the read performance by caching the look ahead data
    - support bigger binaries using auxiliary files
    - when a process gets empty as an answer, send him a message when new binary arrives to avoid polling
    - improve tests coverage (94% so far)
