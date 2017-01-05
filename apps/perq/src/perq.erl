%%%-------------------------------------------------------------------
%% @doc perq top level supervisor and API
%% Maximum element size: 64KB
%% @end
%%%-------------------------------------------------------------------

-module(perq).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    add_queue/1,
    enq/2,
    deq/1,
    predeq/1,
    move/2
    ]).

%% Supervisor callbacks
-export([init/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(ROOT_CONFIG_FILE, "./perq_data/config_test").
-else.
-define(ROOT_CONFIG_FILE, "./perq_data/config").
-endif.
%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec add_queue(atom()) -> ok | {error, any()}.
add_queue(Name) ->
    Child = {Name, {perq_worker, start_link, [Name]}, permanent, 5000, worker, [perq_worker]},
    case supervisor:start_child(?MODULE, Child) of
        {ok, ChildPid} ->
            append_term(?ROOT_CONFIG_FILE, Child),
            ChildPid;
        Error ->
            {error, Error}
    end.

-spec enq(atom(), binary()) -> ok | {error, unknown | restarting | any()}.
enq(Name, Binary) ->
    find_child_and_call(Name, {enq, Binary}).

-spec deq(atom()) -> binary() | {cut, integer(), binary()} | empty | {error, unknown | restarting | any()}.
deq(Name) ->
    find_child_and_call(Name, {deq, false}).

-spec predeq(atom()) -> binary() | {cut, integer(), binary()} | empty | {error, unknown | restarting | any()}.
predeq(Name) ->
    find_child_and_call(Name, {deq, true}).

-spec move(atom(), atom()) -> binary() | {cut, integer(), binary()} | empty | {error, unknown | restarting | any()}.
move(Name1, Name2) ->
    find_child_and_call(Name1, {move, Name2}).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init(_Args) ->
    filelib:ensure_dir(?ROOT_CONFIG_FILE), %% In place editing    
    Children = case read_terms(?ROOT_CONFIG_FILE) of
        {error, _} ->
            [];
        Terms ->
            Terms
    end,
    {ok, { {one_for_one, 5, 1}, Children} }.

%%====================================================================
%% Internal functions
%%====================================================================

append_term(FileName, Term) ->
    Formatted = io_lib:format("~tp.~n", [Term]),
    {ok, IoDevice} = file:open(FileName, [read, append]),
    file:write(IoDevice, Formatted),
    file:close(IoDevice).

read_terms(FileName) ->
    case file:consult(FileName) of
        {ok, Terms} ->
            Terms;
        {error, Reason} ->
            {error, Reason}
    end.

find_child_and_call(Name, CallMessage) ->
    Children = supervisor:which_children(?MODULE), %% simple call to supervisor state, plus a lists:map.
    case lists:keyfind(Name, 1, Children) of
        false ->
            {error, unknown};
        {_Id, restarting, _Type, _Modules} ->
            {error, restarting};
        {_Id, ChildPid, _Type, _Modules} ->
            gen_server:call(ChildPid, CallMessage)
    end.

-ifdef(TEST).
perq_test() ->
    file:delete("./perq_data/queue_test__0000"),
    file:delete("./perq_data/queue_test__0001"),
    file:delete("./perq_data/queue_test__0002"),
    file:delete("./perq_data/queue_test__0003"),
    file:delete("./perq_data/config_test"),
    application:start(perq),
    add_queue(test),
    enq(test, <<"toto">>),
    enq(test, <<"toti">>),
    ?assertEqual(deq(test), <<"toto">>),
    ?assertEqual(predeq(test), <<"toti">>),
    ?assertEqual(deq(test), <<"toti">>),
    ?assertEqual(deq(test), empty),
    ?assertEqual(deq(test), empty),

    enq(test, <<"tot0">>),
    enq(test, <<"tot1">>),
    enq(test, <<"tot2">>),
    enq(test, <<"tot3">>),
    enq(test, <<"tot4">>),
    enq(test, <<"tot5">>),
    application:stop(perq),
    application:start(perq),
    ?assertEqual(deq(test), <<"tot0">>),
    ?assertEqual(deq(test), <<"tot1">>),
    ?assertEqual(deq(test), <<"tot2">>),
    ?assertEqual(deq(test), <<"tot3">>),
    enq(test, <<"tot6">>),
    ?assertEqual(deq(test), <<"tot4">>),
    ?assertEqual(deq(test), <<"tot5">>),
    ?assertEqual(deq(test), <<"tot6">>),
    ?assertEqual(deq(test), empty),
    application:stop(perq).

perq_restart_test() ->
    application:start(perq),
    add_queue(test), %% Unique queue test
    application:stop(perq).

perq_move_test() ->
    file:delete("./perq_data/queue_test__0000"),
    file:delete("./perq_data/queue_test2__0000"),
    application:start(perq),
    add_queue(test),
    add_queue(test2),
    enq(test, <<"move">>),
    enq(test, <<"move2">>),
    enq(test, <<"move3">>),
    ?assertEqual(move(test, test2), <<"move">>),
    ?assertEqual(deq(test2), <<"move">>),
    ?assertEqual(deq(test2), empty),
    ?assertEqual(move(test, test), <<"move2">>),
    ?assertEqual(deq(test), <<"move3">>),
    ?assertEqual(deq(test), <<"move2">>),
    ?assertEqual(deq(test), empty),
    ?assertEqual(move(test, test), empty),
    application:stop(perq).

-endif.
