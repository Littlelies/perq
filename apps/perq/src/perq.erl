%%%-------------------------------------------------------------------
%% @doc perq top level supervisor and API
%% Maximum element size: 64KB
%% Maximum file size: 2GB
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
    predeq/1
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

-spec deq(atom()) -> binary() | empty | {error, unknown | restarting | any()}.
deq(Name) ->
    find_child_and_call(Name, {deq, false}).

-spec predeq(atom()) -> binary() | empty | {error, unknown | restarting | any()}.
predeq(Name) ->
    find_child_and_call(Name, {deq, true}).


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
    Children = supervisor:which_children(?MODULE),
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
    file:delete("./perq_data/queue_test.0000"),
    file:delete("./perq_data/config_test"),
    application:start(perq),
    add_queue(test),
    enq(test, <<"toto">>),
    enq(test, <<"toti">>),
    ?assert(deq(test) =:= <<"toto">>),
    ?assert(predeq(test) =:= <<"toti">>),
    ?assert(deq(test) =:= <<"toti">>),
    ?assert(deq(test) =:= empty),
    ?assert(deq(test) =:= empty),

    enq(test, <<"tot0">>),
    enq(test, <<"tot1">>),
    enq(test, <<"tot2">>),
    enq(test, <<"tot3">>),
    enq(test, <<"tot4">>),
    enq(test, <<"tot5">>),
    ?assert(deq(test) =:= <<"tot0">>),
    ?assert(deq(test) =:= <<"tot1">>),
    ?assert(deq(test) =:= <<"tot2">>),
    ?assert(deq(test) =:= <<"tot3">>),
    ?assert(deq(test) =:= <<"tot4">>),
    ?assert(deq(test) =:= <<"tot5">>),
    application:stop(perq).

perq_restart_test() ->
    application:start(perq),
    add_queue(test), %% Unique queue test
    application:stop(perq).

-endif.
