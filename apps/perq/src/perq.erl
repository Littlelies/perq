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
    deq/1
    ]).

%% Supervisor callbacks
-export([init/1]).

-define(ROOT_CONFIG_FILE, "./perq_data/config").

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
    find_child_and_call(Name, {deq}).

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
