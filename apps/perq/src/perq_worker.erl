-module(perq_worker).

-include_lib("kernel/include/file.hrl").

-export([start_link/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
    ]).

-define(QUEUE_FILENAME, "./perq_data/queue_").
-record(state, {
    write_fd :: file:fd(),
    write_position,
    read_fd :: file:fd(),
    read_position
}).

%% @todo: store in first bytes the location of write and read
%% @todo: when file exceeds a given size, use a new file
%% @todo: when reads ends a file, delete it

%%====================================================================
%% API functions
%%====================================================================

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Name]) ->
    lager:info("[spread_queue] Init started for ~p", [Name]),
    case file:open(?QUEUE_FILENAME ++ atom_to_list(Name), [read, write, binary, raw]) of
        {ok, Fd} ->
            lager:info("[spread_queue] Init done for ~p", [Name]),
            erlang:send_after(60 * 1000, self(), {gc}),
            case file:pread(Fd, 0, 8) of
                eof ->
                    Write = 8,
                    Read = 8,
                    file:pwrite(Fd, 0, <<Read:32, Write:32>>);
                {ok, <<Read:32, Write:32>>} ->
                    lager:info("[perq ~p] Read position ~p, write position ~p", [Name, Read, Write]),
                    ok
            end,
            {ok, #state{write_fd = Fd, write_position = Write, read_fd = Fd, read_position = Read}};
        {error, Reason} ->
            lager:error("[spread_queue] Can't start spread_queue, error occured loading the queue file: ~p", [Reason]),
            {stop, Reason}
    end.


handle_call({enq, Binary}, _From, State) ->
    %% Write into file
    WritePosition = State#state.write_position,
    file:pwrite(State#state.write_fd, WritePosition, <<(size(Binary)):32, Binary/binary>>),
    NewWritePosition = WritePosition + 4 + size(Binary),
    file:pwrite(State#state.write_fd, 4, <<NewWritePosition:32>>),
    {reply, ok, State#state{write_position = NewWritePosition}};
handle_call({deq}, _From, State) ->
    %% Read from file
    ReadPosition = State#state.read_position,
    case file:pread(State#state.write_fd, ReadPosition, 64000) of
        eof ->
            {reply, empty, State};
        {ok, <<Size:32, Binary/binary>>} ->
            NewReadPosition = ReadPosition + 4 + Size,
            file:pwrite(State#state.write_fd, 0, <<NewReadPosition:32>>),
            {reply, binary:part(Binary, {0, Size}), State#state{read_position = NewReadPosition}}
    end;
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({gc}, State) ->
    erlang:garbage_collect(self()),
    erlang:send_after(60 * 1000, self(), {gc}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    file:close(State#state.write_fd),
    file:close(State#state.read_fd),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
