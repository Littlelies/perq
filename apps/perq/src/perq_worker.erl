%%%-------------------------------------------------------------------
%% @doc perq_worker is the gen_server per queue
%% @end
%%%-------------------------------------------------------------------

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
-define(INDEX_BITSIZE, 24).
-define(INDEX_SIZE, 3). %% BITSIZE/8

-ifdef(TEST).
-define(MAX_FILE_SIZE, 11).
-else.
-define(MAX_FILE_SIZE, 16777216). %% 2 ^ BITSIZE
-endif.

-record(state, {
    name :: atom(),
    root_filemane :: list(),
    write_filename :: list(),
    write_fd :: file:fd(),
    write_position,
    read_filename :: list(),
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
    State = init_state(Name),
    erlang:send_after(60 * 1000, self(), {gc}),
    {ok, State}.

handle_call({enq, Binary}, _From, State) ->
    enqueue(Binary, State);
handle_call({deq, JustLook}, _From, State) ->
    dequeue(State, JustLook);
handle_call({move, NewQueue}, _From, State) ->
    {reply, Answer, NewState} = dequeue(State, true),
    case Answer of
        empty ->
            {reply, empty, NewState};
        _ ->
            case State#state.name of
                NewQueue ->
                    {reply, ok, NewState2} = enqueue(Answer, State),
                    dequeue(NewState2, false);
                _ ->
                    case perq:enq(NewQueue, Answer) of
                        ok ->
                            dequeue(NewState, false);
                        Any ->
                            {reply, Any, NewState}
                    end
            end
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

init_state(Name) ->
    FileName = ?QUEUE_FILENAME ++ atom_to_list(Name),
    RootFileName = FileName ++ "__0000",

    FileNames = lists:sort(filelib:wildcard(FileName ++ "__*")),
    case FileNames of
        [] ->
            ReadFileName = RootFileName,
            WriteFileName = RootFileName;
        [OneFileName] ->
            ReadFileName = OneFileName,
            WriteFileName = increment_filename(OneFileName); %% To avoid inconsistent queues, we use a new file for writing new items
        _ ->
            {ReadFileName, WriteFileName0} = find_right_boundaries(FileNames),
            WriteFileName = increment_filename(WriteFileName0) %% To avoid inconsistent queues, we use a new file for writing new items
    end,

    io:format("FileNames are ~p\n", [{ReadFileName, WriteFileName}]),

    {ok, ReadFd, ReadPosition} = open(ReadFileName),

    WritePosition = ?INDEX_SIZE,
    if
        ReadFileName =:= WriteFileName ->
            {ok, FileInfo} = file:read_file_info(WriteFileName),
            WritePosition = FileInfo#file_info.size,            
            WriteFd = ReadFd;
        true ->
            {ok, WriteFd, WritePosition} = open(WriteFileName)
    end,
    #state{
        name = Name,
        root_filemane = RootFileName,
        write_filename = WriteFileName, write_fd = WriteFd, write_position = WritePosition,
        read_filename = ReadFileName, read_fd = ReadFd, read_position = ReadPosition
    }.

open(FileName) ->
    case file:open(FileName, [read, write, binary, raw]) of
        {ok, Fd} ->
            case file:pread(Fd, 0, ?INDEX_SIZE) of
                eof ->
                    %% Empty file, create it with right metadata
                    Read = ?INDEX_SIZE,
                    ok = file:pwrite(Fd, 0, <<Read:?INDEX_BITSIZE>>);
                {ok, <<Read:?INDEX_BITSIZE>>} ->
                    ok
            end,
            {ok, Fd, Read};
        {error, Reason} ->
            {error, Reason}
    end.

increment_filename(FileName) ->
    [Root, OccurenceAsList] = re:split(FileName, "__", [{return, list}]),
    Occurence = (list_to_integer(OccurenceAsList) + 1) rem 10000,
    Root ++ "__" ++ lists:flatten(io_lib:format("~4..0w", [Occurence])).

dequeue(State, JustLook) ->
    ReadFileName = State#state.read_filename,
    ReadPosition = State#state.read_position,
    RootFileName = State#state.root_filemane,
    %% Read from file
    case file:pread(State#state.read_fd, ReadPosition, 64000) of
        eof ->
            case {State#state.read_filename, ReadPosition} of
                {RootFileName, ?INDEX_SIZE} ->
                    %% Nothing in queue, nothing to be flushed
                    {reply, empty, State};
                _ ->
                    %% Nothing more to be read, let delete that file
                    file:close(State#state.read_fd),
                    file:delete(State#state.read_filename),
                    case State#state.write_filename of
                        ReadFileName ->
                            %% Writer is at same position: queue is empty! Let reset all files
                            NewState = init_state(State#state.name),
                            {reply, empty, NewState};
                        _ ->
                            %% Writer is on another file, open the next one and try again to dequeue
                            NewFileName = increment_filename(State#state.read_filename),
                            case State#state.write_filename of
                                NewFileName ->
                                    Fd = State#state.write_fd;
                                _ ->
                                    {ok, Fd, ?INDEX_SIZE} = open(NewFileName)
                            end,
                            dequeue(State#state{read_filename = NewFileName, read_fd = Fd, read_position = ?INDEX_SIZE}, JustLook)
                    end
            end;
        {ok, <<Size:?INDEX_BITSIZE, Binary/binary>>} ->
            case JustLook of
                true ->
                    NewReadPosition = ReadPosition;
                false ->
                    NewReadPosition = ReadPosition + ?INDEX_SIZE + Size,
                    ok = file:pwrite(State#state.read_fd, 0, <<NewReadPosition:?INDEX_BITSIZE>>)
            end,
            %% @todo: keep extra bytes for next reads, it will help reduce the number of OS calls
            ReadSize = size(Binary),
            if
                ReadSize < Size ->
                    {reply, {cut, Size, binary:part(Binary, {0, ReadSize})}, State#state{read_position = NewReadPosition}};
                true ->
                    {reply, binary:part(Binary, {0, Size}), State#state{read_position = NewReadPosition}}
            end
    end.

enqueue(Binary, State) ->
    %% Write into file
    WritePosition = State#state.write_position,
    ToWrite = <<(size(Binary)):?INDEX_BITSIZE, Binary/binary>>,
    ok = file:pwrite(State#state.write_fd, WritePosition, ToWrite),

    %% Update write position
    NewWritePosition = WritePosition + size(ToWrite),
    if
        NewWritePosition > ?MAX_FILE_SIZE ->
            %% Maybe close current file
            WriteFd = State#state.write_fd,
            case State#state.read_fd of
                WriteFd ->
                    dont_close;
                _ ->
                    file:close(WriteFd)
            end,
            %% Create a new file
            NewFileName = increment_filename(State#state.write_filename),
            {ok, Fd, _Read} = open(NewFileName),
            {reply, ok, State#state{write_filename = NewFileName, write_fd = Fd, write_position = ?INDEX_SIZE}};
        true ->
            {reply, ok, State#state{write_position = NewWritePosition}}
    end.

find_right_boundaries([A | Next]) ->
    find_right_boundaries(Next, A, A).

find_right_boundaries([], Prev, First) ->
    {First, Prev};
find_right_boundaries([A | Next], Prev, First) ->
    case increment_filename(Prev) of
        A ->
            find_right_boundaries(Next, A, First);
        _ ->
            {A, Prev}
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
find_right_boundaries_test() ->
    ?assertEqual(find_right_boundaries(["a__0100","a__0101","a__0102"]), {"a__0100", "a__0102"}),    
    ?assertEqual(find_right_boundaries(["a__0000","a__0001","a__9999"]), {"a__9999", "a__0001"}).
-endif.
