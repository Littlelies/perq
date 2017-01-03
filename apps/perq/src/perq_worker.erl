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
-define(MAX_FILE_SIZE, 16777216). %% 2 ^ BITSIZE

-record(state, {
    name :: atom(),
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
    %% Write into file
    WritePosition = State#state.write_position,
    ToWrite = <<(size(Binary)):?INDEX_BITSIZE, Binary/binary>>,
    file:pwrite(State#state.write_fd, WritePosition, ToWrite),

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
    end;
handle_call({deq}, _From, State) ->
    dequeue(State);
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

    FileNames = lists:sort(filelib:wildcard(FileName ++ ".*")),
    case FileNames of
        [] ->
            ReadFileName = FileName ++ ".0000",
            WriteFileName = ReadFileName;
        _ ->
            ReadFileName = hd(FileNames),
            WriteFileName = lists:last(FileNames)
    end,

    {ok, ReadFd, ReadPosition} = open(ReadFileName),

    {ok, FileInfo} = file:read_file_info(WriteFileName),
    WritePosition = FileInfo#file_info.size,
    case WriteFileName of
        ReadFileName ->
            WriteFd = ReadFd;
        _ ->
            {ok, WriteFd} = file:open(FileName, [read, write, binary, raw])
    end,
    #state{
        name = Name,
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
                    file:pwrite(Fd, 0, <<Read:?INDEX_BITSIZE>>);
                {ok, <<Read:?INDEX_BITSIZE>>} ->
                    ok
            end,
            {ok, Fd, Read};
        {error, Reason} ->
            {error, Reason}
    end.

increment_filename(FileName) ->
    [Root, OccurenceAsList] = re:split(FileName, "\\.", [{return, list}]),
    Occurence = list_to_integer(OccurenceAsList) + 1,
    Root ++ "." ++ lists:flatten(io_lib:format("~4..0w", [Occurence])).

dequeue(State) ->
    ReadFileName = State#state.read_filename,
    ReadPosition = State#state.read_position,
    %% Read from file
    case file:pread(State#state.read_fd, ReadPosition, 64000) of
        eof ->
            %% Nothing more to be read, let delete that file
            file:close(State#state.read_fd),
            file:delete(State#state.read_filename),
            case State#state.write_filename of
                ReadFileName ->
                    %% Writer is at same position: queue is empty! Let reset files
                    NewState = init_state(State#state.name),
                    {reply, empty, NewState};
                _ ->
                    %% Writer is on another file, open the next one and try again to dequeue
                    NewFileName = increment_filename(State#state.read_filename),
                    {ok, Fd, ?INDEX_SIZE} = open(NewFileName),
                    dequeue(State#state{read_filename = NewFileName, read_fd = Fd, read_position = ?INDEX_SIZE})
            end;
        {ok, <<Size:?INDEX_BITSIZE, Binary/binary>>} ->
            NewReadPosition = ReadPosition + ?INDEX_SIZE + Size,
            file:pwrite(State#state.read_fd, 0, <<NewReadPosition:?INDEX_BITSIZE>>),
            %% @todo: keep extra bytes for next reads, it will help reduce the number of OS calls
            {reply, binary:part(Binary, {0, Size}), State#state{read_position = NewReadPosition}}
    end.
