%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(yymg_mongo_client_auth_helper).
-author("yinye").
-include_lib("bson/include/bson_binary.hrl").
-include_lib("yyutils/include/yyu_comm.hrl").


-define(NOT_MASTER_ERROR, 13435).
-define(UNAUTHORIZED_ERROR(C), C =:= 10057; C =:= 16550).
-define(QueryOpcode, 2004).
-define(ReplyOpcode, 1).
-define(get_header(Opcode, ResponseTo), ?get_int32(_RequestId), ?get_int32(ResponseTo), ?get_int32(Opcode)).
-record(reply, {
  cursornotfound :: boolean(),
  queryerror :: boolean(),
  awaitcapable = false :: boolean(),
  cursorid :: mc_worker_api:cursorid(),
  startingfrom = 0 :: integer(),
  documents :: [map()]
}).


%% API
-export([sync_command/4]).

sync_command(Socket, Database,Cmd,RequestId) ->
  Doc = priv_read_one_sync(Socket, Database,Cmd,RequestId),
  priv_process_reply(Doc, Cmd).

priv_read_one_sync(Socket, Database, Cmd,RequestId) ->
  {?OK, Docs} = priv_request_raw(Socket, Database, Cmd,RequestId),
  case Docs of
    [] -> #{};
    [Doc | _] -> Doc
  end.

priv_request_raw(Socket, Database,Cmd,RequestId) ->
  Timeout = 30000,
  {ok, _, _} = priv_make_request(Socket, Database, Cmd,RequestId),
  Responses = priv_recv_all(Socket, Timeout),
  {_, Reply} = hd(Responses),
  priv_reply(Reply).

priv_make_request(Socket, Database, Cmd,RequestId) ->
  {Packet, Id} = priv_encode_request(Database, Cmd,RequestId),
  {gen_tcp:send(Socket, Packet), iolist_size(Packet), Id}.
priv_encode_request(Database, Cmd,RequestId) ->
  Payload = priv_put_message(Database, Cmd,RequestId),
  {<<(byte_size(Payload) + 4):32/little, Payload/binary>>, RequestId}.

priv_put_message(Db,Cmd,RequestId)->

  Coll = <<"$cmd">>,
  <<?put_int32(RequestId), ?put_int32(0), ?put_int32(?QueryOpcode),
    ?put_bits32(0, 0, 0, 0, 0, 0, 0, 0),
    (bson_binary:put_cstring(priv_dbcoll(Db, Coll)))/binary,
    ?put_int32(0),
    ?put_int32(-1),
    (bson_binary:put_document(Cmd))/binary>>.

priv_dbcoll(Db, Coll) when is_binary(Db) and is_binary(Coll) ->
  <<Db/binary, $., Coll/binary>>.

priv_reply(#reply{queryerror = false} = Reply) ->
  {?OK, Reply#reply.documents};
priv_reply({error, Error}) ->
  priv_process_error(error, Error).

priv_process_error(?NOT_MASTER_ERROR, _) ->
  erlang:error(not_master);
priv_process_error(Code, _) when ?UNAUTHORIZED_ERROR(Code) ->
  erlang:error(unauthorized);
priv_process_error(_, Doc) ->
  erlang:error({bad_query, Doc}).

priv_process_reply(Doc = #{<<"ok">> := N}, _) when is_number(N) ->   %command succeed | failed
  {N == 1, maps:remove(<<"ok">>, Doc)};
priv_process_reply(Doc, Command) -> %unknown result
  erlang:error({bad_command, Doc}, [Command]).

%% @private
priv_recv_all(Socket, Timeout) ->
  priv_recv_all(Socket, Timeout, <<>>).
priv_recv_all(Socket, Timeout, Rest) ->
  {ok, Packet} = gen_tcp:recv(Socket, 0, Timeout),
  case priv_decode_responses(<<Rest/binary, Packet/binary>>,[]) of
    {[], Unfinished} -> priv_recv_all(Socket, Timeout,  Unfinished);
    {Responses, _} -> Responses
  end.

priv_decode_responses(<<Length:32/signed-little, Data/binary>>, Acc) when byte_size(Data) >= (Length - 4) ->
  PayloadLength = Length - 4,
  <<Payload:PayloadLength/binary, Rest/binary>> = Data,
  {Id, Response, <<>>} = get_reply(Payload),
  priv_decode_responses(Rest, [{Id, Response} | Acc]);
priv_decode_responses(Data, Acc) ->
  {lists:reverse(Acc), Data}.


get_reply(Message) ->
  <<?get_header(?ReplyOpcode, ResponseTo),
    ?get_bits32(_, _, _, _, AwaitCapable, _, QueryError, CursorNotFound),
    ?get_int64(CursorId),
    ?get_int32(StartingFrom),
    ?get_int32(NumDocs),
    Bin/binary>> = Message,
  {Docs, BinRest} = get_docs(NumDocs, Bin, []),
  Reply = #reply{
    cursornotfound = bool(CursorNotFound),
    queryerror = bool(QueryError),
    awaitcapable = bool(AwaitCapable),
    cursorid = CursorId,
    startingfrom = StartingFrom,
    documents = Docs
  },
  {ResponseTo, Reply, BinRest}.
%% @private
get_docs(0, Bin, Docs) -> {lists:reverse(Docs), Bin};
get_docs(NumDocs, Bin, Docs) when NumDocs > 0 ->
  {Doc, Bin1} = bson_binary:get_map(Bin),
  get_docs(NumDocs - 1, Bin1, [Doc | Docs]).
%% @private
bool(0) -> false;
bool(1) -> true.
