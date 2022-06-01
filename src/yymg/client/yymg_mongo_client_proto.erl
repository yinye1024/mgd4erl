%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_client_proto).
-author("yinye").

-include("yymg_comm.hrl").


%% API functions defined
-export([encode_ping/2,encode_buildInfo/2,encode_count/3,
      encode_createIndexes/3,encode_insert/3,encode_update/3,encode_delete/3,
      encode_findAndModify/4,encode_find_one/4, encode_find_list/4]).
-export([encode_find_batch/6,encode_getMore/4,encode_killCursors/3]).
-export([encode_bulk_insert/3,encode_bulk_update/3]).
-export([decode_resp/1,priv_encode_req/2]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================
encode_ping(Db,ReqId)->
  Doc = {ping,1,'$db',Db},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_buildInfo(Db,ReqId)->
  Doc = {buildInfo,1,'$db',Db},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_count({Collection,Db},ReqId,QueryMap)->
  Doc = {count,Collection,'$db',Db, query, QueryMap},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_createIndexes({Collection,Db},ReqId,IndexSpec)->
  Doc = {createIndexes,Collection,'$db',Db, indexes, [IndexSpec]},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_insert({Collection,Db},ReqId,InsertMapList)->
  Doc = {insert,Collection,'$db',Db, documents, InsertMapList},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_update({Collection,Db},ReqId, UpdateMapList)->
  Doc = {update,Collection,'$db',Db, updates, UpdateMapList},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_delete({Collection,Db},ReqId, DeleteMapList)->
  Doc = {delete,Collection,'$db',Db, deletes, DeleteMapList},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_findAndModify({Collection,Db},ReqId, QueryMap,UpdateMap)->
  Doc = {findAndModify,Collection,'$db',Db, query, QueryMap,update,UpdateMap,upsert,true,new,true},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_find_one({Collection,Db},ReqId, QueryMap,Projector)->
  Doc = {find,Collection,'$db',Db, filter, QueryMap,projection,Projector,singleBatch,true,limit,1},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_find_list({Collection,Db},ReqId, QueryMap,Projector)->
  Doc = {find,Collection,'$db',Db, filter, QueryMap,projection,Projector,singleBatch,true},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_find_batch({Collection,Db},ReqId, QueryMap,Projector,Skip,BatchSize)->
  Doc = {find,Collection,'$db',Db, filter, QueryMap,projection,Projector,skip,Skip,batchSize,BatchSize},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_getMore({Collection,Db},ReqId, CursorId,BatchSize)->
  Doc = {getMore,CursorId,collection,Collection,'$db',Db, batchSize,BatchSize},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.

encode_killCursors({Collection,Db},ReqId, CursorId)->
  Doc = {<<"killCursors">>,Collection,'$db',Db, cursors,[CursorId]},
  Pack = priv_encode_req(ReqId,Doc),
  Pack.


priv_encode_req(ReqId,Section)->
  OP_MSG_CODE = 2013, %% OpCode for OpMsg
  OpMsg = <<
%%  messageLength                 %% messageLength set later
    ReqId:32/signed-little,       %% request Id
    0:32/signed-little,           %% response To
    OP_MSG_CODE:32/signed-little, %% opCode
    0:32/unsigned-little,         %% flagBits set to 0
    0:8/unsigned-little,          %% Payload type [0 for 1 single-document|1 for multi-document]
    (bson_binary:put_document(Section))/binary %% section
    >>,
  MsgLength = (byte_size(OpMsg)+4),
  <<MsgLength:32/signed-little,OpMsg/binary>>.

decode_resp(DataBin)->
  <<
    _ReqId:32/signed-little,      %% request Id
    ResponseTo:32/signed-little,  %% response To 响应的是哪个请求id
    _OpCode:32/signed-little, %% opCode
    _FlagBits:32/unsigned-little,         %% flagBits set to 0
    _PayloadType:8/unsigned-little,          %% Payload type [0 for 1 single-document|1 for multi-document]
    RespDocBin/binary %% section
  >> = DataBin,

  {RespMap,_Bin} = bson_binary:get_map(RespDocBin),

  {ResponseTo,RespMap}.

encode_bulk_insert({Collection,Db},ReqId, InsertMapList)->
  Doc_0 = {insert,Collection,'$db',Db},
  OP_MSG_CODE = 2013, %% OpCode for OpMsg
  Payload_0 = <<
%%  messageLength                 %% messageLength set later
    ReqId:32/signed-little,       %% request Id
    0:32/signed-little,           %% response To
    OP_MSG_CODE:32/signed-little, %% opCode
    0:32/unsigned-little,         %% flagBits set to 0
    0:8/unsigned-little,          %% Payload type [0 for 1 single-document|1 for multi-document]
    (bson_binary:put_document(Doc_0))/binary %% section
  >>,
  BinTmp = <<(bson_binary:put_cstring(erlang:atom_to_binary(documents,utf8)))/binary>>,
  BinTmp_2 = priv_add_doc(InsertMapList,BinTmp),
  Payload_1_Length = (byte_size(BinTmp_2)) + 4,
  Payload_1 = <<
              1:8/unsigned-little, %% Payload type [0 for 1 single-document|1 for multi-document]
              Payload_1_Length:32/signed-little,
              BinTmp_2/binary
              >>,
  MsgLength = byte_size(Payload_0) + byte_size(Payload_1) + 4,
  <<MsgLength:32/signed-little,Payload_0/binary,Payload_1/binary>>.

encode_bulk_update({Collection,Db},ReqId, UpdateMapList)->
  Doc_0 = {update,Collection,'$db',Db},
  OP_MSG_CODE = 2013, %% OpCode for OpMsg
  Payload_0 = <<
%%  messageLength                 %% messageLength set later
    ReqId:32/signed-little,       %% request Id
    0:32/signed-little,           %% response To
    OP_MSG_CODE:32/signed-little, %% opCode
    0:32/unsigned-little,         %% flagBits set to 0
    0:8/unsigned-little,          %% Payload type [0 for 1 single-document|1 for multi-document]
    (bson_binary:put_document(Doc_0))/binary %% section
  >>,
  BinTmp = <<(bson_binary:put_cstring(erlang:atom_to_binary(updates,utf8)))/binary>>,
  BinTmp_2 = priv_add_doc(UpdateMapList,BinTmp),
  Payload_1_Length = (byte_size(BinTmp_2)) + 4,
  Payload_1 = <<
              1:8/unsigned-little, %% Payload type [0 for 1 single-document|1 for multi-document]
              Payload_1_Length:32/signed-little,
              BinTmp_2/binary
              >>,
  MsgLength = byte_size(Payload_0) + byte_size(Payload_1) + 4,
  <<MsgLength:32/signed-little,Payload_0/binary,Payload_1/binary>>.

priv_add_doc([Doc|Less],AccBin)->
  NewAccBin = <<AccBin/binary,(bson_binary:put_document(Doc))/binary>>,
  priv_add_doc(Less,NewAccBin);
priv_add_doc([],AccBin)->
  AccBin.
