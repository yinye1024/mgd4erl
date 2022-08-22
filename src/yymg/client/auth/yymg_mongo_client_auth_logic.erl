%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(yymg_mongo_client_auth_logic).
-author("yinye").
-include_lib("yyutils/include/yyu_comm.hrl").

-define(RANDOM_LENGTH, 24).
-define(GS2_HEADER, <<"n,,">>).


%% API
-export([auth/4]).

%% Version > 2.7
auth(Socket, AdminDb, Login, Password) ->
  scram_sha_1_auth(Socket, AdminDb, Login, Password),
  NextReqId = 5,  %% 1~4 used for auth req
  NextReqId.


scram_sha_1_auth(Socket, AdminDb, Login, Password) ->
  scram_first_step(Socket, AdminDb, Login, Password).
%%  try
%%    scram_first_step(Socket, AdminDb, Login, Password)
%%  catch
%%    Error:Reason ->
%%      ?ERROR({"error when do mongo auth.",Error,Reason,[AdminDb, Login, Password]}),
%%      erlang:error(<<"Can't pass authentification">>)
%%  end.

%% @private
scram_first_step(Socket, Database, Login, Password) ->
  RandomBString = priv_random_nonce(?RANDOM_LENGTH),
  FirstMessage = priv_compose_first_message(Login, RandomBString),
  Message = <<?GS2_HEADER/binary, FirstMessage/binary>>,
  Cmd =  {<<"saslStart">>, 1, <<"mechanism">>, <<"SCRAM-SHA-1">>, <<"payload">>, {bin, bin, Message}, <<"autoAuthorize">>, 1},
  RequestId = 1,
  {true, Res} = yymg_mongo_client_auth_helper:sync_command(Socket, Database,Cmd,RequestId),
  ConversationId = maps:get(<<"conversationId">>, Res, {}),
  Payload = maps:get(<<"payload">>, Res),
  scram_second_step(Socket, Database, Login, Password, Payload, ConversationId, RandomBString, FirstMessage).
priv_random_nonce(TextLength) ->
  ByteLength = trunc(TextLength / 4 * 3),
  RandBytes = crypto:strong_rand_bytes(ByteLength),
  base64:encode(RandBytes).
priv_compose_first_message(Login, RandomBString) ->
  UserName = <<<<"n=">>/binary, (priv_encode_name(Login))/binary>>,
  Nonce = <<<<"r=">>/binary, RandomBString/binary>>,
  <<UserName/binary, <<",">>/binary, Nonce/binary>>.
priv_encode_name(Name) ->
  Comma = re:replace(Name, <<"=">>, <<"=3D">>, [{return, binary}]),
  re:replace(Comma, <<",">>, <<"=2C">>, [{return, binary}]).



%% @private
scram_second_step(Socket, Database, Login, Password, {bin, bin, Decoded} = _Payload, ConversationId, RandomBString, FirstMessage) ->
  {Signature, ClientFinalMessage} = priv_compose_second_message(Decoded, Login, Password, RandomBString, FirstMessage),
  Cmd = {<<"saslContinue">>, 1, <<"conversationId">>, ConversationId,
    <<"payload">>, {bin, bin, ClientFinalMessage}},
  RequestId=2,
  {true, Res} = yymg_mongo_client_auth_helper:sync_command(Socket, Database, Cmd,RequestId),
  scram_third_step(base64:encode(Signature), Res, ConversationId, Socket, Database).
priv_compose_second_message(Payload, Login, Password, RandomBString, FirstMessage) ->
  ParamList = priv_parse_response(Payload),
  R = priv_get_value(<<"r">>, ParamList),
  Nonce = <<<<"r=">>/binary, R/binary>>,
  {0, ?RANDOM_LENGTH} = binary:match(R, [RandomBString], []),
  S = priv_get_value(<<"s">>, ParamList),
  I = binary_to_integer(priv_get_value(<<"i">>, ParamList)),
  SaltedPassword = priv_salt_pwd(priv_pw_hash(Login, Password), base64:decode(S), I),
  ChannelBinding = <<<<"c=">>/binary, (base64:encode(?GS2_HEADER))/binary>>,
  ClientFinalMessageWithoutProof = <<ChannelBinding/binary, <<",">>/binary, Nonce/binary>>,
  AuthMessage = <<FirstMessage/binary, <<",">>/binary, Payload/binary, <<",">>/binary, ClientFinalMessageWithoutProof/binary>>,
  ServerSignature = priv_generate_sig(SaltedPassword, AuthMessage),
  Proof = priv_generate_proof(SaltedPassword, AuthMessage),
  {ServerSignature, <<ClientFinalMessageWithoutProof/binary, <<",">>/binary, Proof/binary>>}.
priv_pw_hash(Username, Password) ->
  Bin = crypto:hash(md5, [Username, <<":mongo:">>, Password]),
  HexStr = lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]),
  bson:utf8(HexStr).
priv_salt_pwd(Password, Salt, Iterations) ->
  {ok, Key} = pbkdf2:pbkdf2(sha, Password, Salt, Iterations, 20),
  Key.
priv_generate_proof(SaltedPassword, AuthMessage) ->
  ClientKey = priv_hmac(SaltedPassword, <<"Client Key">>),
  StoredKey = crypto:hash(sha, ClientKey),
  Signature = priv_hmac(StoredKey, AuthMessage),
  ClientProof = priv_xorKeys(ClientKey, Signature, <<>>),
  <<<<"p=">>/binary, (base64:encode(ClientProof))/binary>>.
priv_generate_sig(SaltedPassword, AuthMessage) ->
  ServerKey = priv_hmac(SaltedPassword, "Server Key"),
  priv_hmac(ServerKey, AuthMessage).
priv_hmac(One, Two) ->
  crypto:hmac(sha, One, Two).
priv_xorKeys(<<>>, _, Res) -> Res;
priv_xorKeys(<<FA, RestA/binary>>, <<FB, RestB/binary>>, Res) ->
  priv_xorKeys(RestA, RestB, <<Res/binary, <<(FA bxor FB)>>/binary>>).


%% @private
scram_third_step(ServerSignature, Response, ConversationId, Socket, Database) ->
  {bin, bin, Payload} = maps:get(<<"payload">>, Response),
  Done = maps:get(<<"done">>, Response, false),
  ParamList = priv_parse_response(Payload),
  ServerSignature = priv_get_value(<<"v">>, ParamList),
  scram_forth_step(Done, ConversationId, Socket, Database).



%% @private
scram_forth_step(true, _, _, _) -> true;
scram_forth_step(false, ConversationId, Socket, Database) ->
  Cmd = {<<"saslContinue">>, 1, <<"conversationId">>,
    ConversationId, <<"payload">>, {bin, bin, <<>>}},
  RequestId = 3,
  {true, Res} = yymg_mongo_client_auth_helper:sync_command(Socket, Database, Cmd,RequestId),
  true = maps:get(<<"done">>, Res, false).


priv_parse_response(Responce) ->
  ParamList = binary:split(Responce, <<",">>, [global]),
  lists:map(
    fun(Param) ->
      [K, V] = binary:split(Param, <<"=">>),
      {K, V}
    end, ParamList).

priv_get_value(Key, List) ->
  priv_get_value(Key, List, undefined).
priv_get_value(Key, List, Default) ->
  case lists:keyfind(Key, 1, List) of
    {_, Value} -> Value;
    false -> Default
  end.
