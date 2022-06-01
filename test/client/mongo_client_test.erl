%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. 六月 2021 19:07
%%%-------------------------------------------------------------------
-module(mongo_client_test).
-author("yinye").
-include("yymg_comm.hrl").
-include_lib("eunit/include/eunit.hrl").


%% ===================================================================================
%% API functions implements
%% ===================================================================================
client_test_() ->
  ?LOG_INFO({"client test ==================="}),

  {foreach,
  fun start/0,
  fun stop/1,
  [
    fun ensure_indexes/1,
    fun insert/1,
    fun update/1,
    fun bulk_update/1]
  }.
%%  [].


start() ->
  {Socket,McCfg,NextReqId} = new_conn(),
  ?LOG_INFO({"test start"}),
  {Socket,McCfg,NextReqId}.

stop({Socket,_McCfg,_NextReqId}) ->
  ?LOG_INFO({"test end"}),
  yymg_mongo_client_mgr:close(Socket),
  ?OK.


new_conn()->
  McCfg = yymg_mongo_client_cfg:new_auth_cfg("192.168.43.27", 27017,<<"test_db">>,30000,{"mongo-admin", "mgadmin@123456"}),
  {Socket,NextReqId} = yymg_mongo_client_mgr:new_conn(McCfg),
  {Socket,McCfg,NextReqId}.

ensure_indexes({Socket,McCfg,NextReqId})->
  Collection = <<"test_index">>,
  IndexSpec = {<<"key">>,{name,1},<<"unique">>,false,<<"dropDups">>,false,name,<<"index_name">>},
  yymg_mongo_client_mgr:ensure_indexes({Socket,McCfg},Collection,NextReqId,IndexSpec),
  [].

insert({Socket,McCfg,NextReqId})->
  Collection = <<"test_insert">>,
  Name = <<"yy1">>,
  {Id,Name,Age} = {1,Name,18},
  NewPojo = mongo_client_test_pojo:new_one({Id,Name,Age}),
  yymg_mongo_client_mgr:insert({Socket,McCfg},Collection,NextReqId,NewPojo),

  QueryMap = #{name =>Name},
  Projector = #{},
  {?OK, Item_1} = yymg_mongo_client_mgr:find_one({Socket,McCfg},Collection,NextReqId+1,QueryMap,Projector),
  yymg_mongo_client_mgr:delete({Socket,McCfg},Collection,NextReqId+2,QueryMap),
  {Item_2} = yymg_mongo_client_mgr:find_one({Socket,McCfg},Collection,NextReqId+3,QueryMap,Projector),
  [
    ?_assertNotMatch(?NOT_SET,Item_1),
    ?_assertMatch(?NOT_FOUND,Item_2)
  ].
update({Socket,McCfg,NextReqId})->
  Collection = <<"test_update">>,
  Name = <<"yy1">>,
  {Id,Name,Age} = {1,Name,18},
  NewPojo_1 = mongo_client_test_pojo:new_one({Id,Name,Age}),
  yymg_mongo_client_mgr:insert({Socket,McCfg},Collection,NextReqId,NewPojo_1),

  QueryMap = #{'_id' =>Id},
  Projector = #{},
  {?OK, FromDb_1} = yymg_mongo_client_mgr:find_one({Socket,McCfg},Collection,NextReqId+1,QueryMap,Projector),
  NewAge = 19,
  UpdatePojo = mongo_client_test_pojo:set_age(NewAge,NewPojo_1),
  SelectMap = QueryMap,
  IsUpsert = ?TRUE,
  yymg_mongo_client_mgr:update_one({Socket,McCfg},Collection,NextReqId+2, {SelectMap,UpdatePojo},IsUpsert),
  {?OK, FromDb_2} = yymg_mongo_client_mgr:find_one({Socket,McCfg},Collection,NextReqId+3,QueryMap,Projector),
  ?LOG_INFO({"find update from db",FromDb_2}),

  priv_delete_all({Socket,McCfg},Collection,NextReqId+3),
  [
    ?_assertMatch(Age, mongo_client_test_pojo:get_age(FromDb_1)),
    ?_assertMatch(NewAge, mongo_client_test_pojo:get_age(FromDb_2))
  ].

bulk_update({Socket,McCfg,NextReqId})->
  Collection = <<"test_bulk_update">>,
  NamePrefix = "yymg_",
  Count = 10,
  PojoList = mongo_client_test_pojo:new_list(NamePrefix,Count),
  SelectUpdateMapList = priv_to_selectMap_updateMap_List(PojoList,[]),
  IsUpsert = ?TRUE,
  yymg_mongo_client_mgr:bulk_update({Socket,McCfg},Collection,NextReqId+1, SelectUpdateMapList,IsUpsert),

  ItemList_1 = priv_find_all({Socket,McCfg},Collection,NextReqId+2),

  priv_delete_all({Socket,McCfg},Collection,NextReqId+3),
  ItemList_2 = priv_find_all({Socket,McCfg},Collection,NextReqId+4),

  ?LOG_INFO({"find bulk_update list from db:",ItemList_1}),
  [
    ?_assertMatch(Count,erlang:length(ItemList_1)),
    ?_assertMatch(0,erlang:length(ItemList_2))
  ].

priv_delete_all({Socket,McCfg},Collection,NextReqId)->
  QueryMap = #{},
  yymg_mongo_client_mgr:delete({Socket,McCfg},Collection,NextReqId,QueryMap),
  ?OK.


priv_to_selectMap_updateMap_List([Item|Less],AccList)->
  {SelectMap,UpdateMap} = priv_to_selectMap_updateMap(Item),
  priv_to_selectMap_updateMap_List(Less,[{SelectMap,UpdateMap}|AccList]);
priv_to_selectMap_updateMap_List([],AccList)->
  lists:reverse(AccList).

priv_to_selectMap_updateMap(Item)->
  Id = yymg_map:get_value('_id',Item),
  SelectMap = #{'_id'=>Id},
  {SelectMap,Item}.

priv_find_all({Socket,McCfg},Collection,NextReqId)->
  {QueryMap,Projector} = {#{},#{}},
  {?OK, ItemList_1} = yymg_mongo_client_mgr:find_list({Socket,McCfg},Collection,NextReqId+2,QueryMap,Projector),
  ItemList_1.



