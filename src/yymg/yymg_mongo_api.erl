%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_api).
-author("yinye").

-include_lib("yyutils/include/yyu_comm.hrl").


%% API functions defined
-export([ensure_sup_started/0, new_pool/3,stop_pool/1,ensure_indexes/2]).
-export([insert/2,batch_insert/2,
          update/2,bulk_update/2,update_selection/2,
          delete/2,batch_delete/2]).
-export([find_one/2,find_list/2,incr_and_get_autoId/2,find_and_modify/2]).
-export([get_count/2,find_batch/2,next_batch/1, close_batch/1]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================

%% =======================  WorkMod 相关方法 ============================================================
%% yymg_mongo_mgr:ensure_sup_started().
ensure_sup_started()->
  yymg_mongo_poolboy_mgr:ensure_sup_started(),
  gs_yymg_mongo_cursor_mgr:ensure_sup_started(),
  ?OK.

new_pool(PoolId,PoolSize,McCfg)->
  PoolPid  = yymg_mongo_poolboy_mgr:new_pool(PoolId,PoolSize,McCfg),
  PoolPid.

stop_pool(PoolPid)->
  yymg_mongo_poolboy_mgr:stop_pool(PoolPid),
  ?OK.

ensure_indexes(PoolId,{Collection, IndexSpec})->
  yymg_mongo_poolboy_mgr:ensure_indexes(PoolId,{Collection, IndexSpec}),
  ?OK.

insert(PoolId,{Collection, InsertMap})->
  Item_1 = priv_convert_item_to_db(InsertMap),
  yymg_mongo_poolboy_mgr:insert(PoolId,{Collection, Item_1}),
  ?OK.

batch_insert(PoolId,{Collection, InsertMapList})->
  yymg_mongo_poolboy_mgr:batch_insert(PoolId,{Collection, priv_convert_list_to_db(InsertMapList)}),
  ?OK.

update(PoolId,{Collection, Item, IsUpsert})->
  Item_1 = priv_convert_item_to_db(Item),
  {SelectMap,UpdateMap} = priv_to_selectMap_updateMap(Item_1),
  yymg_mongo_poolboy_mgr:update(PoolId,{Collection, {SelectMap,UpdateMap}, IsUpsert}),
  ?OK.

priv_to_selectMap_updateMap_List([Item|Less],AccList)->
  {SelectMap,UpdateMap} = priv_to_selectMap_updateMap(Item),
  priv_to_selectMap_updateMap_List(Less,[{SelectMap,UpdateMap}|AccList]);
priv_to_selectMap_updateMap_List([],AccList)->
  lists:reverse(AccList).

priv_to_selectMap_updateMap(Item)->
  Id = yyu_map:get_value('_id',Item),
  SelectMap = #{'_id'=>Id},
  {SelectMap,Item}.

update_selection(PoolId,{Collection, SelectMap,UpdateMap})->
  UpdateMap_1 = priv_convert_item_to_db(UpdateMap),
  yymg_mongo_poolboy_mgr:update_selection(PoolId,{Collection, SelectMap,UpdateMap_1}),
  ?OK.

%% 无大小限制
bulk_update(PoolId,{Collection,ItemList, IsUpsert}) when is_list(ItemList)->
  ItemList_1 = priv_convert_list_to_db(ItemList),
  SelectedUpdateMapList = priv_to_selectMap_updateMap_List(ItemList_1,[]),
  yymg_mongo_poolboy_mgr:bulk_update(PoolId,{Collection,SelectedUpdateMapList, IsUpsert}),
  ?OK.

delete(PoolId,{Collection, SelectorMap}) when is_map(SelectorMap)->
  yymg_mongo_poolboy_mgr:delete(PoolId,{Collection, SelectorMap}),
  ?OK.

batch_delete(PoolId,{Collection, QueryMap_LimitList}) when is_list(QueryMap_LimitList)->
  yymg_mongo_poolboy_mgr:batch_delete(PoolId,{Collection, QueryMap_LimitList}),
  ?OK.

incr_and_get_autoId(PoolId,AutoName) when is_atom(AutoName)->
  Collection = <<"autoid">>,
  QueryMap = #{'_id'=>AutoName},
  UpdateMap = {'$inc',{'seq',1}},
  {?OK,NewItem} = find_and_modify(PoolId,{Collection, QueryMap,UpdateMap}),
  AutoId = yyu_map:get_value(<<"seq">>,NewItem),
  AutoId.

find_and_modify(PoolId,{Collection, QueryMap,UpdateMap})->
  {?OK,Result} = yymg_mongo_poolboy_mgr:find_and_modify(PoolId,{Collection, QueryMap,UpdateMap}),
  {?OK,Result}.

%% return all fields of found record if Projector = #{}
find_one(PoolId,{Collection, QueryMap, Projector})->
  Result =
  case yymg_mongo_poolboy_mgr:find_one(PoolId,{Collection, QueryMap, Projector}) of
    {?OK,Item} ->{?OK, priv_convert_item_from_db(Item)};
    {?NOT_FOUND} ->{?NOT_FOUND}
  end,
  Result.

%% return all fields of found record if Projector = #{}

find_list(PoolId,{Collection, QueryMap, Projector})->
  {?OK, ItemList} = yymg_mongo_poolboy_mgr:find_list(PoolId,{Collection, QueryMap, Projector}),
  {?OK, priv_conver_list_from_db(ItemList)}.


%% ==================== 批量查找 要管理好 cursorId (开始)==========================================
get_count(PoolId,{Collection, QueryMap})->
  Count = yymg_mongo_poolboy_mgr:get_count(PoolId,{Collection, QueryMap}),
  Count.

%% return all fields of found record if Projector = #{}
find_batch(PoolId,{Collection,{QueryMap,Projector,Skip,BatchSize}})->
  {CursorPid,FirstBatch} = gs_yymg_mongo_cursor_mgr:find_batch(PoolId,Collection,{QueryMap,Projector,Skip,BatchSize}),
  {CursorPid, priv_conver_list_from_db(FirstBatch)}.

next_batch(CursorPid)->
  {Result, NextBatch} = gs_yymg_mongo_cursor_mgr:next_batch(CursorPid),
  {Result, priv_conver_list_from_db(NextBatch)}.

close_batch(CursorPid)->
  gs_yymg_mongo_cursor_mgr:close(CursorPid),
  ?OK.
%% ==================== 批量查找 要管理好 cursorId (结束)==========================================


priv_convert_list_to_db(DbItemList)->
  priv_convert_list_to_db(DbItemList,[]).
priv_convert_list_to_db([DbItem|Less],AccList) ->
  NewAccList = [priv_convert_item_to_db(DbItem)|AccList],
  priv_convert_list_to_db(Less,NewAccList);
priv_convert_list_to_db([],AccList) ->
  AccList.

%% key 只能是 数字或者是 atom
priv_convert_item_to_db(Map) when is_map(Map)->
  maps:fold(fun(Key,Value,AccMap)->
              NewKey = priv_key_fixed(Key),
              yyu_map:put_value(NewKey, priv_convert_item_to_db(Value),AccMap)
            end, yyu_map:new_map(), Map);
priv_convert_item_to_db(List) when is_list(List)->
  lists:map(fun(Elem)->
    priv_convert_item_to_db(Elem)
            end,List);
priv_convert_item_to_db(Data)->Data.

priv_key_fixed(Key) when is_atom(Key)->
  Key;
priv_key_fixed(Key) when is_integer(Key)->
  integer_to_binary(Key);
priv_key_fixed(Key) ->
  yyu_error:throw_error({"map key should be atom or integer",Key}).




priv_conver_list_from_db(DbItemList)->
  priv_conver_list_from_db(DbItemList,[]).
priv_conver_list_from_db([DbItem|Less],AccList) ->
  NewAccList = [priv_convert_item_from_db(DbItem)|AccList],
  priv_conver_list_from_db(Less,NewAccList);
priv_conver_list_from_db([],AccList) ->
  AccList.

%% key 只能是 数字或者是 atom
priv_convert_item_from_db(Map) when is_map(Map)->
  maps:fold(fun(Key,Value,AccMap)->
    NewKey = case catch binary_to_integer(Key) of
               {'EXIT',{badarg,_R}} -> binary_to_atom(Key,utf8);
               IntKey -> IntKey
             end,
    yyu_map:put_value(NewKey, priv_convert_item_from_db(Value),AccMap)
            end, yyu_map:new_map(), Map);
priv_convert_item_from_db(List) when is_list(List)->
  lists:map(fun(Elem)->
    priv_convert_item_from_db(Elem)
            end,List);
priv_convert_item_from_db(Data)->Data.




