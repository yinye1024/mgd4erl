%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_pb_worker_agent).
-author("yinye").

-include("yymg_comm.hrl").


%% API functions defined
-export([new_pojo/2, init/1,get_tick_time_span/1,loop_tick/1,do_fun/2]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================
new_pojo(WorkMod,WorkData)->
  NewPojo = #{
    mod => WorkMod,
    data => WorkData
  },
  NewPojo.
priv_get_mod(ItemMap) ->
  yymg_map:get_value(mod, ItemMap).
priv_get_data(ItemMap) ->
  yymg_map:get_value(data, ItemMap).
priv_set_data(Value, ItemMap) ->
  yymg_map:put_value(data, Value, ItemMap).


init(ItemMap)->
  NewWorkData = priv_init(ItemMap),
  NewItemMap = priv_set_data(NewWorkData,ItemMap),
  NewItemMap.

get_tick_time_span(ItemMap)->
  TimeSpanMs = priv_get_tick_time_span(ItemMap),
  TimeSpanMs.

loop_tick(ItemMap)->
  NewWorkData = priv_loop_tick(ItemMap),
  NewItemMap = priv_set_data(NewWorkData,ItemMap),
  NewItemMap.


%% =======================  WorkMod 相关方法 ============================================================
%% return result
do_fun({Fun,Param},ItemMap)->
  WorkMod = priv_get_mod(ItemMap),
  WorkData = priv_get_data(ItemMap),
  Result = WorkMod:do_fun({Fun,Param},WorkData),
  Result.

%% return NewWorkData
priv_init(ItemMap)->
  WorkMod = priv_get_mod(ItemMap),
  WorkData = priv_get_data(ItemMap),
  WorkMod:init(WorkData).


%% return NewWorkData
priv_loop_tick(ItemMap)->
  WorkMod = priv_get_mod(ItemMap),
  WorkData = priv_get_data(ItemMap),
  WorkMod:loop_tick(WorkData).

priv_get_tick_time_span(ItemMap)->
  WorkMod = priv_get_mod(ItemMap),
  WorkMod:get_tick_time_span().









