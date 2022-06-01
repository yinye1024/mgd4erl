%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. 六月 2021 19:07
%%%-------------------------------------------------------------------
-module(mongo_client_test_pojo).
-author("yinye").

%% API functions defined
-export([new_list/2, new_one/1]).
-export([get_name/1, get_age/1, set_age/2,get_ver/1,incr_ver/1]).


%% ===================================================================================
%% API functions implements
%% ===================================================================================
new_list(NamePrefix,Count)->
  AccList = [],
  AccId = 1,
  priv_newList(NamePrefix,Count,{AccId,AccList}).

priv_newList(NamePrefix,Count,{AccId,AccList}) when AccId =< Count ->
  NewPojo = new_one({AccId,NamePrefix++erlang:integer_to_list(AccId),AccId}),
  priv_newList(NamePrefix,Count,{AccId+1,[NewPojo|AccList]});
priv_newList(_NamePrefix,_Count,{_AccId,AccList})  ->
  AccList.


new_one({Id,Name,Age}) ->
  #{
    '_id'=>Id,
    name=>Name,
    age=>Age,
    ver=>0
  }.

get_name(ItemMap) ->
  yymg_map:get_value(name, ItemMap).


get_age(ItemMap) ->
  yymg_map:get_value(<<"age">>, ItemMap).

set_age(Value, ItemMap) ->
  yymg_map:put_value(age, Value, ItemMap).



get_ver(ItemMap) ->
  yymg_map:get_value(ver, ItemMap).

incr_ver(ItemMap) ->
  NewVer = get_ver(ItemMap)+1,
  priv_set_ver(NewVer,ItemMap).

priv_set_ver(Value, ItemMap) ->
  yymg_map:put_value(ver, Value, ItemMap).

