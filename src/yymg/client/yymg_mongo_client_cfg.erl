%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_mongo_client_cfg).
-author("yinye").

-include_lib("yyutils/include/yyu_comm.hrl").


%% API functions defined
-export([new_cfg/4, new_auth_cfg/5]).
-export([get_host/1, get_port/1, get_db/1, get_time_out/1]).
-export([get_login_name/1, get_login_pwd/1]).
%% ===================================================================================
%% API functions implements
%% ===================================================================================

new_cfg(Host,Port, DbName,TimeOut)->
  #{
    host => Host,
    port => Port,
    db => DbName,
    time_out=> TimeOut
  }.
new_auth_cfg(Host,Port, DbName,TimeOut,{LoginName,LoginPwd})->
  McCfg = new_cfg(Host,Port, DbName,TimeOut),
  McCfg#{
    login_name => LoginName,
    login_pwd => LoginPwd
  }.



get_host(ItemMap) ->
  yyu_map:get_value(host, ItemMap).

get_port(ItemMap) ->
  yyu_map:get_value(port, ItemMap).

get_db(ItemMap) ->
  yyu_map:get_value(db, ItemMap).

get_time_out(ItemMap) ->
  yyu_map:get_value(time_out, ItemMap).


get_login_name(ItemMap) ->
  yyu_map:get_value(login_name, ItemMap).

get_login_pwd(ItemMap) ->
  yyu_map:get_value(login_pwd, ItemMap).



