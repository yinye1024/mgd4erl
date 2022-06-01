%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_pb_worker_data_dao).
-author("yinye").

-include("yymg_comm.hrl").

-define(DATA_TYPE,?MODULE).
-define(DATA_ID,1).

%% API functions defined
-export([init/1, get_data/0,put_data/1]).

%% ===================================================================================
%% API functions implements
%% ===================================================================================
init(WorkerAgent)->
  yymg_proc_cache_dao:init(?DATA_TYPE),
  DataPojo = yymg_pb_worker_data_pojo:new_pojo(?DATA_ID, WorkerAgent),
  yymg_proc_cache_dao:put_data(?DATA_TYPE,?DATA_ID,DataPojo),
  ?OK.

put_data(DataPojo)->
  yymg_proc_cache_dao:put_data(?DATA_TYPE,?DATA_ID,DataPojo),
  ?OK.

get_data()->
  DataPojo = yymg_proc_cache_dao:get_data(?DATA_TYPE,?DATA_ID),
  DataPojo.
