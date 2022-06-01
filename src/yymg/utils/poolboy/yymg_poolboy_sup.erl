%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%     网关进程，每个用户一个
%%% @end
%%% Created : 25. 四月 2021 19:45
%%%-------------------------------------------------------------------
-module(yymg_poolboy_sup).
-author("yinye").

-behavior(supervisor).
-include("yymg_comm.hrl").
-define(SERVER,?MODULE).


%% API functions defined
-export([ensure_sup_started/0, init/1]).
-export([start_pool/3,stop_pool/1]).
%% ===================================================================================
%% API functions implements
%% ===================================================================================
start_pool(PoolId,PoolSize,WorkerAgent)->
  PoolArgs = [{name,{local,PoolId}},{worker_module,yymg_pb_worker_gen},{size,PoolSize},{max_overflow,0}] ,
  supervisor:start_child(?MODULE,[PoolArgs,WorkerAgent]).

stop_pool(Pid) when is_pid(Pid)->
  poolboy:stop(Pid).


ensure_sup_started()->
  case priv_start_link() of
    {?OK,_}->?OK;
    {?ERROR,{already_started,_}}->?OK;
    {?ERROR,_}=Err->Err
  end.

priv_start_link()->
  supervisor:start_link({local,?SERVER},?MODULE,{}).

init({}) ->
  ChileSpec = #{
    id=> ?MODULE,
    start => {poolboy,start_link,[]},
    restart => temporary,  %% 挂了就挂了，不处理
    shutdown => 20000,
    type => worker,
    modules => [poolboy]
  },
  {?OK,{ {simple_one_for_one,10,10},[ChileSpec]} }.



