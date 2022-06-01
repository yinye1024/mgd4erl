%%%-------------------------------------------------------------------
%%% @author yinye
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. 四月 2021 11:50
%%%-------------------------------------------------------------------
-author("yinye").
-include("yymg_comm_atoms.hrl").

-ifndef(YYMG_LOGGER).
-define(YYMG_LOGGER,yymg_logger_hrl).

-define(LOG_ERROR(MsgTuple),yymg_logger:error(MsgTuple,{?MODULE,?FUNCTION_NAME,?LINE})).
-define(LOG_INFO(MsgTuple),yymg_logger:info(MsgTuple,{?MODULE,?FUNCTION_NAME,?LINE})).
-define(LOG_WARNING(MsgTuple),yymg_logger:warning(MsgTuple,{?MODULE,?FUNCTION_NAME,?LINE})).


-ifndef(release).
-define(TMP(MsgTuple),yymg_logger:info({tmp,MsgTuple},{?MODULE,?FUNCTION_NAME,?LINE})).
-else.
-define(TMP(MsgTuple),?OK).
-endif.

-endif.
