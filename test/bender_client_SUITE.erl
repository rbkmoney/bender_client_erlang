-module(bender_client_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).

-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-export([
    gen_internal_id/1,
    get_internal_id/1
]).

-type test_case_name()  :: atom().

-spec all() ->
    [test_case_name()].
all() ->
    [
        {group, all_tests}
    ].

-spec groups() -> [{atom(), list(), [test_case_name()]}].
groups() ->
    [
        {all_tests, [sequence], [
            gen_internal_id,
            get_internal_id
        ]}
    ].


-type config() :: [{atom(), any()}].

-spec init_per_suite(config()) ->
    config().
init_per_suite(Config) ->
    Apps =
        genlib_app:start_application_with(bender_client, [
            {service_url, <<"http://bender:8022/v1/bender">>},
            {deadline, 10000},
            {retries, #{
                'GenerateID' => finish,
                'GetInternalID' => finish,
                '_' => finish
            }}
        ]),
    [{apps, Apps}] ++ Config.

-spec end_per_suite(config()) ->
    _.
end_per_suite(Config) ->
    [application:stop(App) || App <- proplists:get_value(apps, Config)],
    Config.

%%

-define(EXTERNAL_ID, <<"external_id">>).

-spec gen_internal_id(config()) ->
    _.
gen_internal_id(_) ->
    WoodyContext = woody_context:new(),
    IdempotentKey = get_idempotent_key(?EXTERNAL_ID),
    {ok, <<"1">>} = bender_client:gen_by_sequence(IdempotentKey, <<"SEQ">>, <<"HASH">>, WoodyContext).

-spec get_internal_id(config()) ->
    _.
get_internal_id(_) ->
    WoodyContext = woody_context:new(),
    IdempotentKey = get_idempotent_key(?EXTERNAL_ID),
    {ok, <<"1">>, _} = bender_client:get_internal_id(IdempotentKey, WoodyContext).

get_idempotent_key(ExternalID) ->
    bender_client:get_idempotent_key(<<"domain">>, <<"prefix">>, <<"party">>, ExternalID).