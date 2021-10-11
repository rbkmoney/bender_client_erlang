-module(bender_client_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0]).

-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-export([
    sequence/1,
    get_internal_id/1,
    sequence_w_context/1,
    get_internal_id_w_context/1,
    generator_snowflake/1,
    generator_sequence/1
]).

-type test_case_name() :: atom().

-spec all() -> [{group, test_case_name()}].
all() ->
    [
        {group, all_tests},
        {group, no_external_id}
    ].

-spec groups() -> [{atom(), list(), [test_case_name()]}].
groups() ->
    [
        {all_tests, [sequence], [
            sequence,
            get_internal_id,
            sequence_w_context,
            get_internal_id_w_context
        ]},
        {no_external_id, [], [
            generator_snowflake,
            generator_sequence
        ]}
    ].

-type config() :: [{atom(), any()}].

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
    Apps =
        genlib_app:start_application_with(bender_client, [
            {services, #{
                'Bender' => <<"http://bender:8022/v1/bender">>,
                'Generator' => <<"http://bender:8022/v1/generator">>
            }},
            {deadline, 10000},
            {retries, #{
                'GenerateID' => finish,
                'GetInternalID' => finish,
                '_' => finish
            }}
        ]),
    [{apps, Apps}] ++ Config.

-spec end_per_suite(config()) -> _.
end_per_suite(Config) ->
    _ = [application:stop(App) || App <- proplists:get_value(apps, Config)],
    Config.

%%

-define(EXTERNAL_ID, <<"external_id">>).
-define(EXTERNAL_ID_2, <<"external_id_2">>).

-spec sequence(config()) -> _.
sequence(_) ->
    WoodyContext = woody_context:new(),
    IdempotentKey = get_idempotent_key(?EXTERNAL_ID),
    {ok, <<"1">>} = bender_client:gen_sequence(IdempotentKey, <<"SEQ">>, WoodyContext).

-spec get_internal_id(config()) -> _.
get_internal_id(_) ->
    WoodyContext = woody_context:new(),
    IdempotentKey = get_idempotent_key(?EXTERNAL_ID),
    {ok, <<"1">>} = bender_client:get_internal_id(IdempotentKey, WoodyContext).

-spec sequence_w_context(config()) -> _.
sequence_w_context(_) ->
    WoodyContext = woody_context:new(),
    IdempotentKey = get_idempotent_key(?EXTERNAL_ID_2),
    Context = #{<<"key">> => <<"my_context">>},
    {ok, <<"2">>} = bender_client:gen_sequence(IdempotentKey, <<"SEQ">>, WoodyContext, Context).

-spec get_internal_id_w_context(config()) -> _.
get_internal_id_w_context(_) ->
    WoodyContext = woody_context:new(),
    IdempotentKey = get_idempotent_key(?EXTERNAL_ID_2),
    {ok, <<"2">>, #{<<"key">> := <<"my_context">>}} = bender_client:get_internal_id(IdempotentKey, WoodyContext).

-spec generator_snowflake(config()) -> _.
generator_snowflake(_) ->
    WoodyContext = woody_context:new(),
    _ID = bender_generator_client:gen_snowflake(WoodyContext).

-spec generator_sequence(config()) -> _.
generator_sequence(_) ->
    WoodyContext = woody_context:new(),
    SequenceID = genlib:unique(),
    <<"1">> = bender_generator_client:gen_sequence(SequenceID, WoodyContext),
    <<"2">> = bender_generator_client:gen_sequence(SequenceID, WoodyContext),
    <<"4">> = bender_generator_client:gen_sequence(SequenceID, WoodyContext, #{minimum => 4}),
    <<"5">> = bender_generator_client:gen_sequence(SequenceID, WoodyContext).

get_idempotent_key(ExternalID) ->
    bender_client:get_idempotent_key(<<"domain">>, <<"prefix">>, <<"party">>, ExternalID).
