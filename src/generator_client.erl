-module(generator_client).

-export([gen_by_snowflake/1]).
-export([gen_by_sequence/2]).
-export([gen_by_sequence/3]).
-export([gen_by_constant/2]).


-include_lib("bender_proto/include/bender_thrift.hrl").

-type woody_context() :: woody_context:ctx().
-type sequence_params() :: #{minimum => integer()}.

-spec gen_by_snowflake(woody_context()) ->
    {ok, {binary(), integer() | undefined}}.

gen_by_snowflake(WoodyContext) ->
    Snowflake = {snowflake, #bender_SnowflakeSchema{}},
    generate_id(Snowflake, WoodyContext).

-spec gen_by_sequence(binary(), woody_context()) ->
    {ok, {binary(), integer() | undefined}}.

gen_by_sequence(SequenceID, WoodyContext) ->
    gen_by_sequence(SequenceID, WoodyContext, #{}).

-spec gen_by_sequence(binary(), woody_context(), sequence_params()) ->
    {ok, {binary(), integer() | undefined}}.

gen_by_sequence(SequenceID, WoodyContext, Params) ->
    Minimum = maps:get(minimum, Params, undefined),
    Sequence = {sequence, #bender_SequenceSchema{
        sequence_id = SequenceID,
        minimum = Minimum
    }},
    generate_id(Sequence, WoodyContext).

-spec gen_by_constant(binary(), woody_context()) ->
    {ok, {binary(), integer() | undefined}}.

gen_by_constant(ConstantID, WoodyContext) ->
    Constant = {constant, #bender_ConstantSchema{internal_id = ConstantID}},
    generate_id(Constant, WoodyContext).

%%

generate_id(BenderSchema, WoodyContext) ->
    Args = [BenderSchema],
    {ok, #bender_GeneratedID{id = ID, integer_id = IntegerID}} =
        bender_client_woody:call('Generator', 'GenerateID', Args, WoodyContext),
    {ok, {ID, IntegerID}}.
