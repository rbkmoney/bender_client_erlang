-module(bender_client).

-include_lib("bender_proto/include/bender_thrift.hrl").

-type woody_context() :: woody_context:ctx().
-type context_data() :: #{binary() => term()}.
-type bender_context() :: #{binary() => term()}.
-type sequence_params() :: #{minimum => integer()}.
-type generated_id() :: {binary(), integer() | undefined}.

-export_type([
    bender_context/0,
    context_data/0,
    generated_id/0
]).

-export([gen_snowflake/2]).
-export([gen_snowflake/3]).
-export([gen_sequence/3]).
-export([gen_sequence/4]).
-export([gen_sequence/5]).
-export([gen_constant/3]).
-export([gen_constant/4]).
-export([get_idempotent_key/4]).
-export([get_internal_id/2]).

-define(SCHEMA_VER1, 1).

-spec gen_snowflake(binary(), woody_context()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_snowflake(IdempotentKey, WoodyContext) ->
    gen_snowflake(IdempotentKey, WoodyContext, undefined).

-spec gen_snowflake(binary(), woody_context(), context_data()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_snowflake(IdempotentKey, WoodyContext, Context) ->
    Snowflake = {snowflake, #bender_SnowflakeSchema{}},
    generate_id(IdempotentKey, Snowflake, WoodyContext, Context).

-spec gen_sequence(binary(), binary(), woody_context()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_sequence(IdempotentKey, SequenceID, WoodyContext) ->
    gen_sequence(IdempotentKey, SequenceID, WoodyContext, undefined).

-spec gen_sequence(binary(), binary(), woody_context(), context_data()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_sequence(IdempotentKey, SequenceID, WoodyContext, Context) ->
    gen_sequence(IdempotentKey, SequenceID, WoodyContext, Context, #{}).

-spec gen_sequence(binary(), binary(), woody_context(), context_data(), sequence_params()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_sequence(IdempotentKey, SequenceID, WoodyContext, Context, Params) ->
    Minimum = maps:get(minimum, Params, undefined),
    Sequence =
        {sequence, #bender_SequenceSchema{
            sequence_id = SequenceID,
            minimum = Minimum
        }},
    generate_id(IdempotentKey, Sequence, WoodyContext, Context).

-spec gen_constant(binary(), binary(), woody_context()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_constant(IdempotentKey, ConstantID, WoodyContext) ->
    gen_constant(IdempotentKey, ConstantID, WoodyContext, undefined).

-spec gen_constant(binary(), binary(), woody_context(), context_data()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_constant(IdempotentKey, ConstantID, WoodyContext, Context) ->
    Constant = {constant, #bender_ConstantSchema{internal_id = ConstantID}},
    generate_id(IdempotentKey, Constant, WoodyContext, Context).

-spec get_idempotent_key(binary(), atom() | binary(), binary(), binary() | undefined) -> binary().
get_idempotent_key(Domain, Prefix, PartyID, ExternalID) when is_atom(Prefix) ->
    get_idempotent_key(Domain, atom_to_binary(Prefix, utf8), PartyID, ExternalID);
get_idempotent_key(Domain, Prefix, PartyID, undefined) ->
    get_idempotent_key(Domain, Prefix, PartyID, gen_external_id());
get_idempotent_key(Domain, Prefix, PartyID, ExternalID) ->
    <<Domain/binary, "/", Prefix/binary, "/", PartyID/binary, "/", ExternalID/binary>>.

-spec get_internal_id(binary(), woody_context()) ->
    {ok, binary(), context_data()}
    | {error, internal_id_not_found}.
get_internal_id(ExternalID, WoodyContext) ->
    case bender_client_woody:call('Bender', 'GetInternalID', {ExternalID}, WoodyContext) of
        {ok, #bender_GetInternalIDResult{internal_id = InternalID, context = Context}} ->
            UnmarshaledCtx = bender_msgp_marshalling:unmarshal(Context),
            {ok, InternalID, UnmarshaledCtx};
        {exception, #bender_InternalIDNotFound{}} ->
            {error, internal_id_not_found}
    end.

%% Internal

gen_external_id() ->
    genlib:unique().

generate_id(Key, BenderSchema, WoodyContext, Context) ->
    MarshalledContext = bender_msgp_marshalling:marshal(Context),
    Args = {Key, BenderSchema, MarshalledContext},
    case bender_client_woody:call('Bender', 'GenerateID', Args, WoodyContext) of
        {ok, #bender_GenerationResult{
            internal_id = InternalID,
            context = undefined
        }} ->
            {ok, InternalID};
        {ok, #bender_GenerationResult{
            internal_id = InternalID,
            context = Context
        }} ->
            {ok, InternalID, Context}
    end.
