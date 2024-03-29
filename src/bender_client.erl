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

-export([gen_snowflake/4]).
-export([gen_snowflake/3]).
-export([gen_sequence/4]).
-export([gen_sequence/5]).
-export([gen_sequence/6]).
-export([gen_constant/4]).
-export([gen_constant/5]).
-export([get_idempotent_key/4]).
-export([get_internal_id/2]).

-define(SCHEMA_VER1, 1).

-spec gen_snowflake(binary(), integer(), woody_context()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_snowflake(IdempotentKey, Hash, WoodyContext) ->
    gen_snowflake(IdempotentKey, Hash, WoodyContext, #{}).

-spec gen_snowflake(binary(), integer(), woody_context(), context_data()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_snowflake(IdempotentKey, Hash, WoodyContext, CtxData) ->
    Snowflake = {snowflake, #bender_SnowflakeSchema{}},
    generate_id(IdempotentKey, Snowflake, Hash, WoodyContext, CtxData).

-spec gen_sequence(binary(), binary(), integer(), woody_context()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_sequence(IdempotentKey, SequenceID, Hash, WoodyContext) ->
    gen_sequence(IdempotentKey, SequenceID, Hash, WoodyContext, #{}).

-spec gen_sequence(binary(), binary(), integer(), woody_context(), context_data()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_sequence(IdempotentKey, SequenceID, Hash, WoodyContext, CtxData) ->
    gen_sequence(IdempotentKey, SequenceID, Hash, WoodyContext, CtxData, #{}).

-spec gen_sequence(binary(), binary(), integer(), woody_context(), context_data(), sequence_params()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_sequence(IdempotentKey, SequenceID, Hash, WoodyContext, CtxData, Params) ->
    Minimum = maps:get(minimum, Params, undefined),
    Sequence =
        {sequence, #bender_SequenceSchema{
            sequence_id = SequenceID,
            minimum = Minimum
        }},
    generate_id(IdempotentKey, Sequence, Hash, WoodyContext, CtxData).

-spec gen_constant(binary(), binary(), integer(), woody_context()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_constant(IdempotentKey, ConstantID, Hash, WoodyContext) ->
    gen_constant(IdempotentKey, ConstantID, Hash, WoodyContext, #{}).

-spec gen_constant(binary(), binary(), integer(), woody_context(), context_data()) ->
    {ok, generated_id()}
    | {error, {external_id_conflict, generated_id()}}.
gen_constant(IdempotentKey, ConstantID, Hash, WoodyContext, CtxData) ->
    Constant = {constant, #bender_ConstantSchema{internal_id = ConstantID}},
    generate_id(IdempotentKey, Constant, Hash, WoodyContext, CtxData).

-spec get_idempotent_key(binary(), atom() | binary(), binary(), binary() | undefined) -> binary().
get_idempotent_key(Domain, Prefix, PartyID, ExternalID) when is_atom(Prefix) ->
    get_idempotent_key(Domain, atom_to_binary(Prefix, utf8), PartyID, ExternalID);
get_idempotent_key(Domain, Prefix, PartyID, undefined) ->
    get_idempotent_key(Domain, Prefix, PartyID, gen_external_id());
get_idempotent_key(Domain, Prefix, PartyID, ExternalID) ->
    <<Domain/binary, "/", Prefix/binary, "/", PartyID/binary, "/", ExternalID/binary>>.

-spec get_internal_id(binary(), woody_context()) ->
    {ok, {binary(), integer() | undefined}, context_data()}
    | {error, internal_id_not_found}.
get_internal_id(ExternalID, WoodyContext) ->
    case bender_client_woody:call('Bender', 'GetInternalID', {ExternalID}, WoodyContext) of
        {ok, #bender_GetInternalIDResult{
            internal_id = InternalID,
            integer_internal_id = IntegerInternalID,
            context = Context
        }} ->
            UnmarshaledCtx = bender_msgp_marshalling:unmarshal(Context),
            {ok, {InternalID, IntegerInternalID}, get_context_data(UnmarshaledCtx)};
        {exception, #bender_InternalIDNotFound{}} ->
            {error, internal_id_not_found}
    end.

%% Internal

gen_external_id() ->
    genlib:unique().

generate_id(Key, BenderSchema, Hash, WoodyContext, CtxData) ->
    Context = bender_msgp_marshalling:marshal(#{
        <<"version">> => ?SCHEMA_VER1,
        <<"params_hash">> => Hash,
        <<"context_data">> => CtxData
    }),
    Args = {Key, BenderSchema, Context},
    Result =
        case bender_client_woody:call('Bender', 'GenerateID', Args, WoodyContext) of
            {ok, #bender_GenerationResult{
                internal_id = InternalID,
                integer_internal_id = IntegerInternalID,
                context = undefined
            }} ->
                {ok, {InternalID, IntegerInternalID}};
            {ok, #bender_GenerationResult{
                internal_id = InternalID,
                integer_internal_id = IntegerInternalID,
                context = Ctx
            }} ->
                #{<<"params_hash">> := BenderHash} = bender_msgp_marshalling:unmarshal(Ctx),
                {ok, {InternalID, IntegerInternalID}, BenderHash}
        end,
    case Result of
        {ok, ID} -> {ok, ID};
        {ok, ID, Hash} -> {ok, ID};
        {ok, ID, _Other} -> {error, {external_id_conflict, ID}}
    end.

-spec get_context_data(bender_context()) -> undefined | context_data().
get_context_data(Context) ->
    maps:get(<<"context_data">>, Context, #{}).
