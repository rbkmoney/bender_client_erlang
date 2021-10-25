-module(bender_client).

-include_lib("bender_proto/include/bender_thrift.hrl").

-type woody_context() :: woody_context:ctx().
-type bender_context() :: #{binary() => term()}.
-type sequence_params() :: #{minimum => integer()}.
-type bender_schema() :: bender_thrift:'GenerationSchema'().
-type id() :: binary().

-type result() :: {ok, id()} | {ok, id(), bender_context()}.

-export_type([
    bender_context/0,
    bender_schema/0,
    id/0
]).

-export([gen_snowflake/2]).
-export([gen_snowflake/3]).
-export([gen_sequence/3]).
-export([gen_sequence/4]).
-export([gen_sequence/5]).
-export([gen_constant/3]).
-export([gen_constant/4]).
-export([gen_id/3]).
-export([gen_id/4]).

-export([get_idempotent_key/4]).
-export([get_internal_id/2]).

-spec gen_snowflake(binary(), woody_context()) -> result().
gen_snowflake(IdempotentKey, WoodyContext) ->
    gen_snowflake(IdempotentKey, WoodyContext, undefined).

-spec gen_snowflake(binary(), woody_context(), bender_context() | undefined) -> result().
gen_snowflake(IdempotentKey, WoodyContext, Context) ->
    Snowflake = {snowflake, #bender_SnowflakeSchema{}},
    gen_id(IdempotentKey, Snowflake, WoodyContext, Context).

-spec gen_sequence(binary(), binary(), woody_context()) -> result().
gen_sequence(IdempotentKey, SequenceID, WoodyContext) ->
    gen_sequence(IdempotentKey, SequenceID, WoodyContext, undefined).

-spec gen_sequence(binary(), binary(), woody_context(), bender_context() | undefined) -> result().
gen_sequence(IdempotentKey, SequenceID, WoodyContext, Context) ->
    gen_sequence(IdempotentKey, SequenceID, WoodyContext, Context, #{}).

-spec gen_sequence(binary(), binary(), woody_context(), bender_context() | undefined, sequence_params()) -> result().
gen_sequence(IdempotentKey, SequenceID, WoodyContext, Context, Params) ->
    Minimum = maps:get(minimum, Params, undefined),
    Sequence =
        {sequence, #bender_SequenceSchema{
            sequence_id = SequenceID,
            minimum = Minimum
        }},
    gen_id(IdempotentKey, Sequence, WoodyContext, Context).

-spec gen_constant(binary(), binary(), woody_context()) -> result().
gen_constant(IdempotentKey, ConstantID, WoodyContext) ->
    gen_constant(IdempotentKey, ConstantID, WoodyContext, undefined).

-spec gen_constant(binary(), binary(), woody_context(), bender_context() | undefined) -> result().
gen_constant(IdempotentKey, ConstantID, WoodyContext, Context) ->
    Constant = {constant, #bender_ConstantSchema{internal_id = ConstantID}},
    gen_id(IdempotentKey, Constant, WoodyContext, Context).

-spec get_idempotent_key(binary(), atom() | binary(), binary(), binary() | undefined) -> binary().
get_idempotent_key(Domain, Prefix, PartyID, ExternalID) when is_atom(Prefix) ->
    get_idempotent_key(Domain, atom_to_binary(Prefix, utf8), PartyID, ExternalID);
get_idempotent_key(Domain, Prefix, PartyID, undefined) ->
    get_idempotent_key(Domain, Prefix, PartyID, gen_external_id());
get_idempotent_key(Domain, Prefix, PartyID, ExternalID) ->
    <<Domain/binary, "/", Prefix/binary, "/", PartyID/binary, "/", ExternalID/binary>>.

-spec get_internal_id(binary(), woody_context()) -> result() | {error, internal_id_not_found}.
get_internal_id(ExternalID, WoodyContext) ->
    case bender_client_woody:call('Bender', 'GetInternalID', {ExternalID}, WoodyContext) of
        {ok, #bender_GetInternalIDResult{internal_id = InternalID, context = Context}} ->
            case bender_msgp_marshalling:unmarshal(Context) of
                undefined ->
                    {ok, InternalID};
                UnmarshaledCtx ->
                    {ok, InternalID, UnmarshaledCtx}
            end;
        {exception, #bender_InternalIDNotFound{}} ->
            {error, internal_id_not_found}
    end.

%% Internal

gen_external_id() ->
    genlib:unique().

-spec gen_id(binary(), bender_schema(), woody_context()) -> result().
gen_id(Key, BenderSchema, WoodyContext) ->
    gen_id(Key, BenderSchema, WoodyContext, undefined).

-spec gen_id(binary(), bender_schema(), woody_context(), bender_context() | undefined) -> result().
gen_id(Key, BenderSchema, WoodyContext, Context) ->
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
            context = SavedContext
        }} ->
            {ok, InternalID, bender_msgp_marshalling:unmarshal(SavedContext)}
    end.
