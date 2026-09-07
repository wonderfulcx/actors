# Transactional actor consistency

Transactional consistency is an opt-in execution mode for runtimes that need a
single canonical writer and durable operation outcomes. Ordinary Rivet actors
remain concurrent and retain their existing persistence behavior.

## Enabling the mode

Set `ActorConfig.consistency` to `ActorConsistency::Transactional` and construct
the native `ActorFactory` with `with_transactional_operations()`.

The factory marker is a capability declaration: the runtime adapter must use the
transactional operation APIs and must not make results or external effects
visible before commit. Actor startup fails with
`actor.transactional_runtime_required` when the mode is enabled for an adapter
that does not declare support. NAPI and browser runtimes do not currently
support the mode.

## Operation lifecycle

A mutating operation has two durable boundaries:

1. `ActorContext::begin_transactional_operation` commits a `started` operation
   row before guest code executes.
2. `TransactionalOperation::commit` atomically commits user SQLite writes,
   actor and connection state deltas, the result, a monotonic commit sequence,
   and buffered effect intents.

`ActorTask` forwards at most one action at a time to a transactional runtime. It
does not release the next action until the runtime replies. A conforming runtime
replies only after `commit` or `fail` has completed.

Operation IDs are idempotency keys. Reusing an ID with the same operation kind,
name, and input returns its durable record. Reusing it with different input
fails with `actor.operation_id_conflict`.

## Failure and recovery

Guest failures roll back the operation transaction, discard staged state and
effects, and then persist a sanitized terminal failure. If terminal failure
persistence fails, the operation remains `started` and no speculative result is
safe to expose.

Started operations are **never replayed**. On actor startup,
`recover_interrupted_transactional_operations` changes every remaining
`started` operation to `interrupted` before the runtime becomes ready. This
preserves at-most-once execution after the durable-start boundary.

An indeterminate remote SQLite commit must fence and discard the resident actor.
The next actor generation reads the operation row:

- a committed terminal row is authoritative;
- a remaining `started` row is terminalized as interrupted;
- an unavailable store keeps the actor unavailable.

## Buffered effects

Transactional effects are inserted in the same commit as state and result. They
are selected in `(commit_sequence, effect_index)` order and acknowledged after
external delivery. Delivery is ordered and at least once: a crash after delivery
but before acknowledgement can redeliver an effect. Destinations must use the
operation/effect identity for idempotency where available.

No effect may leave the runtime before the aggregate commit. Pending effects
protect their operation records from retention pruning.

## Actor-local schema

Internal schema v2 appends:

- `_rivet_operations` for durable start, terminal result, and idempotency;
- `_rivet_operation_effects` for ordered post-commit delivery;
- `_rivet_transactional_runtime` for the actor-local commit sequence.

The tables live in the same actor-local SQLite database as user SQL and actor
state. Successful completion therefore uses `ActorStateTransaction`; it does
not introduce a second transaction coordinator.

## Wasmtime runtime contract

The initial supported adapter hosts one isolated Wasmtime `Store` and component
instance per active actor. It must:

- admit each command through `begin_transactional_operation` before calling the
  guest;
- route guest SQL through `TransactionalOperation::execute_sql`;
- keep state and effects in operation-local buffers;
- call `commit` before replying;
- call `fail` and discard/reset the resident after traps or handler errors;
- fence and discard the resident on indeterminate storage outcomes;
- expose only bounded, explicitly linked host capabilities.

Compiled component machine code may be shared. Stores, linear memory, the
StarlingMonkey/SpiderMonkey heap, and resident component instances may not be
shared between active actors. See
[Wonderful Wasmtime actor runtime](wonderful-wasmtime-runtime.md) for the concrete
adapter sequence and capability boundary.

## Read-only query replicas

The Wonderful Wasmtime adapter classifies manifest-declared queries as concurrent
actions while transactional commands remain serialized through the canonical
writer. Query replicas run against a writer-fenced committed state/database version,
have read-only SQL and reduced host capabilities, never share the writer's ephemeral
heap, and fail mutation attempts at runtime. Build-time query purity checks remain
ergonomic checks, not the security boundary.
