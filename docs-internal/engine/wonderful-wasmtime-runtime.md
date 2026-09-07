# Wonderful Wasmtime actor runtime

`rivetkit-wasmtime` is an opt-in native `ActorFactory` for Wonderful Cells. It
combines Rivet's transactional actor mode with an isolated Wasmtime component
instance for each active Cell.

## Ownership and isolation

The factory compiles the component once. Compiled machine code is immutable and
shareable. `ActorFactory::run` creates a fresh `Resident` for each actor start. That
resident exclusively owns:

- a Wasmtime `Store<StoreData>`;
- the component instance and generated bindings;
- every linear memory and table created by that instance;
- the embedded guest-language heap (SpiderMonkey when the component is built with
  ComponentizeJS/StarlingMonkey).

A resident is never shared between actors. The bundle loader may share immutable
source and manifest bytes. Sleep or process loss drops the resident; wake constructs
a new instance and hydrates the last committed actor state.

## Operation sequence

1. The caller sends a versioned `CellActionEnvelope` containing a stable operation
   ID, input bytes, and start time.
2. The adapter durably admits the operation through
   `ActorContext::begin_transactional_operation`.
3. A duplicate operation returns its existing terminal result. A mismatched digest
   or in-progress operation fails closed according to the core contract.
4. The resident executes the guest command once. Guest SQL imports operate on the
   active `TransactionalOperation`, so they join its actor-local SQLite transaction.
5. The guest returns state, result, and buffered effects. The adapter validates all
   configured bounds and commits the aggregate transaction.
6. Only a successful commit produces the actor action reply. A commit error is
   treated as indeterminate: the resident is discarded and the actor run exits so
   durable state can be reread before further dispatch.
7. A guest error durably fails the operation, rolls back guest SQL, and discards the
   resident. Error codes/messages are bounded and sanitized before persistence.

Guest destruction intent is persisted as the ordered
`wonderful.cell.destroy` effect. Other guest effects retain guest order and receive
the transactional core's `(commit_sequence, effect_index)` ordering.

## Capability boundary

The component ABI is `rivetkit-rust/packages/rivetkit-wasmtime/wit/cell.wit`.
Actor-local SQL and sandboxed WASI Preview 2 are linked. The WASI context has closed
stdin, sink stdout/stderr, no arguments, no environment variables, no filesystem
preopens, and deny-by-default TCP, UDP, and DNS policy. It provides host clocks and
cryptographic randomness required by ComponentizeJS. WASI HTTP is not linked, so a
guest cannot perform network requests through the component. Component instantiation
fails if a guest requests any other undeclared capability.

HTTP, queue, and connection events are rejected until their transactional adapters
exist. WebSocket and scheduling effects are follow-up work and must preserve the
same commit-before-visibility rule.

## Resource controls

The factory enforces source/manifest/state/result/effect byte limits, component
instance/memory/table limits, fuel budgets for activation and each command, and a
wall-clock command deadline. Fuel exhaustion and traps durably fail the operation
and discard the resident.

See also [Transactional actor consistency](transactional-actor-consistency.md).
