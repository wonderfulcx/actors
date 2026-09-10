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

Application admission is checked under the runtime gate, including direct scheduled
and connection lifecycle events that bypass ordinary RPC dispatch. Once destruction
is fenced, only runtime cleanup/state plumbing remains admissible. Fencing releases
its writer guard before joining the effect pump; cancellation may interrupt a pump
waiting for the gate but never abandons an already-started delivery transaction.

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

Raw HTTP, queue, raw WebSocket, subscription, and generic disconnect events remain
rejected. Framework REST/RPC request connections are admitted only as ephemeral action
context and do not invoke socket lifecycle methods. Managed ActorConnect connection
open and close callbacks execute as reserved transactional commands. This primitive
does not itself implement Wonderful ticket, origin, tenant, or deployment policy;
those checks must be supplied by the production integration. A failed open command rejects the
connection. Hibernatable connections therefore do
not keep the actor resident, and wake recreates the component from committed state.

## WebSocket and scheduling effects

Guest output uses bounded `vbare::OwnedVersionedData` payloads generated from
`rivetkit-wasmtime/schemas/v1.bare`:

- `wonderful.cell.socket.state` becomes a `ConnHibernation` state delta and commits
  atomically with actor state/result;
- `wonderful.cell.socket.send` and `.broadcast` deliver only after commit using a
  stable, versioned `(operation_id, effect_index)` envelope;
- `wonderful.cell.socket.close` disconnects a live connection after commit;
- `wonderful.cell.schedule.at` upserts a stable one-shot schedule whose action carries
  a versioned command envelope;
- `wonderful.cell.schedule.cancel` removes the stable schedule;
- `wonderful.cell.destroy` requests actor destruction after commit.

Bespoke BARE ActorConnect callers can submit the internal versioned action envelope.
Generic JSON/CBOR ActorConnect is not compatible with that inner encoding, and the
current adapter does not map raw Wonderful JSON messages to `onMessage` or allocate
transport-owned operation IDs for them. A dedicated integration is required; a raw
u16 transport message index cannot serve as a permanent ID because it wraps.
`socket:` and `schedule:` operation-ID prefixes are server-owned and rejected on
external actions. Expired connection-open replies cancel the guest invocation and
discard its Store before commit.

A per-actor effect pump resumes pending effects on startup and after every command,
delivers in commit/index order, records failed attempts, and retries with backoff while
holding an internal keep-awake region. Its pending-effect scans, acknowledgements, and
compaction share the exclusive runtime writer fence so the single remote SQLite
connection never interleaves transactions. Cancellation is observed only between
deliveries, never by dropping an in-flight actor-local transaction. Socket delivery is ordered
at-least-once: durable acknowledgement follows active Envoy writer admission, and a crash
after admission and before acknowledgement may duplicate an envelope. Missing closed
connections are acknowledged as stale best-effort output. Schedule insertion/cancellation
commits with the command aggregate; ordered schedule effects reconcile alarm transport
and acknowledge idempotently without postponing schedule visibility. Scheduled targets
must be manifest-declared commands. Alarm
synchronization failures install one actor-scoped retry that keeps the actor awake until
the local/engine alarm is armed. Every actor generation resubmits its persisted alarm
because a prior Envoy acknowledgement proves only local writer admission. Due one-shot
schedules remain durable until their action replies, closing the delete-before-admission
crash window. Claiming atomically returns the current row so a pre-claim replacement
cannot dispatch an obsolete payload. A persisted one-shot claim token prevents completion of an old dispatch
from deleting a same-ID/same-deadline replacement. Compaction runs after every effect
attempt, and commands that would add effects are rejected once the bounded pending-effect
operation limit is reached; a committed destruction request always bypasses that
backpressure so deletion remains available.

## Query replicas

The immutable bundle manifest declares every method as `command` or `query`. The
versioned action envelope repeats the mode so `ActorTask` can avoid taking the
transactional dispatch slot for queries; the runtime verifies that untrusted hint
against the manifest before execution. A mismatch never reaches guest code.

Commands execute on the canonical resident under an exclusive writer gate. Queries
hold a shared reader gate and create an independent component Store from the current
committed state and transactional commit sequence. The reader gate keeps the state
and actor-local database at that version until the query finishes. Query replicas
never share the writer's heap and are discarded after one call. This is whole-query
writer exclusion, not MVCC or a separately copied SQLite snapshot. A query's native
SQL task retains the reader guard until the executor returns, even when guest timeout
or shutdown cancels the surrounding invocation.

All guest SQL is bounded before execution and cannot reference `_rivet_*`, `sqlite_*`,
dangerous pragmas, extension loading, or file functions. A trusted host prefix makes the
SQLite executor enforce row/byte result limits incrementally before materialization.
Query SQL additionally accepts only one `SELECT` against public tables. Mutable
statements, CTEs, and statement separators fail with `CELL_QUERY_MUTATION_VIOLATION`. The host records attempted SQL mutation even if guest
code catches the import error. Returned state must exactly equal the snapshot and
returned effects/destruction intent must be empty, or the query fails with the same
code.

## Resource controls

The factory enforces source/manifest/state/result/effect and SQL limits, aggregate effect
bytes, bounded guest errors, per-actor query concurrency, explicit component instance/
memory/table/table-element limits, fuel budgets for activation and each method, and a
wall-clock method deadline. Fuel-based cooperative yielding lets the executor poll
CPU-only guest deadlines. Activation uses one deadline spanning component instantiation
and the activation export. The linear-memory limit is per memory, not a combined Store
or process-memory limit; host and SQLite allocations are outside it.

Guest SQL carries an absolute deadline to the executor, including queue time. The native
executor additionally caps VM work, expression/statement structure and value size with
scoped limits restored after each statement. These do not establish a total SQLite or
process-memory quota; deployment memory limits remain necessary. Result collection
limits alone are not an execution budget.

Terminal operation results are compacted to the configured newest-record bound;
operations with pending effects remain protected. Schema v3 retains minimal permanent
identity tombstones so expired results reject retries instead of reexecuting. Identity
storage grows with lifetime operations. Fuel exhaustion and traps durably fail the
operation and discard the resident.

## Production-path fault validation

`rivetkit-wasmtime/tests/production_fault_model.rs` launches the real `rivet-engine`
binary and serves a real `CoreRegistry` with this factory. It validates operation-ID
deduplication, concurrent command serialization, immutable query snapshots,
actor-local SQLite, bundle reload across sleep/wake, hibernating WebSocket restoration,
ordered post-commit socket envelopes, engine process restart, state/SQLite recovery,
and Cell-requested destruction. A separate actor persists historical v1 schedule
arguments, sleeps, rehydrates and fires them through the actual scheduler. Build
`rivet-engine` before running the test target.
Crate-local tests retain deterministic coverage for pre/post-commit failures and
indeterminate storage outcomes that cannot yet be injected through the engine API.
A separate deterministic fault test intercepts one successful runtime reply after
aggregate commit, restarts the real engine while its HTTP request remains in flight,
and loses the reply. Retrying the same ID must return the committed result without
executing the command twice. This tests post-commit reply loss, not interruption
inside SQLite COMMIT or the socket delivery/acknowledgement window. The broader
lifecycle scenario also tests engine restart after sleep.
The checked-in component is Rust and ignores bundle source; these tests are
not proof of real TypeScript bundle execution, Wonderful JSON socket framing,
platform capabilities, or live Wonderful deployment authorization.

See also [Transactional actor consistency](transactional-actor-consistency.md).
