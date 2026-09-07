# rivetkit-wasmtime

Opt-in native Wasmtime adapter for Wonderful Cells. The adapter implements Rivet's
`ActorFactory` directly; it does not use the N-API/browser runtimes and has no Node or
V8 dependency.

## Runtime contract

- The component artifact is compiled once and may be shared.
- Every active actor creates its own Wasmtime `Store`, component instance, linear
  memories, tables, and guest-language heap.
- The bundle source and manifest are immutable, length-separated, and SHA-256
  verified before activation.
- Actor state hydrates the component on activation and after wake/replacement.
- Manifest-declared commands use Rivet's opt-in transactional operation API. Guest
  SQLite calls run through the active actor-local transaction. State, result, and
  buffered effects commit before the action reply becomes visible.
- Manifest-declared queries run concurrently, up to a strict per-actor limit, in
  disposable component Stores against a writer-fenced committed state/database version.
  They receive bounded `SELECT`-only SQL and cannot mutate state or emit effects.
- All guest SQL denies Rivet/SQLite metadata and extension surfaces. SQL text, parameters,
  rows, and result bytes are bounded before or during executor materialization.
- Guest failure durably terminalizes the operation and discards the resident. An
  uncertain commit fences the actor and also discards the resident.
- Sandboxed WASI Preview 2 uses closed/sink stdio, no args or environment, no
  filesystem preopens, and deny-by-default networking. WASI HTTP is not linked.
- Hibernating connection open/close callbacks are reserved transactional commands.
  Socket state joins the aggregate commit; send/broadcast/close intents are delivered
  afterward by a retrying ordered effect pump that holds internal keep-awake while work
  remains and acknowledges socket output only after active Envoy writer admission.
  Persisted effects and socket delivery envelopes use versioned BARE generated from
  `schemas/v1.bare`.
- One-shot schedule upsert/cancel mutations commit with the command aggregate; ordered
  effects reconcile alarm transport and acknowledge idempotently. Due one-shots remain
  durable until their action replies, and persisted claim tokens fence replacement races.
  Alarm writer admission is acknowledged, every generation idempotently resubmits the
  persisted alarm, and failed synchronization retries while keeping the actor awake.
- Effect-producing commands are backpressured at a bounded pending-operation limit, while
  unprotected terminal operations are compacted after every delivery attempt. Committed
  destruction bypasses effect backpressure so deletion remains available.
- Committed destruction establishes the local admission fence before replying. The
  durable destroy intent remains pending until engine-owned destruction removes the actor.
- Raw HTTP, queues, raw WebSockets, subscriptions, and generic disconnect events fail
  closed until dedicated transactional adapters are implemented.

The component ABI is defined in [`wit/cell.wit`](wit/cell.wit). A production
component may embed ComponentizeJS/StarlingMonkey (SpiderMonkey in Wasm) or another
component-compatible guest language. Wasmtime itself does not execute JavaScript.

## Test fixture

`tests/fixtures/wonderful_cell_test_guest.component.wasm` is a small Rust component
used to test Store isolation and transaction behavior without coupling tests to a
JavaScript toolchain. Rebuild it with:

```bash
fixture=rivetkit-rust/packages/rivetkit-wasmtime/tests/fixtures/guest
cargo build --manifest-path "$fixture/Cargo.toml" \
  --release --target wasm32-unknown-unknown

# Componentize the resulting core module with wit-component 0.244.0, then:
sha256sum \
  rivetkit-rust/packages/rivetkit-wasmtime/tests/fixtures/wonderful_cell_test_guest.component.wasm \
  > rivetkit-rust/packages/rivetkit-wasmtime/tests/fixtures/wonderful_cell_test_guest.component.wasm.sha256
```

The fixture intentionally mutates its ephemeral state before one failure. Tests prove
that a failed resident is discarded and the replacement sees only committed state.

The production-path fault model additionally requires the engine binary:

```bash
cargo build -p rivet-engine
cargo test -p rivetkit-wasmtime --test production_fault_model
```

It traverses the public engine gateway, Envoy, `CoreRegistry`, remote actor-local
SQLite, the Wasmtime adapter, hibernating WebSockets, sleep/wake, engine restart, and
destruction rather than substituting mocks.
