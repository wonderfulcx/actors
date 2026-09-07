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
- Manifest-declared queries run concurrently in disposable component Stores against
  a writer-fenced committed state/database version. They receive bounded `SELECT`-only
  SQL and cannot mutate state or emit effects.
- Guest failure durably terminalizes the operation and discards the resident. An
  uncertain commit fences the actor and also discards the resident.
- Sandboxed WASI Preview 2 uses closed/sink stdio, no args or environment, no
  filesystem preopens, and deny-by-default networking. WASI HTTP is not linked.
- Raw HTTP, queues, and connection events fail closed until dedicated transactional
  adapters are implemented.

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
