# Go-compatible Service Registration and Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect TarsRust registration, object-name discovery, refresh, cache, and healthy routing with the behavior specified from TarsGo.

**Architecture:** Retain synchronous proxy construction; discover asynchronously before the first invocation through the existing EndpointManager. ServantProxy owns refresh lifetime and routing; Application owns registration lifetime. Native QueryF provides discovery only; platform configuration and NodeF retain responsibility for native service availability.

**Tech Stack:** Existing Rust workspace, Tokio, async-trait, parking_lot, Serde, TOML, and Tars codec.

**Spec:** `docs/superpowers/specs/2026-10-15-service-discovery-design.md`

## Global Constraints

- Preserve synchronous `Communicator::string_to_proxy(&self, obj_name: &str) -> Result<Arc<ServantProxy>>` and generated proxy compatibility.
- Default discovery uses SameGroup; SET discovery uses SameSet and setdivision.
- Query failures and empty active results preserve existing active endpoints.
- Native registration and deregistration are no-ops; custom callback errors are logged and do not stop startup or shutdown.
- Direct endpoints bypass discovery; no new UDP or TLS transport is included.
- No synchronous locks held across await; no detached refresh task retains its owner forever.
- Reuse existing components and dependencies; complete feature verification before final implementation commit.

## Review Focus

- First invocation cancelled during discovery must permit retry and leave no queue or response-map leak (Task 2).
- Concurrent cache updates must preserve other objects; damaged or mismatched records cannot supply endpoints (Task 3).
- Endpoint identity equality ignores metadata; weight-only updates must still reach selectors and adapters (Task 4).
- Direct `shutdown()` and signal-driven shutdown must each deregister once before closing business listeners (Task 5).
- Configuration changes must not redirect existing proxies or reuse proxies across Locator/SET/registrar identities (Task 2).

## File Responsibilities

- `tars-registry/src/registry/mod.rs`: Registrar, native QueryF protocol, cancellation-safe network queries, EndpointManager snapshots and SET selection.
- `tars-registry/tests/queryf.rs` (new): wire-level QueryF stub and failover tests.
- `tars-rpc/src/communicator/mod.rs`: registrar injection, configuration snapshots, identity-aware proxy cache.
- `tars-rpc/src/servant/mod.rs`: asynchronous discovery entry point, owned refresh lifetime, invocation and healthy routing.
- `tars-rpc/src/servant/discovery.rs` (new): discovery options and refresh lifecycle helpers, keeping proxy code focused.
- `tars-rpc/src/communicator/cache.rs` (new): versioned on-disk records and atomic updates.
- `tars-registry/src/adapter/mod.rs`, `tars-util/src/selector/mod.rs`: integrate existing health and selection algorithms as needed.
- `tars-util/src/util/config.rs`, `tars-util/src/util/platform_config.rs`: backward-compatible optional cache configuration and health-check interval.
- `tars-rpc/src/application/mod.rs`: configuration propagation and bounded custom registration lifecycle.
- `tars-rpc/tests/service_discovery.rs` (new), `tars-rpc/tests/platform_lifecycle.rs`: end-to-end discovery and registration tests.
- `docs/platform-integration.md`: public setup, lifecycle, cache, and transport limits.

## Task 1: Correct Native QueryF Discovery

**Interfaces:** Preserve `Registrar` methods and `TarsRegistry::new` / `with_timeout`. Internally replace blocking registry I/O with cancellable Tokio TCP operations. Keep QueryF framing in the registry layer and honor the configured total query timeout.

- [ ] Write failing `queryf` integration tests: default query function is SameGroup with id tag 1; SET query sends set id tag 2 and decodes outputs 3/4; ordinary outputs use 2/3; request servant matches the Locator object.
- [ ] Add assertions that nonzero packet return and nonzero business tag 0 fail; EndpointF metadata survives conversion; unreachable first registry falls through to a reachable second registry; stalled reads time out and release the connection. Assert each candidate is attempted at most once per query.
- [ ] Run `cargo test -p tars-registry --test queryf` and confirm the tests expose the existing protocol and failover gaps.
- [ ] Implement method-specific output tags, return-code validation, Locator name parsing, metadata conversion, deterministic candidate iteration, and cancellable TCP framing. Bound frame allocation using the existing protocol frame limit.
- [ ] Run `cargo test -p tars-registry` and confirm all tests pass.

## Task 2: Connect Object-Name Proxies to EndpointManager

**Interfaces:** Add `Communicator::set_registrar(&self, registrar: Arc<dyn Registrar>)`; add `EndpointManager::with_set_division(self, set_division: Option<String>) -> Self`. Introduce an internal `DiscoveryOptions` snapshot with object identity, registrar, SET, refresh interval, query timeout, and optional cache path. Add `ServantProxy::ensure_endpoints(&self) -> Result<()>` internally, called before normal, Hash, and oneway invocation. Existing direct `ServantProxy::new` remains valid.

- [ ] Write failing `service_discovery` tests using an injected recording Registrar and a real local RPC listener. Assert bare object invocation succeeds, concurrent first calls issue one initial query, SET reaches `query_servant_by_set`, and direct objects issue no registry calls.
- [ ] Assert missing Locator/registrar fails explicitly; a failed or cancelled initial query allows a later retry; calling synchronous construction outside a Tokio runtime works; Locator/SET/registrar changes create separate proxies while existing proxies retain their snapshot.
- [ ] Run `cargo test -p tars-rpc --test service_discovery` and confirm failures reproduce the missing integration.
- [ ] Wire registrar selection and identity-aware caching in Communicator, initialize locator properties consistently in `with_config`, and serialize first discovery with an async mutex. Enforce query timeout for custom registrars too. In EndpointManager preserve previous active endpoints for failed or empty refresh results.
- [ ] Start refresh only when async invocation supplies a runtime. Use weak ownership and an abortable task handle; include an internal close path and ensure task drop cancels an in-flight query. Apply successful snapshots to existing proxy routing.
- [ ] Add dynamic-refresh tests: A becomes B, failure/empty results retain A, releasing the owner stops further queries. Use explicit synchronization or Tokio paused time, avoiding wall-clock sleeps. Add cancellation coverage for invocation queue counts and pending response cleanup.
- [ ] Run `cargo test -p tars-rpc --test service_discovery` and `cargo test -p tars-registry` and confirm passing results.

## Task 3: Persist Discovery Cache and Recover on Cold Start

**Interfaces:** Add serde-default `ClientConfig::endpoint_cache_path: Option<String>`; internally `DiscoveryCache::load(&self, identity: &str) -> Result<Option<Vec<Endpoint>>>` and `DiscoveryCache::store(&self, identity: &str, endpoints: &[Endpoint]) -> Result<()>`. Cache DTOs encode all internal endpoint metadata without changing Endpoint identity equality. Use the workspace TOML dependency and a versioned record format.

- [ ] Add failing tests for cold-start query failure with matching cached endpoints, mismatched Locator/SET rejection, corrupted/unsupported-version cache rejection, metadata round-trip, and preservation of two objects updated concurrently.
- [ ] Run `cargo test -p tars-rpc cache` and confirm the new tests fail before implementation.
- [ ] Implement cache read/merge/write under a shared per-path asynchronous lock, a unique temporary file in the destination directory, and atomic rename. File operations must not block a runtime worker. Log cache I/O failures without rejecting a valid live discovery result.
- [ ] Consult cache only after initial live discovery fails or yields no usable endpoint. Save successful nonempty discovery results; retain periodic live refresh after fallback. Standalone clients without a path perform no disk I/O.
- [ ] Run `cargo test -p tars-rpc cache` and the discovery integration suite; confirm all assertions pass.

## Task 4: Connect Health, Hash, and Weight Selection

**Interfaces:** Preserve `AdapterProxy::check_active`, `is_active`, `reset`, and existing Selector contracts. Make proxy selection consume `Message` hash type/code. Add serde-default `ClientConfig::check_status_interval: u64` in milliseconds with a 10,000 ms default; preserve existing adapter failure thresholds and recovery interval.

- [ ] Write failing routing tests: deterministic ModHash and consistent Hash reach the expected nodes, static weights affect ordinary routing, unhealthy nodes are skipped, eligible recovery probes run, successful probes reset health, and all-unhealthy registry-active nodes remain eligible for fallback.
- [ ] Add tests for weight-only and SET metadata refresh at the same host/port, removed adapters closing, direct-node health checks, and oneway send failure accounting. Drive health time using a small internal test clock seam if existing wall-clock logic prevents deterministic tests.
- [ ] Run `cargo test -p tars-rpc routing` and `cargo test -p tars-registry adapter` to establish the failing cases.
- [ ] Integrate the existing selector algorithms and static-weight helper. Keep registry-active endpoints distinct from locally healthy candidates; prioritize eligible probes and update health on actual outcomes. Ensure snapshots replace metadata even when Endpoint Eq/Hash considers addresses equal.
- [ ] Extend the owned maintenance task to schedule health checks independently of discovery interval, including direct proxies. Do not create competing permanent background loops or hold selection locks during network operations.
- [ ] Run the routing, adapter, selector, and discovery tests; confirm deterministic passing results.

## Task 5: Application Registration Lifecycle and Final Verification

**Interfaces:** Add `Application::set_registrar(&self, registrar: Arc<dyn Registrar>)`, delegating client discovery injection to its Communicator. Propagate server SET settings and client configuration before constructing proxies. For application-managed cache, default to `data_path/<server>.tarsdat` when data_path is available unless explicitly overridden. Internal lifecycle callbacks receive `ServantInstance` and use a 5,000 ms timeout per invocation.

- [ ] Extend `platform_lifecycle` with a recording Registrar: register runs after binding, dynamic ports are actual nonzero ports, AdminObj is excluded, and metadata contains the exact complete servant name without double-prefixing.
- [ ] Assert failed/timed-out registration continues to the next adapter and server startup; failed/timed-out deregistration continues shutdown. Test direct shutdown and signal shutdown, including shutdown during registration, with exactly-once deregistration before listener closure.
- [ ] Run `cargo test -p tars-rpc --test platform_lifecycle` and confirm new cases fail before lifecycle integration.
- [ ] Implement bounded callback sequencing and track the business instance snapshot after binding. Coordinate shutdown entry points with one lifecycle guard; avoid holding server/config locks while awaiting callbacks. Preserve existing NodeF and AdminF tests.
- [ ] Update the public integration guide with native versus custom registration, object-name lookup, SET, cache configuration and Rust-only cache format, callback timeout semantics, and unchanged transport scope.
- [ ] Run `cargo test --workspace`; confirm no new ignored tests and all runnable tests pass. Run `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets`; resolve new diagnostics without unrelated formatting/refactoring.
- [ ] Exercise the wire tests with a Go QueryF encoder/service using the local TarsGo checkout. Record the exact command and observed results; if external dependencies or a live platform are unavailable, report that limitation without claiming platform deployment verification.
- [ ] Review the final diff against all spec acceptance criteria, resolve findings, run `git diff --check`, and commit the complete feature with `feat: implement Go-compatible service registration and discovery`.

## Execution Recommendation

Use native execution in this session: Tasks 2–5 share proxy/configuration lifecycle interfaces, so retaining one implementation context avoids repeated handoffs. Run a fresh whole-change review after integration. No temporary branch or worktree is required for these changes while the checkout remains clean apart from the approved documents.

## Execution Results (2026-10-15)

Tasks 1–5 implemented in the current session on `codex/service-discovery`.

- `cargo test --workspace`: 146 passed, 0 failed, 3 previously ignored tests.
- `cargo clippy --workspace --all-targets`: succeeded; existing warnings remain.
- `rustfmt --check --edition 2021` on all changed/new Rust files: passed.
- `cargo fmt --all -- --check`: existing formatting differences in 32 untouched files; baseline 8cdba27 also fails (39 files). No new formatting failures.
- Go fixture generation: `GOCACHE=/tmp/tarsrust-go-cache GOPROXY=off GOWORK=off go run <repo>/tars-registry/tests/fixtures/queryf.go` from a temporary module replacing TarsGo with the local checkout; succeeded. Both generated response bodies are consumed by QueryF integration tests.
- Independent review found shutdown cancellation and delayed transport closure. Both were reproduced by failing tests and fixed; pending response release during removal was additionally covered and corrected.
- No live Tars platform was available; real platform publishing/restarting is not claimed as verified.

Implementation decisions: isolate on a feature branch in the current checkout; keep disk cache identity object/Locator/SET (stable across restarts, matching Go); use a monotonic health clock and independently owned shutdown operation. Cache records use Rust-specific versioned TOML as specified. Removing an endpoint can terminate its in-flight RPCs with ConnectionClosed, after which callers can retry against refreshed nodes.
