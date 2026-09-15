# Restrict diagnostic SQL to read-only statements

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while implementing the change.

Reference: `PLANS.md` at the repository root.

## Purpose / Big Picture

Diagnostic TiDB is used to build plans for sampled customer SQL and inspect metadata. A client connected to a diagnostic-mode TiDB can submit non-executing `EXPLAIN SELECT` (including pure set operations), `USE`, and an explicit allowlist of read-only `SHOW` metadata/status statements. DML, DDL, transaction control, administrative SQL outside that allowlist, prepared execution, locking reads, `EXPLAIN ANALYZE`, and SELECT forms with known side effects are rejected before transaction preparation or plan execution.

## Progress

- [x] (2026-09-15) Read the canary design/audit documents and repository guidance.
- [x] (2026-09-15) Locate the user SQL execution entry point and confirm that internal restricted SQL must bypass the user-facing allowlist.
- [x] (2026-09-15) Add the diagnostic SQL predicate and enforce it before transaction preparation.
- [x] (2026-09-15) Add focused tests for allowed and rejected statements, including metadata SHOW commands, prepared execution, and side-effecting SELECT forms.
- [x] (2026-09-15) Run targeted validation, `make bazel_prepare` if required, and Ready-profile lint.
- [x] (2026-09-15) Review the final diff and document residual risks.

## Surprises & Discoveries

- Diagnostic startup and internal metadata/statistics readers use sessions with `InRestrictedSQL=true`. A guard that checked only the process-wide diagnostic flag would block startup and internal SQL. The user-facing server session has `InRestrictedSQL=false`; internal SQL and `ExecuteInternal` set it to true.
- `ast.IsReadOnly` intentionally treats some statements such as `SHOW` and `DO` as read-only, so it cannot implement the requested allowlist. It also allows a non-ANALYZE Explain based on the child statement without enforcing the narrower SELECT-only contract.
- Prepared protocol execution enters `session.ExecuteStmt` with an `ast.ExecuteStmt`, while `PrepareStmt` prepares a transaction before compiling. The execution guard rejects binary execution and the separate `PrepareStmt` guard rejects the request before that transaction setup.
- `PrepareStmt` performs `PrepareTxnCtx` before compiling the SQL. It therefore needs a separate early rejection in diagnostic mode; otherwise a rejected prepared statement could still allocate transaction state.
- `SET_VAR` hints live in `SelectStmtOpts.TableHints`, which are not traversed by `ast.Walk`; the predicate checks both optimizer-hint fields explicitly.
- `ShowStmt` covers both harmless metadata inspection and specialized cluster/statistics/status commands. An explicit type allowlist is safer than treating every parser `SHOW` type as read-only; unknown or unreviewed types remain rejected.

## Decision Log

- Decision: Enforce a strict allowlist at `session.executeStmtImpl`, before `PrepareTxnCtx`, only for non-internal sessions when diagnostic mode is enabled.
  Rationale: This prevents transaction/session side effects before rejection, covers COM_QUERY and prepared execution, and preserves internal restricted SQL used by diagnostic startup and metadata readers.
  Date/Author: 2026-09-15 / Codex.
- Decision: Allow direct `UseStmt` and an explicit list of metadata/status `ShowStmt` types in addition to `ExplainStmt` with `Analyze=false` whose child is `SelectStmt` or `SetOprStmt`; reject SELECT locking, SELECT INTO, known side-effect functions (including session/sequence state changes), assignment expressions, and SET_VAR hints.
  Rationale: `USE`, `SHOW TABLES`, `SHOW DATABASES`, and `SHOW CREATE TABLE` are required diagnostic inspection operations, while the explicit list prevents unreviewed `SHOW` forms from becoming available by default.
  Date/Author: 2026-09-15 / Codex.
- Decision: Reuse `plannererrors.ErrSQLInReadOnlyMode` for the rejection error rather than adding a new public error code.
  Rationale: It is already the repository's read-only execution error and avoids changing error-code compatibility for a diagnostic-only restriction.
  Date/Author: 2026-09-15 / Codex.
- Decision: Reject user `COM_STMT_PREPARE` requests before transaction preparation, including prepared `EXPLAIN SELECT`.
  Rationale: The diagnostic endpoint accepts direct `EXPLAIN SELECT` requests only; prepared protocol requests are a separate request class and otherwise allocate transaction state before the execution guard can run.
  Date/Author: 2026-09-15 / Codex.

## Outcomes & Retrospective

Implemented the allowlist at the session execution boundary and added a pre-transaction guard for binary protocol prepare. Focused parser/execution tests cover the allowlist, `USE`, read-only metadata `SHOW` forms, side-effecting SELECT forms, rejected DML/DDL/transaction statements, prepared requests, internal restricted SQL, and normal-mode compatibility. `make bazel_prepare`, the failpoint-aware session test, and `make lint` completed successfully after the allowlist expansion.

The guard is intentionally scoped to user sessions. It does not replace the existing PD/TiKV diagnostic read-path protections, and it does not attempt to classify arbitrary future extension functions as pure; only known side-effecting functions are explicitly rejected inside an otherwise valid `EXPLAIN SELECT` or filtered `SHOW` statement.

## Context and Orientation

User SQL reaches `pkg/session/session.go` through `(*session).ExecuteStmt`, then `executeStmtImpl`. That function currently prepares transaction context, resets statement context, validates transaction/staleness constraints, compiles the AST, and executes the physical plan. The new check must run before transaction preparation so a rejected statement cannot allocate a transaction or mutate session transaction state.

`pkg/config/diagnosticmode` exposes the process-wide diagnostic flag. Internal sessions are deliberately marked with `SessionVars.InRestrictedSQL`; they execute metadata and maintenance reads during startup and must not be blocked by the user SQL rule. `pkg/parser/ast` provides `ExplainStmt`, `SelectStmt`, `SetOprStmt`, lock metadata, SELECT INTO metadata, function-call AST nodes, and optimizer hint nodes.

## Plan of Work

Add a small predicate in the session package, keeping the policy close to the execution boundary. It will return true for a non-ANALYZE `ExplainStmt` whose child is a `SelectStmt` or `SetOprStmt`, direct `UseStmt`, and explicitly reviewed metadata/status `ShowStmt` types. It recursively inspects set-operation children and expressions in SELECT/SHOW nodes. Reject lock information, SELECT INTO, `VariableExpr` assignments, `SET_VAR` hints, and known side-effect function names (`get_lock`, `release_lock`, `release_all_locks`, `nextval`, `lastval`, `setval`, `sleep`, and `setvar`). Unknown statement types are rejected by default.

Call this predicate at the top of `executeStmtImpl` when `diagnosticmode.Enabled()` and `!s.sessionVars.InRestrictedSQL`; return `plannererrors.ErrSQLInReadOnlyMode` on failure. Keep ordinary mode and internal restricted execution unchanged.

Add tests in a session-focused test file. Parse representative statements and exercise `ExecuteStmt`/`PrepareStmt` where practical. Verify `EXPLAIN SELECT 1` is accepted, while DML, transaction, DDL, SHOW, PREPARE/EXECUTE, `EXPLAIN ANALYZE`, `EXPLAIN UPDATE`, locking SELECT, SELECT INTO, and side-effect function forms are rejected. Verify the same statement remains executable when diagnostic mode is disabled, and that internal restricted SQL bypasses the user allowlist.

## Concrete Steps

Run from `/home/xzx/canary/tidb`:

    gofmt -w pkg/session/diagnostic_sql.go pkg/session/diagnostic_sql_test.go pkg/session/session.go
    go test -tags=intest,deadlock ./pkg/session -run 'TestDiagnosticSQL' -count=1 -timeout=5m
    git diff --check

Because this change adds Go source files and changes imports in `pkg/session/session.go`, run:

    make bazel_prepare

Then run the Ready profile checks required by `AGENTS.md`:

    ./tools/check/failpoint-go-test.sh . ./pkg/session -run 'TestDiagnosticSQL' -count=1 -timeout=5m
    make lint

## Validation and Acceptance

The focused tests must show:

1. Diagnostic mode accepts `EXPLAIN SELECT 1`, a pure `EXPLAIN SELECT ... UNION SELECT ...`, `USE`, and reviewed metadata SHOW statements.
2. Diagnostic mode rejects every non-allowlisted statement and all Explain children other than Select/set operations.
3. Diagnostic mode rejects `EXPLAIN ANALYZE`, lock clauses, SELECT INTO, assignment expressions, SET_VAR hints, and known side-effect functions.
4. Internal restricted SQL is not rejected solely because the process is in diagnostic mode.
5. Normal mode behavior is unchanged.

## Idempotence and Recovery

The predicate and tests are additive and safe to rerun. If a test changes a process-wide diagnostic flag, it must restore the prior value with `t.Cleanup`. If `make bazel_prepare` changes unrelated generated metadata, inspect and revert only unrelated generated changes before finishing. Revert the new guard and tests together if validation exposes an internal restricted-SQL path that was not covered by the bypass condition.

## Artifacts and Notes

The canary source documents are outside the repository in `/home/xzx/tmp/canary`. The design requirement is also summarized in `diagnostic-pd-tikv-read-rpc-audit.md`: SQL filtering does not replace PD/TiKV write guards, and ordinary reads can trigger lock-resolution writes or TSO allocation. This change therefore addresses only the user SQL entry point; existing client and background-task protections remain required.

## Interfaces and Dependencies

The implementation will use:

- `pkg/config/diagnosticmode.Enabled` to select diagnostic behavior.
- `(*session).sessionVars.InRestrictedSQL` to distinguish internal SQL from user SQL.
- `pkg/parser/ast` node types and `ast.Walk` to inspect the statement tree.
- `plannererrors.ErrSQLInReadOnlyMode` as the rejection error.

No new external dependency or SQL protocol interface is required.
