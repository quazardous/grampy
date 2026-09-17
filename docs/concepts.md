# grampy's notions, under the names you may know

grampy borrows most of its mechanisms from job queues and workflow engines
that came before it. This page maps each notion to the closest equivalent in
Graphile Worker, BullMQ, Hatchet, Inngest, Temporal and Oban, so that a
reader coming from one of them finds their bearings.

How to read a cell: the tool's own term, **—** when it has none, *partial:*
when it comes close with a real difference, and **?** when its documentation
did not settle the question. *Pro* marks a paid edition. Cells cite the page that supports them (see
[Sources](#sources)); "in code" cells without a number describe what any
general-purpose workflow code can do, not a feature. The table was
checked against each tool's documentation on 2026-09-17 and will drift as
they evolve.

The differences that matter most:

- **Events received before the wait began.** grampy records a signal
  durably and a wait settles on it even if it arrived first. Hatchet
  (`lookback_window`), Oban Pro and Temporal can do the same; Inngest's
  `step.waitForEvent` does not see events sent before it starts [55].
- **Keeping the first place when a newer version arrives** (grampy's
  `Lane.throttle`) exists as such in Graphile Worker (`preserve_run_at`) and
  Oban (`replace` on scheduled jobs). BullMQ's `replace` gives the new job its
  own delay [26].
- **Several rate bands at once** (100 a minute *and* 1000 an hour) are rare:
  Hatchet takes several rate limits per task; Inngest distinguishes a
  throttle that queues from a rate limit that drops, one band each [45][46].
- **Any storage.** The tools below each come with their own backend
  (PostgreSQL, Redis, a server); grampy's rule runs in Python over a small
  driver contract — memory, SQLite and PostgreSQL today.

## Taking and finishing work

| grampy | Graphile Worker | BullMQ | Hatchet | Inngest | Temporal | Oban |
|---|---|---|---|---|---|---|
| **claim** a batch, a **lease token** proving ownership (`journal.claim`, `Lease.token`) | *partial:* jobs locked per worker id, no per-claim token [4][5]; `localQueue` locks a batch [6] | lock `token` (`getNextJob(token)`, `extendLock`) [12]; batches: *Pro* `batch` [19] | *partial:* pushed to worker slots, no token exposed [31][32] | — (Inngest calls your function) [51] | *partial:* Task Token per activity; rejection of a stale report: ? [61] | ? (`attempted_by` only) [75] |
| **retry** with constant, linear or exponential backoff, max delay, jitter (`Retry`) | *partial:* `maxAttempts`, fixed exponential curve [2][3] | `attempts` + `backoff {type, delay, jitter}`, no max delay [13][24] | `retries`, `backoff_factor`, `backoff_max_seconds` [33] | `retries`, fixed exponential with jitter [52][54] | Retry Policy: initial interval, coefficient, maximum interval and attempts [62] | `max_attempts` + `backoff/1` [72] |
| per-node **lease**, released by `journal.expire` (visibility timeout) | lock kept 4 h, swept every 8–10 min [4]; *Pro* heartbeats [8] | `lockDuration` + stalled jobs check [12][14] | heartbeats, `execution_timeout` [34][35] | *partial:* `timeouts.start` / `finish` cancel the run [54] | Start-To-Close + Heartbeat timeouts [63] | `Oban.Lifeline` `rescue_after` [76] |

## Shaping the graph

| grampy | Graphile Worker | BullMQ | Hatchet | Inngest | Temporal | Oban |
|---|---|---|---|---|---|---|
| **wait** for a named event, **timeout** (`Node(wait=…, timeout=…)`, `journal.signal`) | — | — [22] | `aio_wait_for_event` with `lookback_window` [36] | `step.waitForEvent`, earlier events not seen [55] | signal + `wait_condition(…, timeout)` [64] | *Pro:* `await_signal/1`, signals kept [78] |
| optional node skipped after a **grace** delay | — | — | *partial:* `SleepCondition` + `skip_if` [37][38] | *partial:* in code [55] | *partial:* in code [64] | *partial, Pro:* `deadline` + `ignore_cancelled` [78][74] |
| exclusive **choice** (`Node(choice=True)`, `branch=`) | — | — [22] | `skip_if=[ParentCondition]` [37] | *partial:* if/else in code [56] | *partial:* if/else in code | — [74] |
| **k-of-n join**, a node run on a parent's **failure** (`need=`, `on=`) | — | *partial:* all children required; `ignoreDependencyOnFailure`, `continueParentOnFailure` [15][16] | *partial:* all parents required; `on_failure_task` [37][39] | *partial:* `Promise.all` / `race`, `onFailure` [57][58] | *partial:* in code | *partial, Pro:* `ignore_*`, compensation, no k-of-n [74] |
| bounded **loop** back to an earlier node (`Loop`) | — | *partial:* step jobs by hand [22] | — (acyclic) [37] | *partial:* a loop in code [56] | *partial:* a loop in code | — [74] |

## Subjects that come back — lanes

| grampy | Graphile Worker | BullMQ | Hatchet | Inngest | Temporal | Oban |
|---|---|---|---|---|---|---|
| `Lane.throttle`: latest version, **place of the first** | `jobKeyMode: 'preserve_run_at'` [1] | *partial:* `deduplication {id, ttl, replace}`, new delay [18][26] | — | — [45] | — [65] | `unique` + `replace: [scheduled: [:args]]` [73] |
| `Lane.debounce`: latest version, quiet `delay`, capped by `max_wait` | *partial:* `jobKeyMode: 'replace'`, no max wait [1] | debounce mode `{extend, replace}` + `delay`, no max wait [18] | *partial:* `CANCEL_QUEUED_EXCEPT_NEWEST` [40] | `debounce {period, key, timeout}` [47] | *partial:* Signal-With-Start + timer [66] | *partial:* `unique` + `replace` of `scheduled_at` [73] |
| `Lane.dedupe`: first version, later ones dropped | `jobKeyMode: 'unsafe_dedupe'` [1] | simple mode `deduplication {id}` [18] | `CANCEL_QUEUED_EXCEPT_OLDEST` [40] | *partial:* `idempotency` key, 24 h [49] | ID Conflict Policy [65] | `unique` [73] |
| `while_running="queue"` / `"skip"` | *partial:* same `queueName` runs serially [1][10] | `keepLastIfActive` / simple mode [18] | `GROUP_ROUND_ROBIN` / `CANCEL_NEWEST` [40] | `singleton` skip; queue: *partial*, `concurrency` 1 per key [48][50] | Use Existing / Fail; queue only for Schedules [65][67] | *Pro:* `chain` / `unique` states [78][73] |
| **cooldown** after the previous pass ended | — [1] | *partial:* throttle `ttl` from insertion [18] | *partial:* TTL idempotency from acceptance [41] | *partial:* `rateLimit` 1 per period per key [46] | — [65] | *partial:* `unique` `period` over finished states [73] |

## Protecting a resource

| grampy | Graphile Worker | BullMQ | Hatchet | Inngest | Temporal | Oban |
|---|---|---|---|---|---|---|
| **rate** bands with burst, GCRA, several at once, queued (`Node(rate=(Rate(…), …))`) | — (`forbiddenFlags` to build one) [9] | *partial:* `limiter {max, duration}`, one band [27]; *Pro* per group [21] | several `RateLimit` per task, re-queued [42] | `throttle {limit, period, burst, key}` GCRA, queued; `rateLimit` drops [45][46] | *partial:* one rate per level [68][69] | *partial, Pro:* `rate_limit` per queue, algorithm to choose [79][80] |
| **concurrency** cap, per channel (`concurrency=`, `per="channel"`) | *partial:* `concurrentJobs` per worker [6] | `setGlobalConcurrency`; per key *Pro* groups [28][20] | `ConcurrencyExpression` per task [40] | `concurrency {limit, key, scope}` [50] | *partial:* per worker; fairness keys [68][69] | queue `limit`; *Pro* `global_limit` with partition [79] |
| **channel** settings: retry, lease, rate, lane per source (`Graph(channels=…)`) | *partial:* options per job [2] | *partial, Pro:* `setGroupConcurrency` [29] | *partial:* keys split limits, same values [40][42] | *partial:* `key` per tenant, same limit [60] | *partial:* fairness weight per key [69] | *partial:* options per job; *Pro* runtime limits [72][81] |

## Changing the workflow

| grampy | Graphile Worker | BullMQ | Hatchet | Inngest | Temporal | Oban |
|---|---|---|---|---|---|---|
| subjects **pinned** to their graph version, **migrate** all or nothing | — [11] | ? | *partial:* new task, old runs drain [43] | *partial:* new code over memoized steps [59] | Worker Versioning: Pinned / Auto-Upgrade; patching [70][71] | — [74] |

## Sources

1. https://worker.graphile.org/docs/job-key
2. https://worker.graphile.org/docs/library/add-job
3. https://worker.graphile.org/docs/exponential-backoff
4. https://worker.graphile.org/docs/error-handling
5. https://worker.graphile.org/docs/admin-functions
6. https://worker.graphile.org/docs/config
7. https://worker.graphile.org/docs/tasks
8. https://worker.graphile.org/docs/pro/recovery
9. https://worker.graphile.org/docs/forbidden-flags
10. https://worker.graphile.org/docs/sql-add-job
11. https://worker.graphile.org/docs/pro/migration
12. https://docs.bullmq.io/patterns/manually-fetching-jobs
13. https://docs.bullmq.io/guide/retrying-failing-jobs
14. https://docs.bullmq.io/guide/workers/stalled-jobs
15. https://docs.bullmq.io/guide/flows/continue-parent
16. https://docs.bullmq.io/guide/flows/ignore-dependency
17. https://docs.bullmq.io/guide/flows/fail-parent
18. https://docs.bullmq.io/guide/jobs/deduplication
19. https://docs.bullmq.io/bullmq-pro/batches
20. https://docs.bullmq.io/bullmq-pro/groups/concurrency
21. https://docs.bullmq.io/bullmq-pro/groups/rate-limiting
22. https://docs.bullmq.io/patterns/process-step-jobs
23. https://github.com/taskforcesh/bullmq/blob/master/src/interfaces/worker-options.ts
24. https://github.com/taskforcesh/bullmq/blob/master/src/interfaces/backoff-options.ts
25. https://github.com/taskforcesh/bullmq/blob/master/src/types/job-options.ts
26. https://github.com/taskforcesh/bullmq/blob/master/src/commands/includes/deduplicateJob.lua
27. https://docs.bullmq.io/guide/rate-limiting
28. https://docs.bullmq.io/guide/queues/global-concurrency
29. https://docs.bullmq.io/bullmq-pro/groups/local-group-concurrency
30. https://docs.bullmq.io/bullmq-pro/groups/max-group-size
31. https://docs.hatchet.run/v1/workers
32. https://docs.hatchet.run/v1/architecture-and-guarantees
33. https://docs.hatchet.run/v1/retry-policies
34. https://docs.hatchet.run/v1/faq
35. https://docs.hatchet.run/v1/timeouts
36. https://docs.hatchet.run/v1/durable-event-waits
37. https://docs.hatchet.run/v1/directed-acyclic-graphs
38. https://github.com/hatchet-dev/hatchet/blob/main/examples/python/conditions/worker.py
39. https://docs.hatchet.run/reference/python/runnables
40. https://docs.hatchet.run/v1/concurrency
41. https://docs.hatchet.run/v1/idempotency
42. https://docs.hatchet.run/v1/rate-limits
43. https://docs.hatchet.run/v1/from-temporal-to-hatchet
44. https://docs.hatchet.run/home/durable-best-practices
45. https://www.inngest.com/docs/guides/throttling
46. https://www.inngest.com/docs/guides/rate-limiting
47. https://www.inngest.com/docs/guides/debounce
48. https://www.inngest.com/docs/guides/singleton
49. https://www.inngest.com/docs/guides/handling-idempotency
50. https://www.inngest.com/docs/guides/concurrency
51. https://www.inngest.com/docs/setup/connect
52. https://www.inngest.com/docs/features/inngest-functions/error-retries/retries
53. https://www.inngest.com/docs/reference/typescript/functions/errors
54. https://www.inngest.com/docs/reference/functions/create
55. https://www.inngest.com/docs/features/inngest-functions/steps-workflows/wait-for-event
56. https://www.inngest.com/docs/learn/inngest-steps
57. https://www.inngest.com/docs/guides/step-parallelism
58. https://www.inngest.com/docs/reference/functions/handling-failures
59. https://www.inngest.com/docs/learn/versioning
60. https://www.inngest.com/docs/guides/multi-tenancy
61. https://docs.temporal.io/activity-execution
62. https://docs.temporal.io/encyclopedia/retry-policies
63. https://docs.temporal.io/encyclopedia/detecting-activity-failures
64. https://docs.temporal.io/develop/python/message-passing
65. https://docs.temporal.io/workflow-execution/workflowid-runid
66. https://docs.temporal.io/sending-messages
67. https://docs.temporal.io/schedule
68. https://docs.temporal.io/develop/worker-performance
69. https://docs.temporal.io/develop/task-queue-priority-fairness
70. https://docs.temporal.io/worker-versioning
71. https://docs.temporal.io/patching
72. https://oban.hexdocs.pm/Oban.Worker.html
73. https://oban.hexdocs.pm/unique_jobs.html
74. https://oban.pro/docs/pro/Oban.Pro.Workflow.html
75. https://oban.hexdocs.pm/Oban.Job.html
76. https://oban.hexdocs.pm/Oban.Lifeline.html
77. https://oban.pro/docs/pro/Oban.Pro.Lifeline.html
78. https://oban.pro/docs/pro/Oban.Pro.Worker.html
79. https://oban.pro/docs/pro/Oban.Pro.Engine.html
80. https://oban.pro/docs/pro/Oban.Pro.RateLimit.html
81. https://oban.pro/docs/pro/Oban.Pro.Queues.html
