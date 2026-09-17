# Items: speak objects, not ids

The canonical way to use grampy. The core works on ids and knows nothing
about your data; this layer holds the handlers that read your own objects
and turns their answers into calls the core already understands.

`quazardous.grampy.items` is optional and standard-library only. The core
never imports it, and never calls a handler.

- [The adapter](#the-adapter)
- [One workflow, several kinds of subject](#one-workflow-several-kinds-of-subject)
- [Where the line is](#where-the-line-is)
- [The whole surface](#the-whole-surface)

## The adapter

One method is yours to write — `id_of` — and it takes a candidate, which
may already be an id:

```python
def id_of(self, candidate):
    return getattr(candidate, "id", candidate)
```

`inflate` is handed the batch exactly as the caller passed it and gives
items back. The default lets everything through, so an application that
passes its own objects — or that works in ids and writes only the `id_of`
above — adds nothing. Override it and **you** decide what is an id and what
is already an item; the layer never guesses.

The handlers after that are asked about the **data**, so they only ever see
what `inflate` turned into an item. The rest have answers that suit an
application with nothing special to say.

```python
from quazardous.grampy.items import Adapter, Items

class Bricks(Adapter):
    def id_of(self, brick):      return brick.id
    def inflate(self, ids):         return Brick.objects.filter(id__in=list(ids))

    def policy_of(self, brick): return brick.crate          # its operating policy
    def ref_of(self, brick):     return brick.content_hash   # its version, in a lane
    def branch(self, brick, node):
        return "quarantine" if brick.tnt else "sort"
    def applies(self, brick, node):
        return node != "polish" or brick.crate == "factory"

items = Items(journal, Bricks())
```

Then the application stops handling ids:

```python
items.admit(new_bricks)                # policy read off each brick, once
items.arrive("inbox", new_bricks)      # ref too

lease = items.claim("sort", 10, candidates=my_loader())   # BRICKS in…
for brick in lease:                                       # …and bricks out
    ...                                # your work, on your own object
items.conclude("sort", lease)          # the token travels with the lease
```

**Nothing here asks you to hold ids.** Candidates are your objects too —
any iterable of them, list, tuple or generator — and their ids are read with
`id_of`. They are reused as they are, so a batch you already loaded is never
loaded twice.

**Ids work everywhere items do**, and the two may be mixed in one call,
because the batch goes to `inflate` as it came:

```python
def inflate(self, candidates):
    thin = [c for c in candidates if isinstance(c, int)]
    fat = {b.id: b for b in Brick.objects.filter(id__in=thin)}
    return [fat.get(c, c) for c in candidates]
```

Your objects pass through untouched — not fetched twice — and only what
needed fetching is fetched. What counts as an id is your rule, not a guess
the library makes on your behalf.

A **driver's query** is handed to the journal untouched. That is the one
place ids are unavoidable: the storage produces the candidate set, and it
has no Python objects to give. The layer then loads the lease in **one**
call to `load`, and ids nothing loaded for — a row deleted meanwhile — come
back as `lease.missing` rather than disappearing quietly, the rest of the
lease still concluding.

What a query buys, and it is not small: the **PostgreSQL** driver filters
and pages *in the database*, so it never ships candidates it will not use.
Claiming 10 from a backlog of 100,000 rows, half of them already done,
brings back **210 rows** — the rest is never sent. A list cannot do that:
you have to build it first.

The SQLite driver reads its candidate set at once, on purpose — its cursor
cannot stay open while the same connection writes the claim — so there the
query saves the loading, not the reading.

`conclude` takes `token=` when the lease did not travel with the work: a
worker that took its job off a queue and holds only the proof.

## One workflow, several kinds of subject

This is what the layer is really for. One graph serves subjects that differ
slightly, and the difference is stated **once**, in the adapter, instead of
in every worker that touches them.

`applies(item, node)` answers for an **optional** node. When it says no, the
claim gives that node up — concluded `skipped` — and leaves the item out of
the lease:

```python
lease = items.claim("polish", 10, candidates=waiting)
# factory bricks come back to be polished; salvage bricks are already
# recorded as skipped, and `pack` no longer waits for them.
```

The journal records that the decision was taken, rather than the step
silently never happening. That costs a claim on a step not done: a hot path
may prefer to leave those subjects out of `candidates` in the first place —
which subjects a worker offers has always been
[your sentence](drivers.md#candidates-are-your-sentence) — and keep a
policy `grace` as the safety net for the ones nobody takes.

`branch(item, node)` does the same for a `choice`: instead of working out
the branch by hand at every call site, the handler reads the item and names
it. Items taking different branches may be concluded in one call.

## Where the line is

**The core takes decisions, never criteria.** It is given an id, a label,
the name of a branch — never a rule to evaluate, never a payload to look
inside. That is what keeps a workflow from learning what "source" means:
the day `"partner-a"` becomes "partners whose contract includes
enrichment, except during sales", you change a function on your side and
grampy does not notice.

So the layer translates, and stops there. No retry loop, no logging, no
worker lifecycle. Everything it does is a call you could have written by
hand against the id-based API — which stays exactly as it is, and stays the
right choice when you already hold ids and no objects.

## The whole surface

| | |
|---|---|
| `admit(items)` | record each item's policy, once |
| `skip(node, candidates=…)` · `settle(candidates)` | the janitor, in items' terms |
| `arrive(node, items, urgent=False)` | a lane, each item bringing its `ref_of` |
| `claim(node, limit, candidates=…)` | items in (reused) or a driver query; an `ItemLease` out |
| `conclude(node, items, token=None, status=…)` | asks `branch` for a choice |
| `fail(node, items, token=None)` | |
| `signal(items, event)` · `progress(item)` · `history(item)` | in items' terms |

Everything else — `settle`, `expire`, `migrate`, the counts — stays on the
journal, which the layer holds as `items.journal`.

Back to the [documentation index](README.md).
