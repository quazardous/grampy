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

Two methods are yours to write. The rest have answers that suit an
application with nothing special to say.

```python
from quazardous.grampy.items import Adapter, Items

class Bricks(Adapter):
    def id_of(self, brick):      return brick.id
    def load(self, ids):         return Brick.objects.filter(id__in=list(ids))

    def channel_of(self, brick): return brick.crate          # its source
    def ref_of(self, brick):     return brick.content_hash   # its version, in a lane
    def branch(self, brick, node):
        return "quarantine" if brick.tnt else "sort"
    def applies(self, brick, node):
        return node != "polish" or brick.crate == "factory"

items = Items(journal, Bricks())
```

Then the application stops handling ids:

```python
items.admit(new_bricks)                # channel read off each brick, once
items.arrive("inbox", new_bricks)      # ref too

lease = items.claim("sort", 10, candidates=my_loader())   # BRICKS in…
for brick in lease:                                       # …and bricks out
    ...                                # your work, on your own object
items.conclude("sort", lease)          # the token travels with the lease
```

**Nothing here asks you to hold ids.** Candidates are your objects too: a
list or tuple of them, whose ids are read with `id_of`. They are reused as
they are, so a batch you already loaded is never loaded twice.

Anything else is handed to the journal untouched — a driver's own query,
read *inside* the claim's transaction, which is what keeps a hot path
atomic. Then the layer loads the lease in **one** call to `load`, and ids
nothing loaded for — a row deleted meanwhile — come back as
`lease.missing` rather than disappearing quietly, the rest of the lease
still concluding.

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
channel `grace` as the safety net for the ones nobody takes.

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
| `admit(items)` | record each item's channel, once |
| `arrive(node, items, urgent=False)` | a lane, each item bringing its `ref_of` |
| `claim(node, limit, candidates=…)` | items in (reused) or a driver query; an `ItemLease` out |
| `conclude(node, items, token=None, status=…)` | asks `branch` for a choice |
| `fail(node, items, token=None)` | |
| `signal(items, event)` · `progress(item)` · `history(item)` | in items' terms |

Everything else — `settle`, `expire`, `migrate`, the counts — stays on the
journal, which the layer holds as `items.journal`.

Back to the [documentation index](README.md).
