"""Generate the org-onboarding runbook page from the plan's §3 tables.

The plan is the source: edit docs/plans/org-onboarding-from-the-invite.md
(the phase table and the item table under §3), run this, then republish the
output to the runbook artifact with the Artifact tool (url set, capabilities
omitted so the db declaration carries forward).

  python3 scripts/docs/gen_onboarding_runbook.py [out.html]
"""
import html
import json
import os
import re
import sys

PLAN = 'docs/plans/org-onboarding-from-the-invite.md'
HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(HERE, 'onboarding_runbook.template.html')
OUT = sys.argv[1] if len(sys.argv) > 1 else 'onboarding-runbook.html'

s = open(PLAN, encoding='utf-8').read()
sec = s[s.index('## 3. What this actually requires building'):s.index('## 4. Not decided here')]

# Phase table.
phases = []
for m in re.finditer(r'^\| (A|B|C|D|E|F|G|—) \| (.+?) \| (.+?) \|$', sec, re.M):
    key, what, items = m.groups()
    phases.append({'key': key, 'what': what.strip(), 'items': [int(x) for x in re.findall(r'\d+', items)]})
assert len(phases) == 8, phases

# Item rows.
items = {}
for m in re.finditer(r'^\| (\d+) \| (.+?) \| (.+?) \|$', sec, re.M):
    n, piece, deps = m.groups()
    n = int(n)
    if n in items:
        continue
    items[n] = {'n': n, 'piece': piece.strip(), 'deps': deps.strip()}
# No fixed count: the table grows as items are filed (it was pinned at 49
# and went stale at item 56). The phase checks below are the real guard —
# every row must sit in exactly one phase.
assert len(items) >= 49, len(items)

# Every item is in exactly one phase.
seen = {}
for ph in phases:
    for n in ph['items']:
        assert n in items, n
        assert n not in seen, (n, seen.get(n), ph['key'])
        seen[n] = ph['key']
assert set(seen) == set(items), set(items) - set(seen)


def md(t):
    """Minimal markdown → HTML for a table cell."""
    t = html.escape(t, quote=False)
    t = re.sub(r'~~(.+?)~~', r'<s>\1</s>', t)
    t = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', t)
    t = re.sub(r'`([^`]+)`', r'<code>\1</code>', t)
    t = re.sub(r'\*(.+?)\*', r'<em>\1</em>', t)
    return t


def deps_html(d):
    if d.strip() in ('—', ''):
        return ''
    nums = re.findall(r'\d+', d)
    rest = re.sub(r'[\d,\s]+', '', d).strip()
    chips = ''.join(f'<span class="dep" data-dep="{x}">{x}</span>' for x in nums)
    if rest:
        chips += f'<span class="dep txt">{html.escape(d if not nums else rest)}</span>'
    return chips


def row(it, active):
    n = it['n']
    # Computed before the row is built (CodeRabbit round 1, #1005,
    # Functional Correctness): a backslash-containing f-string expression
    # (the escaped-quote `deps_html(...)` call below) fails to PARSE at all
    # on Python < 3.12 ("f-string expression part cannot include a
    # backslash") — a hard SyntaxError, not a runtime one. Computing `deps`/
    # `meta` as plain statements first also drops the double `deps_html`
    # call the inline conditional made.
    deps = deps_html(it['deps'])
    meta = f'<span class="lbl">after</span>{deps}' if deps else ''
    # `disabled` (CodeRabbit round 1, #1005): the click handler is wired up
    # only inside `window.claude.use("db").then(...)` in the HTML template
    # this generates into, which also removes `disabled` once the store
    # resolves. Rendering the control already-enabled means a reviewer can
    # click it before that promise resolves — or permanently, if
    # `window.claude` is absent — with no visible cue that nothing happened.
    # `type="button"` alongside it: a bare `<button>` inside any future
    # `<form>` defaults to `type="submit"`, which this control must never be.
    ctl = (f'<button class="st" type="button" data-item="{n}" data-status="todo" disabled '
           f'aria-label="status of item {n}">'
           f'<i class="dot"></i><span class="t">to do</span></button>') if active else \
          '<span class="st struck"><i class="dot"></i><span class="t">struck</span></span>'
    note = (f'<button class="addnote" data-addnote="{n}" hidden>+ note</button>'
            f'<div class="notewrap" data-notewrap="{n}"><div class="notehead"><span class="lbl">Note</span>'
            f'<span class="status" data-status-msg="{n}"></span></div>'
            f'<textarea class="note-in" data-note="{n}" rows="1" placeholder="PR number, blocker, what changed…"></textarea></div>')
    return (f'<div class="item{"" if active else " retired"}" data-row="{n}">'
            f'<span class="num">{n}</span>'
            f'<div class="body"><p class="piece">{md(it["piece"])}</p>'
            f'<div class="meta">{meta}</div>'
            f'{note}</div>{ctl}</div>')


sections = []
for ph in phases:
    active = ph['key'] != '—'
    rows = ''.join(row(items[n], active) for n in ph['items'])
    label = f'Phase {ph["key"]}' if active else 'Struck or not now'
    sections.append(
        f'<section data-phase="{ph["key"]}"><div class="shead"><span class="lbl">{label} · <span class="pc" data-pc="{ph["key"]}">{len(ph["items"])} items</span></span>'
        f'<h2>{md(ph["what"])}</h2></div><div class="items">{rows}</div></section>')

active_total = sum(len(p['items']) for p in phases if p['key'] != '—')
active_ids = [n for p in phases if p['key'] != '—' for n in p['items']]

page = open(TEMPLATE, encoding='utf-8').read()
page = page.replace('{{SECTIONS}}', ''.join(sections))
page = page.replace('{{TOTAL}}', str(active_total))
page = page.replace('{{ACTIVE_IDS}}', json.dumps(active_ids))
open(OUT, 'w', encoding='utf-8').write(page)
print(f'wrote {OUT}: {active_total} active items, {len(items) - active_total} struck')
