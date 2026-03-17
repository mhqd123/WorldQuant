# WorldQuant BRAIN Research Business Logic

## Core objective

The objective is **successful submission on the WorldQuant BRAIN platform**, not raw simulation count, not local git activity, and not near-neighbor score chasing.

## Current operating logic

1. Generate candidates from research hypotheses, not arbitrary formula drift.
2. Simulate candidates and capture full structured results.
3. Diagnose outcomes using failure taxonomy.
4. Route each result into reject / improve / submit / archive pools.
5. Use submission status write-back to learn which candidates truly convert on-platform.
6. Optimize for low-coupling, low-self-correlation, high-conversion families.

## Critical rule: successful template freeze

Once a template has been confirmed as successfully submitted on-platform, that template's family must be treated as **frozen**.

This means the successful template may be used only as:
- an anchor
- an exclusion reference
- a success-pattern reference

It must **not** be used as:
- a mutation parent
- a sibling-expansion parent
- a near-neighbor parameter-tuning base

## Why this rule exists

We observed that even seemingly different siblings around a successful template can still fail due to extreme self-correlation. Small changes in:
- volume confirmation windows
- neutralization level
- smoothing windows
- wrapper transforms

are often still interpreted by the platform as the same crowded family.

Therefore, once a family yields a successful submitted alpha, continuing to generate siblings around the same backbone is usually a poor use of simulation and submission budget.

## Mandatory anti-repeat constraint

To avoid repeating this mistake, any new candidate must be rejected or heavily downranked if it remains too close to a successful anchor family.

In practice, if a candidate preserves the same core backbone and only alters:
- parameter windows
- risk wrappers
- volume confirmation wrappers
- group level

then it should be treated as an anchor-neighbor and not prioritized.

## New policy after a success

After a successful template is found:
1. Freeze the successful family.
2. Do not generate siblings from that family.
3. Use the success only to extract abstract principles.
4. Redirect research budget toward discovering a second independent family.

## What counts as a valid next family

A valid next family should change at least the major mechanism level, such as:
- a different price backbone
- a different confirmation mechanism
- a different structural comparison logic

The system should prefer finding a second independent submit-capable family over harvesting near-neighbor variants of the first successful family.

## Operational summary

- Successful template = anchor, not parent.
- Success pattern can inform future search, but successful expression families are frozen.
- The next target after one success is a second independent family, not more siblings of the first.
