# MMA Analytics Operating Model

This document defines how `MMA-analytics` and `mma-grid` work together as one product system with two repositories.

It is written so it can be copied into `David543M/MMA-analytics` with minimal changes.

## Purpose

The operating model exists to keep three things clean:

- where sports data comes from
- where product logic lives
- how changes move safely from ingestion to the user-facing app

The system goal is:

- `MMA-analytics` owns ingestion, normalization, and canonical sports data
- `mma-grid` owns the application, UX, monetization, deployment, and user workflows

## Repository Roles

### `David543M/MMA-analytics`

This repo is the canonical data pipeline.

It owns:

- scraping and source integration
- parsing and normalization
- canonical combat data tables
- validation and repair jobs
- scheduled ingestion workflows
- sports-data schema changes
- ranking snapshots and historical ingestion state

It does not own:

- frontend UX
- Vercel deployment of the app
- billing
- legal acceptance UI
- feature gating

### `David543M/mma-grid`

This repo is the product application.

It owns:

- frontend routes and UI
- auth surfaces
- billing and pricing UI
- legal pages and acceptance flows
- entitlements and paywalls
- Vercel previews and production deployment
- product-specific Supabase tables

It consumes the sports data produced by `MMA-analytics`.

## Source Of Truth

Use these rules consistently.

### Sports data

Source of truth:

- `MMA-analytics`

Examples:

- `fighter_profiles`
- `fighter_ufc_profiles`
- `fighter_ufc_stats`
- `fighter_ufc_advanced_stats`
- `event_overview`
- `event_bouts`
- `ufc_rankings`
- `ufc_ranking_snapshots`

### Product and business state

Source of truth:

- `mma-grid`

Examples:

- `ranking_presets`
- `event_prep_sessions`
- `event_picks`
- `billing_subscriptions`
- `entitlements`
- `legal_acceptances`
- `beta_access_invites`

## Shared Supabase Model

Both repos may interact with the same Supabase project, but ownership must remain explicit.

Rules:

- `MMA-analytics` writes ingestion-owned tables
- `mma-grid` writes product-owned tables
- ingestion tables must be safe for the app to read without hidden frontend repair logic

If a table’s main purpose is “sports data consumed by the product,” it belongs here.

If a table’s main purpose is “user/account/product/business state,” it belongs in `mma-grid`.

## Table Ownership

### Owned by `MMA-analytics`

- `fighter_profiles`
- `fighter_ufc_profiles`
- `fighter_ufc_stats`
- `fighter_ufc_advanced_stats`
- `event_overview`
- `event_bouts`
- `ufc_rankings`
- `ufc_ranking_snapshots`

### Owned by `mma-grid`

- `ranking_presets`
- `event_prep_sessions`
- `event_picks`
- `billing_customers`
- `billing_subscriptions`
- `entitlements`
- `legal_acceptances`
- `beta_access_invites`

## Contract With `mma-grid`

`mma-grid` should be able to treat ingestion tables as stable contracts.

That means `MMA-analytics` should document, for every app-facing table:

- table purpose
- required columns
- optional columns
- nullability assumptions
- freshness expectations
- ranking or ordering semantics
- owner repo

Recommended file:

- `docs/data-contract.md`

If the app needs fallback behavior because a field can be absent, document that explicitly.

## Change Management Rules

### Work belongs in `MMA-analytics` when it changes:

- scraping logic
- source-site parsing
- canonical name matching
- event/fighter linking
- rankings ingestion
- historical snapshots
- data repair logic
- ingestion-owned schema

### Work belongs in `mma-grid` when it changes:

- how data is displayed
- routing or page UX
- account/billing/legal/product state
- entitlements and premium access
- custom ranking builder UX

### If a feature spans both repos

Use this order:

1. define or update the data contract
2. change `MMA-analytics`
3. run ingestion or repair
4. verify Supabase rows
5. change `mma-grid`
6. verify UI against the new data

Never require `mma-grid` to guess a schema that is not yet deployed.

## Branching Model

Keep branching simple.

Recommended:

- `main` for stable work
- `feature/...` for planned ingestion features
- `fix/...` for targeted repairs
- `codex/...` for AI-assisted work if used

For cross-repo changes, prefer mirrored names.

Example:

- `MMA-analytics`: `feature/ufc-rankings-history`
- `mma-grid`: `feature/ufc-rankings-history-ui`

## Deployment Model

### `MMA-analytics`

Deployment model:

- GitHub Actions
- scheduled jobs
- manual repair runs when needed
- writes to Supabase

Release checks:

- parser succeeds
- validators pass
- target tables contain expected row counts and fields
- downstream app contracts remain satisfied

### `mma-grid`

Deployment model:

- Vercel preview
- Vercel production

This repo should not take over ingestion responsibilities.

## Environments

Always verify which Supabase environment this repo is writing to.

Most dangerous failure mode:

- `MMA-analytics` updates one Supabase environment
- `mma-grid` reads another

Before diagnosing the app, verify:

- Supabase project ref
- workflow secrets
- CLI target
- branch or environment target

The app can only show what this repo actually wrote into the environment it reads from.

## Release Workflow

For any app-facing data feature:

1. update the contract
2. implement ingestion or repair
3. run the job
4. validate row counts and sample rows
5. notify or coordinate with `mma-grid`
6. verify the UI in preview or local
7. merge after both layers are green

## Incident Ownership

Use this quick triage rule.

### Start in `MMA-analytics` when:

- rankings are incomplete
- fighters are not linked to bouts
- events are stale
- parsing broke after a source-site change
- row counts are wrong
- snapshots are missing

### Start in `mma-grid` when:

- data exists in Supabase but renders incorrectly
- sort order is wrong in the UI
- compare links do not appear despite valid data
- billing/legal/account behavior is broken

## Quality Bar For App-Facing Tables

Before calling a table “ready for the app,” confirm:

1. row counts are plausible
2. key identifiers are linked where expected
3. required fields are present
4. nulls are intentional
5. ordering fields are correct
6. downstream `mma-grid` screens have been sanity checked

Example:

- `ufc_rankings` is not ready if it only contains champions and not contenders
- `event_bouts` is not ready if fighter IDs are missing despite resolvable names

## Ownership Checklist Before Merge

Before merging a change in `MMA-analytics`, ask:

1. Is this table owned here?
2. Does this change alter the app-facing contract?
3. Does `mma-grid` need a follow-up change?
4. Did I validate the correct Supabase environment?
5. Will the app still know how to read this table safely?

If the answer is unclear, document the change before merging.

## Short Version

If you only remember one thing, remember this:

- `MMA-analytics` decides what the sports data is
- `mma-grid` decides how the product uses it

Keep the boundary clean and both repos will stay much easier to operate.
