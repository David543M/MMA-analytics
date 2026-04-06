# War Room Operating Model

This document defines how `mma-grid` and `MMA-analytics` work together as one product system with two repositories.

It is written so it can live in both repos with minimal changes.

## Purpose

The operating model exists to keep three things clean:

- where data comes from
- where product logic lives
- how changes move safely from idea to production

The goal is simple:

- `MMA-analytics` owns sports data ingestion and normalization
- `mma-grid` owns product experience, monetization, deployment, and user workflows

## Repository Roles

### `David543M/MMA-analytics`

This repo is the canonical data pipeline.

It owns:

- scraping and source integration
- parsing and normalization
- data validation and repair logic
- scheduled jobs and ingestion workflows
- canonical sports data tables and snapshots
- schema changes required for ingestion

It does not own:

- frontend UX
- billing
- product entitlements
- legal acceptance flows
- Vercel deployment of the app

### `David543M/mma-grid`

This repo is the application and product shell.

It owns:

- React frontend and routes
- product UX and navigation
- auth surfaces and private workspace flows
- pricing, paywalls, and billing UI
- legal pages and acceptance UI
- Vercel deployment and preview workflow
- feature gating and entitlements
- frontend-side read models consuming Supabase data

It does not own:

- scraping
- source website parsing
- canonical sports data cleanup jobs

## Source Of Truth

Use these rules consistently.

### Sports data

Source of truth:

- `MMA-analytics`

Examples:

- `fighter_profiles`
- `event_overview`
- `event_bouts`
- `ufc_rankings`
- `ufc_ranking_snapshots`

### Product and business state

Source of truth:

- `mma-grid`

Examples:

- pricing UI
- account UI
- feature gating
- beta access behavior
- billing surfaces
- legal documents
- custom ranking UX

### Shared persistence in Supabase

Both repos may touch the same Supabase project, but ownership must stay explicit.

- `MMA-analytics` writes ingestion-owned tables
- `mma-grid` writes product-owned tables

If a table’s main purpose is “sports data for the app,” it belongs to `MMA-analytics`.

If a table’s main purpose is “user/account/product/billing/legal state,” it belongs to `mma-grid`.

## Table Ownership

The default ownership model should be:

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

## Data Contract Between Repos

`mma-grid` must treat ingestion tables as external contracts, even if they are in the same Supabase project.

For every ingestion-backed table the app depends on, document:

- purpose
- required columns
- optional columns
- fallback behavior if data is missing
- freshness expectations
- owner repo

Recommended contract file:

- `docs/data-contract.md`

Minimum fields to document:

- table name
- owner repo
- consumer repo(s)
- update frequency
- expected nullability
- primary sorting rules
- known fallback logic in the app

## Change Management Rules

### If the change is about scraping or normalization

Work in:

- `MMA-analytics`

Examples:

- UFC rankings parser changes
- fixing fighter/event matching
- repairing ingestion gaps
- adding new source fields

### If the change is about product display or user workflow

Work in:

- `mma-grid`

Examples:

- new page sections
- comparison UX
- ranking builder UX
- account and billing screens
- legal acceptance flow

### If a change affects both repos

Use this order:

1. update `MMA-analytics`
2. deploy or run ingestion
3. verify Supabase data shape
4. update `mma-grid`
5. verify UI against the new contract

Never change the app to depend on a new ingestion field before the data contract exists.

## Branching Model

Keep branching lightweight.

### Recommended branches

- `main` for stable work
- `codex/...` for AI-assisted implementation
- `feature/...` for planned product work
- `fix/...` for targeted bug fixes

### Cross-repo changes

When one feature spans both repos:

- use matching branch names where possible
- reference the paired branch or commit in the PR description

Example:

- `MMA-analytics`: `feature/ufc-rankings-history`
- `mma-grid`: `feature/ufc-rankings-history-ui`

## Deployment Model

### `MMA-analytics`

Deployment model:

- GitHub Actions or scheduled jobs
- writes to Supabase
- no Vercel dependency required

Release checks:

- scraping/parsing succeeds
- validation checks pass
- canonical tables are populated correctly

### `mma-grid`

Deployment model:

- Vercel preview for branch testing
- Vercel production from `main`

Release checks:

- `npm test`
- `npm run build`
- required env vars configured
- billing and legal flows verified for the target environment

## Environments

Do not assume one environment.

At minimum, separate:

- local
- preview / beta
- production

Recommended rule:

- `MMA-analytics` should clearly know which Supabase environment it writes to
- `mma-grid` should clearly know which Supabase environment it reads from

The most common failure mode in a two-repo system is writing fresh data to one environment and reading stale data from another.

Always verify:

- Supabase project ref
- environment variables
- workflow target
- app runtime target

before debugging the UI.

## Release Workflow

For data-driven features, use this release path:

1. define the contract
2. implement data changes in `MMA-analytics`
3. validate rows and snapshots in Supabase
4. implement product changes in `mma-grid`
5. verify on local
6. verify on Vercel preview
7. merge to `main`
8. promote to production when checks are green

## Incident Ownership

Use this quick triage rule.

### Data looks wrong

Start in:

- `MMA-analytics`

Examples:

- only champions appear in rankings
- event bouts are missing fighter links
- stale or partially filled tables

### Data exists but UI is wrong

Start in:

- `mma-grid`

Examples:

- cards render incorrectly
- rankings appear in the wrong order
- compare links do not show despite valid data

### Billing or access is wrong

Start in:

- `mma-grid`

Examples:

- wrong plan label
- paywall missing
- legal acceptance flow broken
- billing portal button failing

## Ownership Checklist Before Merge

Before merging a change, ask:

1. Which repo owns this behavior?
2. Am I changing data contract or just UI?
3. Does the other repo need updating?
4. Do I know which environment I validated against?
5. Is the source of truth still clear after this change?

If any answer is unclear, stop and document it before merging.

## Short Version

If you only remember one thing, remember this:

- `MMA-analytics` decides what the sports data is
- `mma-grid` decides how the product uses it

Keep those boundaries clean, and the system will scale much more easily.
