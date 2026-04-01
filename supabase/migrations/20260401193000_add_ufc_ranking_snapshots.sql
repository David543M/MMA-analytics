create table if not exists public.ufc_ranking_snapshots (
  id uuid primary key default gen_random_uuid(),
  category_key text not null,
  category_label text not null,
  category_group text not null check (category_group in ('p4p', 'men', 'women')),
  division_label text,
  sort_order integer not null check (sort_order > 0),
  rank_position integer,
  fighter_id uuid references public.fighters(id) on delete set null,
  fighter_name text not null,
  is_champion boolean not null default false,
  is_interim boolean not null default false,
  source_label text not null default 'UFC Rankings',
  source_url text,
  scraped_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create unique index if not exists ufc_ranking_snapshots_snapshot_sort_idx
  on public.ufc_ranking_snapshots (category_key, sort_order, scraped_at);

create index if not exists ufc_ranking_snapshots_scraped_at_idx
  on public.ufc_ranking_snapshots (scraped_at desc);

create index if not exists ufc_ranking_snapshots_source_idx
  on public.ufc_ranking_snapshots (source_label, scraped_at desc);

alter table public.ufc_ranking_snapshots enable row level security;

grant select on public.ufc_ranking_snapshots to anon, authenticated;
grant all on public.ufc_ranking_snapshots to service_role;

drop policy if exists "ufc_ranking_snapshots_readable_by_everyone" on public.ufc_ranking_snapshots;
create policy "ufc_ranking_snapshots_readable_by_everyone"
  on public.ufc_ranking_snapshots
  for select
  using (true);
