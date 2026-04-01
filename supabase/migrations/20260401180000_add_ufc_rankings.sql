create table if not exists public.ufc_rankings (
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
  updated_at timestamptz not null default now()
);

create unique index if not exists ufc_rankings_category_sort_idx
  on public.ufc_rankings (category_key, sort_order);

create index if not exists ufc_rankings_group_key_idx
  on public.ufc_rankings (category_group, category_key);

create index if not exists ufc_rankings_fighter_id_idx
  on public.ufc_rankings (fighter_id);

alter table public.ufc_rankings enable row level security;

grant select on public.ufc_rankings to anon, authenticated;
grant all on public.ufc_rankings to service_role;

drop policy if exists "ufc_rankings_readable_by_everyone" on public.ufc_rankings;
create policy "ufc_rankings_readable_by_everyone"
  on public.ufc_rankings
  for select
  using (true);
