-- Supabase Database Schema for CexDex Pairs

-- Create pairs table (simplified)
create table pairs (
  id text primary key,
  network text not null,
  pair_address text not null,
  dex_name text,
  base_token jsonb,
  quote_token jsonb,
  pool_name text,
  created_at timestamp with time zone,
  indexed_at timestamp with time zone,
  updated_at timestamp with time zone default now()
);

-- Create indexes
create index pairs_network_idx on pairs(network);
create index pairs_created_at_idx on pairs(created_at desc);

-- Enable Row Level Security
alter table pairs enable row level security;

-- Policy for public read
create policy "Enable read access for all users" on pairs
  for select using (true);

-- Policy for write (service role)
create policy "Enable write access for service role" on pairs
  for all using (true);

-- Auto-update updated_at
create or replace function update_updated_at_column()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

create trigger update_pairs_updated_at
  before update on pairs
  for each row
  execute function update_updated_at_column();