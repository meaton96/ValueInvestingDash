CREATE_SQL = """
create extension if not exists "pgcrypto";

create table if not exists securities(
    cik          bigint primary key,          
    ticker       varchar(7)  not null,
    name         varchar(40) not null,
    exchange     varchar(15) not null,
    company_name varchar(50) not null,
    symbol_yf    varchar(7)  not null,
    first_seen   date        not null,
    last_seen    date        not null,
    constraint securities_cik_check check (cik between 1 and 9999999999)
);

create index if not exists idx_securities_ticker   on securities(ticker);
create index if not exists idx_securities_exchange on securities(exchange);
"""

UPSERT_SQL = """
insert into securities(
    cik, ticker, name, exchange, company_name, symbol_yf, first_seen, last_seen
) values (
    :cik, :ticker, :name, :exchange, :company_name, :symbol_yf, :first_seen, :last_seen
)
on conflict (cik) do update
set
    ticker       = excluded.ticker,
    name         = excluded.name,
    exchange     = excluded.exchange,
    company_name = excluded.company_name,
    symbol_yf    = excluded.symbol_yf,
    first_seen   = least(securities.first_seen, excluded.first_seen),
    last_seen    = greatest(securities.last_seen, excluded.last_seen);
"""
CREATE_UNRESOLVED = """
create table if not exists securities_unresolved (
  id bigserial primary key,
  ticker       varchar(7)  not null,
  name         varchar(80) not null,
  exchange     varchar(15) not null,
  company_name varchar(80),
  symbol_yf    varchar(7)  not null,
  first_seen   date not null,
  last_seen    date not null,
  reason       text,
  unique (ticker, exchange)
);
"""

UPSERT_UNRESOLVED = """
insert into securities_unresolved(
  ticker, name, exchange, company_name, symbol_yf, first_seen, last_seen, reason
) values (
  :ticker, :name, :exchange, :company_name, :symbol_yf, :first_seen, :last_seen, :reason
)
on conflict (ticker, exchange) do update
set
  name         = excluded.name,
  company_name = coalesce(excluded.company_name, securities_unresolved.company_name),
  symbol_yf    = excluded.symbol_yf,
  last_seen    = greatest(securities_unresolved.last_seen, excluded.last_seen),
  reason       = excluded.reason;
"""