DDL_FUNDAMENTALS_RAW = """
    create table if not exists fundamentals_raw (
    cik bigint not null,
    accession_no text not null,
    fiscal_year int,
    fiscal_period text,
    tag text not null,
    value numeric,
    unit text,
    frame text,
    primary key (cik, accession_no, tag, frame)
    );


    create index if not exists idx_fund_raw_cik_tag on fundamentals_raw (cik, tag);
    create index if not exists idx_fund_raw_year on fundamentals_raw (fiscal_year);
"""


MV_FUNDAMENTALS_LATEST = """
    create materialized view if not exists fundamentals_latest as
    select cik,
    max(case when tag in ('AssetsCurrent') then value end) as current_assets,
    max(case when tag in ('LiabilitiesCurrent') then value end) as current_liabilities,
    max(case when tag in ('Liabilities') then value end) as total_liabilities,
    max(case when tag in ('StockholdersEquity','StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest') then value end) as equity,
    max(case when tag in ('EarningsPerShareBasic','EarningsPerShareDiluted') then value end) as eps,
    max(case when tag in ('NetIncomeLoss','ProfitLoss') then value end) as net_income,
    max(case when tag in ('NetCashProvidedByUsedInOperatingActivities') then value end) as op_cf,
    max(case when tag in ('PaymentsOfDividendsCommonStock','CommonStockDividendsPerShareDeclared') then value end) as dividends
    from fundamentals_raw
    group by cik;
"""