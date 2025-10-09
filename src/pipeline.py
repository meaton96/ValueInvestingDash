import argparse

from src.scripts.fundamentals.fetch_fund import getSECZips
from src.scripts.fundamentals.update_fund_db import upsertFundamentals
from src.scripts.securities.build_security_master import get_securities_list
from src.scripts.securities.update_securities_db import db_update


def parse_args() -> argparse.Namespace:
    """Build and return the CLI arguments for the pipeline runner."""

    parser = argparse.ArgumentParser(
        description="Run the ValueInvestingDash ETL pipeline.",
    )
    parser.add_argument(
        "--write-csv",
        dest="write_csv",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Control whether the securities table snapshot is written to 'data/temp/temp_sec_table.csv'.",
    )

    return parser.parse_args()


def run_pipeline(write_csv: bool = False) -> None:
    """Execute the ETL workflow, optionally exporting a CSV snapshot."""

    df = get_securities_list()
    status_sec = db_update(df)
    if status_sec != 200:
        print(f"Error: {status_sec}")
        return
    print("Securities DB Updated")
    print("fetching fundamentals data")

    if write_csv:
        df.to_csv("data/temp/temp_sec_table.csv")
        print("Wrote securities snapshot to data/temp/temp_sec_table.csv")
    else:
        print("Skipping securities CSV snapshot (write_csv disabled)")

    # response = getSECZips()
    # if response["status"] != 200:
    #     return
    # print("Finished fetching data")
    # print("Parsing fundamentals zips")
    # upsertFundamentals(response["cf_path"], response["sub_path"], df)


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(write_csv=args.write_csv)