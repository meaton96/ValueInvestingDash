from src.scripts.securities.build_security_master import get_securities_list
from src.scripts.securities.update_securities_db import db_update
from src.scripts.fundamentals.fetch_fund import getSECZips
from src.scripts.fundamentals.update_fund_db import upsertFundamentals


def run_pipeline():
    
    df = get_securities_list()
    status_sec = db_update(df)
    if status_sec != 200:
        print(f"Error: {status_sec}")
        return
    print(f'Securities DB Updated')
    print('fetching fundamentals data')
    response = getSECZips()
    if response['status'] != 200:
        return
    upsertFundamentals(response['cf_path'], response['sub_path'], df)




if __name__ == "__main__":
    run_pipeline()