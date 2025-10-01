from src.scripts.securities.build_security_master import get_securities_list
from src.scripts.securities.update_db import db_update




def run_pipeline():
     # get_securities_list now RETURNS the csv_path it wrote (or raises)
    df = get_securities_list()
    status = db_update(df)
    print(f"status: {status}")

if __name__ == "__main__":
    run_pipeline()