from src.scripts.build_security_master import get_securities_list
from src.scripts.setup_db import db_update


def run_pipeline():
    status = get_securities_list()
    if status == 200:
        status = db_update()

    print(f'status: {status}')

if __name__ == "__main__":
    run_pipeline()