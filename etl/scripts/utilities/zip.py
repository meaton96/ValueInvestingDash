from contextlib import contextmanager
import zipfile

@contextmanager
def open_zip(path: str):
    zf = zipfile.ZipFile(path, "r")
    try:
        yield zf
    finally:
        zf.close()