import os
import time


def timestr():
    return time.strftime("%H:%M:%S")


def is_float(val):
    try:
        float(val)
        return True
    except ValueError:
        return False


def iterrows(cursor, chunksize=1000): #, max_rows=500000):
    rows = 0
    while True: #rows <= max_rows:
        results = cursor.fetchmany(chunksize)
        rows += chunksize
        if not results:
            break
        for result in results:
            yield result


def files_in_folder(folder, prefix=None):
    if prefix:
        return sorted([os.path.abspath(fp) for fp in os.listdir(folder) if fp.startswith(prefix)])
    return sorted([os.path.abspath(fp) for fp in os.listdir(folder)])
