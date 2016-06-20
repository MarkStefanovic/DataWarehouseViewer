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


def files_in_folder(folder, prefix=None):
    if prefix:
        return sorted([os.path.abspath(fp) for fp in os.listdir(folder) if fp.startswith(prefix)])
    return sorted([os.path.abspath(fp) for fp in os.listdir(folder)])
