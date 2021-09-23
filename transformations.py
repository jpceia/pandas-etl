from unidecode import unidecode
import pandas as pd
import numpy as np
import os



def sanitize_text(col):
    return col.fillna("").str.lower().map(unidecode)


def text_contains_any(col, *args):
    col = pd.Series(False, index=col.index)
    for w in args:
        col |= col.str.contains(w)
    return col


def text_contains_all(col, *args):
    col = pd.Series(True, index=col.index)
    for w in args:
        col &= col.str.contains(w)
    return col


def is_equal(col, value):
    return col == value


def contains(col, *args):
    return col.isin(args)


def notnull(col):
    return col.notnull()


def between(col, min, max):
    return (col > min) & (col < max)


def greater_than(col, value):
    return col > value


def explode_dictionary(col):
    return pd.DataFrame(col.tolist(), index=col.index)
