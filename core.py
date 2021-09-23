import os
import gc
import json
import pandas as pd
from pprint import pprint



def apply_filter_columns(df, instructions):
    assert "columns" in instructions
    cols = instructions["columns"]
    return df[cols]


def apply_rename_column(df, instructions):
    assert "column" in instructions
    assert "result" in instructions
    col = instructions["column"]
    res = instructions["result"]
    return df.rename(columns={col: res})


def apply_drop_duplicates(df, instructions):
    assert "columns" in instructions
    cols = instructions["columns"]
    return df.drop_duplicates(cols)


def apply_set_index(df, instructions):
    assert "column" in instructions
    index_col = instructions["column"]
    return df.set_index(index_col)


def apply_remove_columns(df, instructions):
    assert "columns" in instructions
    cols = instructions["columns"]
    return df.drop(cols, axis=1)


def apply_transform_column(df, instructions):
    assert "columns" in instructions
    assert "script" in instructions
    assert "result" in instructions
    cols = instructions["columns"]
    res = instructions["result"]
    script = instructions["script"]
    fname, ext = os.path.splitext(script[0])
    lib_path = fname.replace('/', '.')
    lib = __import__(lib_path)
    func = getattr(lib, script[1])
    args = instructions.get("args", [])
    kwargs = instructions.get("kwargs", {})
    if not isinstance(args, list):
        args = [args]
    df[res] = func(df[cols], *args, **kwargs)
    return df


def apply_filter_rows(df, instructions):
    assert "columns" in instructions
    assert "script" in instructions
    cols = instructions["columns"]
    script = instructions["script"]
    assert len(script) == 2
    fname, ext = os.path.splitext(script[0])
    assert ext == ".py"
    lib_path = fname.replace('/', '.')
    lib = __import__(lib_path)
    func = getattr(lib, script[1])
    args = instructions.get("args", [])
    kwargs = instructions.get("kwargs", {})
    if not isinstance(args, list):
        args = [args]
    filt = func(df[cols], *args, **kwargs)
    return df[filt]


def apply_save(df, instructions):
    assert "fname" in instructions
    fname = instructions["fname"]
    args = instructions.get("args", [])
    kwargs = instructions.get("kwargs", {})
    df.to_csv(fname, *args, **kwargs)


def apply_instructions(df, instructions):
    pprint(instructions)
    assert "action" in instructions
    action = instructions["action"]
    if action == "filter_columns":
        return apply_filter_columns(df, instructions)
    elif action == "filter_rows":
        return apply_filter_rows(df, instructions)
    elif action == "rename_column":
        return apply_rename_column(df, instructions)
    elif action == "drop_duplicates":
        return apply_drop_duplicates(df, instructions)
    elif action == "set_index":
        return apply_set_index(df, instructions)
    elif action == "transform_column":
        return apply_transform_column(df, instructions)
    elif action == "remove_columns":
        return apply_remove_columns(df, instructions)
    elif action == "save":
        return apply_save(df, instructions)
    raise ValueError("Unknown action type: %s" % action)


def apply_pipeline(df, config, **kwargs):
    if isinstance(config, str):
        with open(config) as f:
            config = json.load(f)
    elif not isinstance(config, list):
        raise ValueError("Invalid argument")
    for instructions in config:
        df = apply_instructions(df, instructions)
        gc.collect()