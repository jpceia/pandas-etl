import os
import gc
import json
import pandas as pd
import transformations
from pprint import pprint


def _pick_col_names(instructions):
    if "columns" in instructions:
        return instructions["columns"]
    elif "column" in instructions:
        return instructions["column"]
    raise ValueError("Column(s) label not available")


# aux function
def _load_func(path):
    if isinstance(path, str):
        return getattr(transformations, path)
    assert len(path) == 2
    fname, ext = os.path.splitext(path[0])
    lib_path = fname.replace('/', '.')
    lib = __import__(lib_path)
    return getattr(lib, path[1])


def _rename_columns(df, cols):
    if isinstance(df, pd.Series):
        if isinstance(cols, str):
            df.name = cols
        elif isinstance(cols, list):
            assert len(cols) == 1
            df.name = cols[0]
        else:
            raise ValueError
    elif isinstance(df, pd.DataFrame):
        if isinstance(cols, str):
            cols = [cols]
        df.columns = cols
    else:
        raise ValueError


def _join_dataframes(df1, df2):
    if isinstance(df1, pd.Series):
        df1 = df1.to_frame()
    if isinstance(df2, pd.Series):
        df2 = df2.to_frame()
    df = pd.concat([df1, df2], axis=1)
    return df.loc[:,~df.columns.duplicated(keep='last')]


def apply_set_index(df, instructions):
    cols = _pick_col_names(instructions)
    return df.set_index(cols)


def apply_select_columns(df, instructions):
    return df[_pick_col_names(instructions)]


def apply_rename_column(df, instructions):
    assert "old" in instructions
    assert "new" in instructions
    col_old = instructions["old"]
    col_new = instructions["new"]
    return df.rename(columns={col_old: col_new})


def apply_remove_columns(df, instructions):
    cols = _pick_col_names(instructions)
    return df.drop(cols, axis=1)


def apply_drop_duplicates(df, instructions):
    cols = _pick_col_names(instructions)
    return df.drop_duplicates(cols)


def apply_transform(df, instructions):
    assert "columns" in instructions
    assert "script" in instructions
    assert "result" in instructions
    cols = instructions["columns"]
    res = instructions["result"]
    func = _load_func(instructions["script"])
    args = instructions.get("args", [])
    kwargs = instructions.get("kwargs", {})
    if not isinstance(args, list):
        args = [args]
    df[res] = func(df[cols], *args, **kwargs)
    return df


def apply_filter_rows(df, instructions):
    assert "condition" in instructions
    condition = instructions["condition"]
    if isinstance(condition, str):
        filt = df[condition]
    elif isinstance(condition, dict):
        condition["result"] = "replace_all"
        filt = apply_transform(df, condition)
        assert isinstance(filt, pd.Series)
    else:
        raise ValueError
    return df[filt]


def apply_save(df, instructions):
    assert "fname" in instructions
    fname = instructions["fname"]
    args = instructions.get("args", [])
    kwargs = instructions.get("kwargs", {})
    _, ext = os.path.splitext(fname)
    if ext == ".csv":
        df.to_csv(fname, *args, **kwargs)
    else:
        raise ValueError("Unsuported extension: {}".format(ext))
    return df


def apply_instructions(df, instructions):
    assert "action" in instructions
    action = instructions["action"]
    if action == "select_columns":
        return apply_select_columns(df, instructions)
    elif action == "filter_rows":
        return apply_filter_rows(df, instructions)
    elif action == "rename_column":
        return apply_rename_column(df, instructions)
    elif action == "drop_duplicates":
        return apply_drop_duplicates(df, instructions)
    elif action == "set_index":
        return apply_set_index(df, instructions)
    elif action == "transform":
        return apply_transform(df, instructions)
    elif action == "remove_columns":
        return apply_remove_columns(df, instructions)
    elif action == "log":
        return apply_log(df, instructions)
    elif action == "save":
        return apply_save(df, instructions)
    raise ValueError("Unknown action type: %s" % action)


def apply_pipeline(df, config, verbose=True):
    if isinstance(config, str):
        with open(config) as f:
            config = json.load(f)
    elif not isinstance(config, list):
        raise ValueError("Invalid argument")
    for instructions in config:
        if verbose:
            pprint(instructions)
        df = apply_instructions(df, instructions)
        gc.collect()
    return df


def config_check_actions(config, allowed_actions=None):
    for instructions in config:
        assert "action" in instructions
        action = instructions["action"]
        if not action in allowed_actions:
            raise ValueError("Invalid action in pipeline: {}".format(action))
