from psycopg2.extensions import AsIs
import os
import glob
import pandas as pd
import numpy as np


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


def get_files(filepath):
    """Discover all files from filepath and return
    a list of all filepaths.

    Parameters
    ----------
    filepath : str
        root dir of files

    Returns
    -------
    List
        List of files paths
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def replace_df_nans_and_empty_strings_with_none(df, other=None):
    """To keep data types consistent, we will replace all np.nan
    and empty strings with None."""

    df.replace(to_replace="", value=np.nan, inplace=True)
    df = df.where(cond=pd.notnull(df), other=other)

    return df


def build_df_from_single_filepath(filepath):
    """Build a dataframe from a single filepath"""

    df = pd.read_json(filepath, lines=True)

    # Replace np.nan and empty strings with None
    df = replace_df_nans_and_empty_strings_with_none(df, other=None)

    return df


def build_df_from_multiple_filepaths(filepaths):
    """Read a list of filepaths and turn them into a pd.DataFrame."""

    dfs = [build_df_from_single_filepath(file_) for file_ in filepaths]
    dfs = pd.concat(dfs, ignore_index=True)

    return dfs
