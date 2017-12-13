import pandas as pd


def merge_dataframes(df_merge_list, how='inner', on=None):
    """
    Merge a list (or dict) of dataframes using a shared column or the index
    """
    try:
        # df_merge_list is a dict
        df_merge_list = df_merge_list.values()
    except AttributeError:
        # df_merge_list is already a list
        pass
    result = list(df_merge_list).pop(0)
    for df in df_merge_list:
        if on:
            result = result.merge(df, how=how, on=on)
        else:
            result = result.merge(df, how=how, left_index=True, right_index=True)
    return result


def pivot_dataframe(df, index_name, column_name):
    """
    Pivots a dataframe such that you have a single row for each unique ID.

    index_name should correspond to the unique id

    column_name should correspond to the primary question which the
    subsequent questions/columns are based on.

    The new column names correspond to the primary question ID, the value
    of the primary question, and the subsequent question. Example:
    q103_2_q104 --- this corresponds to values for q104 when q103 has the
    value of 2
    """
    # Pivot dataframe
    result = df.pivot(index=index_name, columns=column_name)
    # Pivot moves index_name to an index... reset to a column
    result.reset_index(inplace=True)
    # Pivot creates multi-index columns if more than two columns (excluding
    # index) exist. Collapse multi-index column names
    c_names = result.columns.tolist()
    c_names = [c_names[0][0]] + [column_name + "_" + str(c[1]) + "_" + str(c[0]) for c in c_names[1:]]
    c_index = pd.Index(c_names)
    result.columns = c_index
    return result
