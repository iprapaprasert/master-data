def flatten_hierarchy(
    df: DataFrame, 
    id: str, 
    parent: str, 
    maxlvl: int, 
    descs: list=None
) -> DataFrame:
    """
    Flatten a parent-child hierarchy table with specific maximum hierarchy level.

    Parameters
    ----------
    df : `DataFrame` 
        a parent-child hierarchy table.
    id : column name 
        the unique identifier for rows in the table.
    parent : column name 
        the unique identifier for the parent of the current row.
    max : int
        maximum depth to resolve.
    descs : list, optional: 
        list of description of each id. 
    
    Returns
    -------
    `DataFrame`
        a flatten hierarchy table
    """

    descs = descs if isinstance(descs, list) else []

    df_hierarchy = (
        df.withColumnRenamed(id, "level1_id")
        .withColumnRenamed(parent, "level2_id")
    )
    for desc in descs:
        df_hierarchy = df_hierarchy.withColumnRenamed(desc, f'level1_{desc}')

    i = 2

    while i <= maxlvl:
        cur_level = f'level{i}_id'
        next_level = f'level{(i+1)}_id'
        next_level_tmp = f'level_{(i+1)}_tmp'

        df_hlevel = (
            df.withColumnRenamed(id, cur_level)
            .withColumnRenamed(parent, next_level)
        )
        for desc in descs:
            df_hlevel = df_hlevel.withColumnRenamed(desc, f'level{i}_{desc}')

        df_hierarchy = df_hierarchy.join(df_hlevel, cur_level, 'left')
        df_hierarchy = df_hierarchy.select('*', df_hierarchy[next_level].alias(next_level_tmp))
        df_hierarchy = df_hierarchy.drop(next_level)
        df_hierarchy = df_hierarchy.withColumnRenamed(next_level_tmp, next_level)

        i += 1

        if i == maxlvl + 1:
            df_hierarchy = df_hierarchy.drop(next_level)

    return df_hierarchy.select(sorted(df_hierarchy.columns))