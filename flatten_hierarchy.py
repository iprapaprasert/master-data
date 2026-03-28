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
    
    Examples
    --------
    >>> data = [(1, None, "Alice", "CEO"), (2, 1, "Bob", "VP")]
    >>> df = spark.createDataFrame(data, ["id", "parent", "Name", "Position"])
    >>> flatten_hierarchy(df, id="id", parent="parent", maxlvl=2, descs=["Name", "Position"])
    +-------+--------+------------+-------+--------+------------+
    |Lvl1Key|Lvl1Name|Lvl1Position|Lvl2Key|Lvl2Name|Lvl2Position|
    +-------+--------+------------+-------+--------+------------+
    |      2|     Bob|          VP|      1|   Alice|         CEO|
    +-------+--------+------------+-------+--------+------------+
    """

    descs = descs if isinstance(descs, list) else []

    df_hierarchy = (
        df.withColumnRenamed(id, "Lvl1Key")
        .withColumnRenamed(parent, "Lvl2Key")
    )
    for desc in descs:
        df_hierarchy = df_hierarchy.withColumnRenamed(desc, f'Lvl1{desc}')

    i = 2

    while i <= maxlvl:
        cur_level = f"Lvl{i}Key"
        next_level = f"Lvl{(i+1)}Key"
        next_level_tmp = f"lvl_{(i+1)}_tmp"

        df_hlevel = (
            df.withColumnRenamed(id, cur_level)
            .withColumnRenamed(parent, next_level)
        )
        for desc in descs:
            df_hlevel = df_hlevel.withColumnRenamed(desc, f'Lvl{i}{desc}')

        df_hierarchy = df_hierarchy.join(df_hlevel, cur_level, 'left')
        df_hierarchy = df_hierarchy.select('*', df_hierarchy[next_level].alias(next_level_tmp))
        df_hierarchy = df_hierarchy.drop(next_level)
        df_hierarchy = df_hierarchy.withColumnRenamed(next_level_tmp, next_level)

        i += 1

        if i == maxlvl + 1:
            df_hierarchy = df_hierarchy.drop(next_level)
    
    ordered_cols = []
    for lvl in range(1, maxlvl + 1):
        ordered_cols.append(f"Lvl{lvl}Key")
        for desc in descs:
            ordered_cols.append(f"Lvl{lvl}{desc}")

    return df_hierarchy.filter(col(f"Lvl{maxlvl}Key").isNotNull()) \
        .select(ordered_cols)