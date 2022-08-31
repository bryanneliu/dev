def normalize_text(dataframe, column):
    """
    Normalize text in the column in the dataframe by removing \\t, \\n,
    punctuations (except .'-), and whitespaces. Lower cases. Replace empty strings
    and NaN by null.

    Parameters
    ----------
    dataframe : PySpark dataframe
        Input dataframe
    column : str
        Name of the column to normalize

    Returns
    -------
    dataframe : PySpark dataframe
        Output dataframe
    """

    dataframe = dataframe \
        .withColumn(column, regexp_replace(col(column), "[\s\p{Punct}&&[^.'-]]+", " ")) \
        .withColumn(column, trim(lower(col(column))))

    dataframe = dataframe \
        .withColumn(column, when((col(column) != '') & (~isnan(col(column))), col(column)))