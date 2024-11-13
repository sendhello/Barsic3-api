CLIENTS_COUNT_SQL = """
    SELECT
        [gr].[c1] as [c11],
        [gr].[StockCategory_Id] as [StockCategory_Id1],
        [c].[Name],
        [c].[NN]
    FROM
        (
            SELECT
                [_].[CategoryId] as [StockCategory_Id],
                Count(*) as [c1]
            FROM
                [AccountStock] [_]
                    INNER JOIN [SuperAccount] [t1] ON [_].[SuperAccountId] = [t1].[SuperAccountId]
            WHERE
                [_].[StockType] = 41 AND
                [t1].[Type] = 0 AND
                [_].[Amount] > 0 AND
                NOT ([t1].[IsStuff] = 1)
            GROUP BY
                [_].[CategoryId]
        ) [gr]
    INNER JOIN [Category] [c] ON [gr].[StockCategory_Id] = [c].[CategoryId]
"""
