# Количество проходов в Аквазону за период
# Расчитывается количество проходов на вход в Аквазону через турникеты,
# при этом не учитываются идентификаторы сотрудников
PERIOD_CUSTOMER_COUNT_SQL = """
    SELECT DISTINCT 
        mt.[MasterTransactionId]
        ,[TransTime]
        ,[SuperAccountFrom]
        ,[SuperAccountTo]
        ,[UserId]
        ,mt.ServicePointId
        ,[ServerTime]
        ,[IsOffline]
        ,[Machine]
        ,mt.Guid
        ,td.StockInfoIdFrom
        ,td.StockInfoIdTo
        ,td.Amount
    FROM [AquaPark_Ulyanovsk].[dbo].[MasterTransaction] mt
        LEFT JOIN TransactionDetail td ON mt.MasterTransactionId = td.MasterTransactionId
        LEFT JOIN AccountStock ast ON td.StockInfoIdFrom = ast.AccountStockId
        LEFT JOIN AccountStock ast2 ON mt.SuperAccountTo = ast2.SuperAccountId
    WHERE mt.ServicePointId = 1  -- Турникет
        AND ast.CategoryId = 488  -- Аквазона
        AND ast2.CategoryId = 62 -- Идентификатор сотрудника
        AND ast.StockType = 41  -- Count на зонах (42 - Sum)
        AND td.StockInfoIdFrom = 523  -- Вход в зону (StockInfoIdTo = 523 - выход)
        AND mt.TransTime > '{date_from}' AND mt.TransTime < '{date_from}'
    ORDER BY mt.TransTime ASC
"""


# Клиентов в Аквазоне
CURRENT_CUSTOMER_COUNT_SQL = """
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
