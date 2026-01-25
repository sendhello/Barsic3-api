ZONE_IN_COUNT = """
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
        AND mt.TransTime > '2025-01-13' AND mt.TransTime < '2025-01-14'
    ORDER BY mt.TransTime ASC
"""
