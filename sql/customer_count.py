from collections.abc import Iterable


def name_like_filter(column: str, names: Iterable[str]) -> str:
    """Собирает OR-цепочку `column LIKE N'%name%'` для поиска по подстроке в названии.

    Если список названий пуст, возвращает всегда ложное условие, чтобы `NOT EXISTS`
    с таким фильтром ничего не отсеивал.
    """
    conditions = []
    for name in names:
        escaped_name = name.replace("'", "''")
        conditions.append(f"{column} LIKE N'%{escaped_name}%'")

    if not conditions:
        return "1 = 0"

    return " OR ".join(conditions)


# Количество проходов в Аквазону за период
# Проходом считается транзакция на турникете (ServicePoint.Type = 1), в которой склад зоны
# (AccountStock со StockType = 41 и категорией Аквазоны) списывается с аккаунта организации на клиента.
# Не учитываются идентификаторы сотрудников и проходы по услугам из NOT_COUNTED_SERVICE_NAMES.
PERIOD_CUSTOMERS_SQL = """
    SELECT mt.[MasterTransactionId]
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
    FROM [MasterTransaction] mt
        JOIN [TransactionDetail] td ON td.MasterTransactionId = mt.MasterTransactionId
        -- Только турникеты, независимо от организации и конкретной точки обслуживания
        JOIN [ServicePoint] sp ON sp.ServicePointId = mt.ServicePointId AND sp.Type = 1
        -- Вход в зону: склад зоны уходит с аккаунта организации клиенту
        JOIN [AccountStock] af ON af.AccountStockId = td.StockInfoIdFrom
            AND af.CategoryId = {zone_category_id}
            AND af.StockType = 41
        JOIN [SuperAccount] org ON org.SuperAccountId = af.SuperAccountId AND org.Type = 1
    WHERE mt.TransTime > '{date_from}' AND mt.TransTime < '{date_to}'
        AND mt.SuperAccountTo IN (
            SELECT SuperAccountId
            FROM [SuperAccount] sa
            WHERE sa.IsStuff <> 1
        )
        -- Отдельные турникеты услуг, не считающихся входом в Аквазону
        AND NOT EXISTS (
            SELECT 1
            FROM [ServicePoint] spx
            WHERE spx.ServicePointId = mt.ServicePointId
                AND ({service_point_name_filter})
        )
        -- Транзакции, в которых участвует услуга, не считающаяся входом в Аквазону
        AND NOT EXISTS (
            SELECT 1
            FROM [TransactionDetail] tdx
                JOIN [AccountStock] ax ON ax.AccountStockId IN (tdx.StockInfoIdFrom, tdx.StockInfoIdTo)
                JOIN [Category] cx ON cx.CategoryId = ax.CategoryId
            WHERE tdx.MasterTransactionId = mt.MasterTransactionId
                AND ({category_name_filter})
        )
    ORDER BY mt.TransTime ASC
"""


PERIOD_CUSTOMER_COUNT_SQL = """
    SELECT COUNT(DISTINCT mt.[MasterTransactionId])
    FROM [MasterTransaction] mt
        JOIN [TransactionDetail] td ON td.MasterTransactionId = mt.MasterTransactionId
        -- Только турникеты, независимо от организации и конкретной точки обслуживания
        JOIN [ServicePoint] sp ON sp.ServicePointId = mt.ServicePointId AND sp.Type = 1
        -- Вход в зону: склад зоны уходит с аккаунта организации клиенту
        JOIN [AccountStock] af ON af.AccountStockId = td.StockInfoIdFrom
            AND af.CategoryId = {zone_category_id}
            AND af.StockType = 41
        JOIN [SuperAccount] org ON org.SuperAccountId = af.SuperAccountId AND org.Type = 1
    WHERE mt.TransTime > '{date_from}' AND mt.TransTime < '{date_to}'
        AND mt.SuperAccountTo IN (
            SELECT SuperAccountId
            FROM [SuperAccount] sa
            WHERE sa.IsStuff <> 1
        )
        -- Отдельные турникеты услуг, не считающихся входом в Аквазону
        AND NOT EXISTS (
            SELECT 1
            FROM [ServicePoint] spx
            WHERE spx.ServicePointId = mt.ServicePointId
                AND ({service_point_name_filter})
        )
        -- Транзакции, в которых участвует услуга, не считающаяся входом в Аквазону
        AND NOT EXISTS (
            SELECT 1
            FROM [TransactionDetail] tdx
                JOIN [AccountStock] ax ON ax.AccountStockId IN (tdx.StockInfoIdFrom, tdx.StockInfoIdTo)
                JOIN [Category] cx ON cx.CategoryId = ax.CategoryId
            WHERE tdx.MasterTransactionId = mt.MasterTransactionId
                AND ({category_name_filter})
        )
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
