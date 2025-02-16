GET_TRANSACTIONS_BY_SERVICE_NAME_PATTERN = """
    WITH accounts AS (
        SELECT mt0.SuperAccountTo AS SuperAccountId
        FROM [AquaPark_Ulyanovsk].[dbo].[MasterTransaction] mt0 
        JOIN (
            SELECT cd0.Id
            FROM [AquaPark_Ulyanovsk].[dbo].[CheckDetail] cd0
            JOIN (
                SELECT [CheckId]  
                FROM [AquaPark_Ulyanovsk].[dbo].[Check]
                WHERE [Data] > '{date_from}' AND [Data] < '{date_to}' AND [Status] = 1
            ) ch0 ON cd0.CheckId = ch0.CheckId
            WHERE [Name] LIKE '%{service_name_pattern}%'
        ) cdetail ON mt0.CheckDetailId = cdetail.Id
        WHERE SuperAccountTo NOT IN ({companies_ids})
    )
    SELECT mt.MasterTransactionId as MasterTransactionId,
        CASE WHEN SuperAccountFrom IN ({companies_ids}) THEN SuperAccountTo ELSE SuperAccountFrom END AS SuperAccount,
        [TransTime],
        [SuperAccountFrom],
        [SuperAccountTo],
        [UserId],
        [ServicePointId],
        [ServerTime],
        [CheckDetailId],
        mt.ExtendedData,
        [ExternalId],
        [Machine],
        [SecSubjectId],
        [Guid],
        [CheckId],
        [Name],
        [Count],
        [Price],
        [CardCode],
        [CategoryId],
        [TypeGood],
        [Account]
    FROM [AquaPark_Ulyanovsk].[dbo].[MasterTransaction] mt
    LEFT JOIN [AquaPark_Ulyanovsk].[dbo].[CheckDetail] cd
    ON mt.CheckDetailId = cd.Id
    WHERE (
        mt.SuperAccountFrom IN (SELECT SuperAccountId FROM accounts)
        OR mt.SuperAccountTo IN (SELECT SuperAccountId FROM accounts)
    ) AND (
        mt.CheckDetailId IS NOT NULL OR mt.ExtendedData IS NOT NULL
    )
"""
