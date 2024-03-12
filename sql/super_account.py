GET_ORGANISATIONS_SQL = """
    SELECT [SuperAccountId],
           [Type],
           [Descr],
           [IsStuff],
           [IsBlocked],
           [Address],
           [Inn],
           [RegisterTime],
           [LastTransactionTime],
           [Email],
           [Phone],
           [WebSite],
           [Guid],
           [ChangeTime]
    FROM [SuperAccount]
    WHERE [Type] = 1
"""
