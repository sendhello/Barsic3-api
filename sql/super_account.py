def get_organisations(db_name: str):
    return f"""
        SELECT TOP (1000) [SuperAccountId],
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
        FROM [{db_name}].[dbo].[SuperAccount]
        WHERE [Type] = 1
    """
