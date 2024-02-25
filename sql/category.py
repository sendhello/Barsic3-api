def get_tariffs(db_name: str, organization_id: int):
    return f"""
        SELECT [CategoryId],
               [StockType],
               [Name],
               [OrganizationId],
               [Guid],
               [ChangeTime]
        FROM [{db_name}].[dbo].[Category]
        WHERE [OrganizationId] = {organization_id}
    """
