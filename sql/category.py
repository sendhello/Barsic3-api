GET_TARIFFS_SQL = """
    SELECT [CategoryId],
           [StockType],
           [Name],
           [OrganizationId],
           [Guid],
           [ChangeTime]
    FROM [Category]
    WHERE [OrganizationId] = {organization_id}
"""
