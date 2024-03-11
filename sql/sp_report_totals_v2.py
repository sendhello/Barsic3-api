"""
Запроса для получения итогового отчета.
"""

# В старых версиях БД нет параметра hide_discount
SP_REPORT_TOTALS_V2_OLD_VERSION_SQL = """
    exec sp_reportOrganizationTotals_v2 
    @sa={org},
    @from='{date_from}',
    @to='{date_to}',
    @hideZeroes={hide_zeroes},
    @hideInternal={hide_internal}
"""

SP_REPORT_TOTALS_V2_SQL = (
    SP_REPORT_TOTALS_V2_OLD_VERSION_SQL + ",@hideDiscount={hide_discount}"
)
