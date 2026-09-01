# Итоги по кассам с видами оплат в разрезе организации.
#
# Повторяет логику хранимой процедуры sp_reportCashDeskMoney, но добавляет отбор по организации.
# Процедура группирует только по номеру кассы (Check.Cassa), а один и тот же номер кассы может
# использоваться фискальными регистраторами разных организаций, поэтому по ее результату
# разделить выручку между организациями невозможно.
# Владелец чека определяется по фискальному регистратору: Check.KkmId -> Kkm.SuperAccountId.
CASH_DESK_MONEY_BY_COMPANY_SQL = """
    SELECT
        c.Cassa,
        SUM(c.Summa) SellSumma,
        SUM(c.Nal) PayCash,
        SUM(c.Beznal) + SUM(ISNULL(cp.Amount, 0)) PayNoCash,
        SUM(c.Chet) PayAccount,
        SUM(c.Bonus) PayBonus,
        SUM(c.Currency) PayCurrency,
        SUM(ISNULL(cp.Amount, 0)) PayNoCash2,
        N'Продажа' ActionName
    FROM [Check] c
        INNER JOIN [Kkm] kkm ON kkm.KkmId = c.KkmId
        LEFT JOIN (
            SELECT cp.*
            FROM [CheckPay] cp
                INNER JOIN [CheckPayType] cpt ON cpt.[CheckPayTypeId] = cp.[CheckPayTypeId]
                    AND CAST(REPLACE(cpt.[ExtendedData], 'encoding="utf-8"', '') AS xml).value(
                        '(/PayTypeExtendedData/IsBeznal)[1]', 'bit') = 1
        ) cp ON cp.CheckId = c.CheckId
    WHERE c.TypeOper = 1 AND c.Status > 0
        AND c.Data >= '{date_from}' AND c.Data <= '{date_to}'
        AND kkm.SuperAccountId = {company_id}
    GROUP BY c.Cassa

    UNION ALL

    SELECT
        c.Cassa,
        SUM(c.Summa) SellSumma,
        SUM(c.Nal) PayCash,
        SUM(c.Beznal) + SUM(ISNULL(cp.Amount, 0)) PayNoCash,
        SUM(c.Chet) PayAccount,
        SUM(c.Bonus) PayBonus,
        SUM(c.Currency) PayCurrency,
        SUM(ISNULL(cp.Amount, 0)) PayNoCash2,
        N'Возврат' ActionName
    FROM [Check] c
        INNER JOIN [Kkm] kkm ON kkm.KkmId = c.KkmId
        LEFT JOIN (
            SELECT cp.*
            FROM [CheckPay] cp
                INNER JOIN [CheckPayType] cpt ON cpt.[CheckPayTypeId] = cp.[CheckPayTypeId]
                    AND CAST(REPLACE(cpt.[ExtendedData], 'encoding="utf-8"', '') AS xml).value(
                        '(/PayTypeExtendedData/IsBeznal)[1]', 'bit') = 1
        ) cp ON cp.CheckId = c.CheckId
    WHERE c.TypeOper = 2 AND c.Status > 0
        AND c.Data >= '{date_from}' AND c.Data <= '{date_to}'
        AND kkm.SuperAccountId = {company_id}
    GROUP BY c.Cassa
"""
