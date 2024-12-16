"""
Запрос для получения суммы по кафе Смайл.
Возвращает кортеж (<Количество заказов>, <Общая сумма>)

STATE = 6 - статус заказа == закрыт
ISTATION = 15033 == касса кафе Смайл
SIFR NOT IN (1164191, 1164182, 1163446) == исключаем валюты Probonus (ими оплачивают корпоративные обеды)
"""

RK_SMILE_TOTAL_SUM = """
    SELECT COUNT(ORIGINALSUM) AS total_count, SUM(ORIGINALSUM) AS total_sum
    FROM [RK7].[dbo].[PAYMENTS]
    WHERE [SIFR] NOT IN (1164191, 1164182, 1163446) AND [STATE] = 6 AND [ISTATION] = 15033 AND [VISIT] IN (
        SELECT [SIFR]
        FROM [RK7].[dbo].[VISITS]
        WHERE [QUITTIME] >= '{date_from}' AND [QUITTIME] < '{date_to}'
    )
"""
