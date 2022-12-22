-- Show unemployment rate by month & year.
SELECT
    DATEPART(year, [DATE]) AS YEAR,
    DATEPART(month, [DATE]) AS MONTH,
    [UNRATE]

FROM
    [workspace].[dbo].[FRED - Unemployment]

WHERE
    DATEPART(year, [DATE]) >= 1950;