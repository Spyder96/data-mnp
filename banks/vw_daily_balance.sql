SELECT * from date_table dt  
	 left join au_bank ab 
	on dt.Date = ab.Date 
order BY Date




WITH CTE AS (
    SELECT
        dt.Date,
        COALESCE(ab.Balance, LAG(ab.Balance) OVER (ORDER BY dt.Date)) AS Balance
    FROM
        date_table dt
    LEFT JOIN
        au_bank ab ON dt.Date = ab.Date
    ORDER BY
        dt.Date
)
SELECT
    Date,
    Balance
FROM
    CTE;
