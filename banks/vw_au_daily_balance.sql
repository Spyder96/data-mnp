CREATE View vw_au_bank_balance as

with recursive cte as 
(
SELECT
	dt.date
,
	ab.Balance
	--,COALESCE (ab.Balance,0) as au_balance
,
	ROW_NUMBER() over(
ORDER by
	dt.Date) as rn
from
	date_table dt
left join au_bank ab 
	on
	dt.Date = ab.Date
where
	dt.Date < DATE('now')
order BY
	dt.Date),

recursivefill as
(
select
		date
		,
	Balance
		,
	rn
from
	cte
where 
		rn = 1
UNION ALL
SELECT
	c.Date,
	COALESCE(c.Balance,
	rf.Balance),
	c.rn
FROM
	CTE c
JOIN
        RecursiveFill rf ON
	c.rn = rf.rn + 1
)


select 
	date 
	,
	COALESCE (balance,	0) as au_balance
from
	recursivefill ;