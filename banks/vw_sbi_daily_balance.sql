DROP VIEW vw_sbi_bank_balance;

CREATE View vw_sbi_bank_balance as

with recursive cte as 
(
SELECT
	dt.date
	,b.Debit
	,b.Credit
	,b.Balance
	
,
	ROW_NUMBER() over(
ORDER by
	dt.Date) as rn
from
	date_table dt
left join sbi_bank b 
	on
	dt.Date = b.Date
where
	dt.Date < DATE('now')
order BY
	dt.Date),

recursivefill as
(
select
		date
		,Debit
		,Credit
		,Balance
		,rn
from
	cte
where 
		rn = 1
UNION ALL
SELECT
	c.Date,
	c.Debit,
	c.Credit,
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
	, COALESCE ( Debit ,0) as Debit
	, COALESCE ( Credit ,0) as Credit
	,COALESCE (balance,	0) as sbi_balance
from
	recursivefill ;