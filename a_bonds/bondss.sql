WITH bond_buy AS (SELECT 
	bd.ISIN 
	, bd."Bond Name" 
	, bd."Maturity Date" 
	, btf."Number Of Units" 
	, btf."Invested Amount" 
	, btf."Date of Investment" 
	, btf.Account 
FROM bonds_dim bd right outer join bond_transactions_fact btf on bd.ISIN = btf."ISIN Code" ) ,


bond_repayment AS 
(SELECT 
	brf."Bond ID" AS ISIN
	, bd."Bond Name" 
	, brf.Dates AS repayment_date
	, brf."Interest payment" 
	, brf."Principal Repaid"  
	, brf."Interest payment" + brf."Principal Repaid" AS total_payment_received
	
FROM 
bonds_repayment_fact brf LEFT OUTER JOIN bonds_dim bd 
	ON brf."Bond ID" = bd.ISIN )
	
	
SELECT  
	rp.ISIN
	, rp."Bond Name"
	, rp.repayment_date
	, rp."Interest payment"
	, rp."Principal Repaid"
	, rp."total_payment_received"
	, buy."Number Of Units"
	, buy."Invested Amount"
	, buy."Date of Investment"
	, buy.Account
FROM bond_repayment rp
JOIN 
bond_buy buy ON rp.ISIN = buy.ISIN
ORDER BY repayment_date asc