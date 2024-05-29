-- First, drop the view if it exists
DROP VIEW IF EXISTS vw_zerodha_buy;

-- Then, create the view
CREATE VIEW vw_zerodha_buy AS
select 
	symbol 
	,trade_date
	--, case when trade_type = 'buy' then 
	,quantity as buy_quantity
	,price as buy_price
	
	--,trade_value
	, ROW_NUMBER() over ( PARTITION by symbol order by trade_date) as buy_execution_order
FROM zerodha z
where trade_type = 'buy' order by trade_date ASC ;
