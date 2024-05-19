CREATE TABLE zerodha_trades (
    symbol VARCHAR(10),
    isin VARCHAR(12),
    trade_date DATE,
    exchange VARCHAR(10),
    segment VARCHAR(10),
    series VARCHAR(5),
    trade_type VARCHAR(5),
    auction BOOLEAN,
    quantity DECIMAL(10, 6),
    price DECIMAL(12, 6),
    trade_id BIGINT,
    order_id BIGINT,
    order_execution_time TIMESTAMP
);


CREATE TABLE zerodha_ledger (
    particulars TEXT,
    posting_date DATE,
    cost_center VARCHAR(50),
    voucher_type VARCHAR(50),
    debit DECIMAL(12, 6),
    credit DECIMAL(12, 6),
    net_balance DECIMAL(12, 6)
);


CREATE TABLE dividends (
    Symbol VARCHAR(10),
    ISIN VARCHAR(12),
    Date DATE,
    Quantity INT,
    Dividend_Per_Share DECIMAL(10, 2),
    Net_Dividend_Amount DECIMAL(12, 2)
);
