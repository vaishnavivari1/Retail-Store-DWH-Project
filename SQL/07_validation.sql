-- Basic validation queries for quick checks
SELECT 'stg_count' AS metric, COUNT(1) AS value FROM stg.SalesRaw;
GO
SELECT 'fact_count' AS metric, COUNT(1) AS value FROM dw.FactSales;
GO
SELECT 'distinct_customers' AS metric, COUNT(DISTINCT CustomerID) AS value FROM stg.SalesRaw;
GO
SELECT 'distinct_products' AS metric, COUNT(DISTINCT ProductID) AS value FROM stg.SalesRaw;
GO