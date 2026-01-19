-- Create indexes after loading data for reporting performance
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactSales_DateKey' AND object_id = OBJECT_ID('dw.FactSales'))
BEGIN
    CREATE INDEX IX_FactSales_DateKey ON dw.FactSales(DateKey);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactSales_StoreID' AND object_id = OBJECT_ID('dw.FactSales'))
BEGIN
    CREATE INDEX IX_FactSales_StoreID ON dw.FactSales(StoreID);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactSales_CustomerID' AND object_id = OBJECT_ID('dw.FactSales'))
BEGIN
    CREATE INDEX IX_FactSales_CustomerID ON dw.FactSales(CustomerID);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FactSales_ProductID' AND object_id = OBJECT_ID('dw.FactSales'))
BEGIN
    CREATE INDEX IX_FactSales_ProductID ON dw.FactSales(ProductID);
END
GO
