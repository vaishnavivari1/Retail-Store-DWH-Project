-- 05_load_fact_sales.sql (UPDATED)

-- Create Fact table if missing. SalesID is identity to auto-generate keys.
IF OBJECT_ID('dw.FactSales','U') IS NULL
BEGIN
    CREATE TABLE dw.FactSales (
    SalesID         BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    DateKey         INT NOT NULL,
    StoreID         NVARCHAR(50),
    CustomerID      NVARCHAR(50),
    ProductID       NVARCHAR(50),
    LoyalityProgramID NVARCHAR(50),
    Quantity        INT,
    OrderAmount     DECIMAL(18,2),
    DiscountAmount  DECIMAL(18,2),
    ShippingCost    DECIMAL(18,2),
    TotalAmount     DECIMAL(18,2),
    SourceFile      NVARCHAR(255),

    CONSTRAINT FK_FactSales_DimDate
        FOREIGN KEY (DateKey) REFERENCES dw.DimDate(DateKey),

    CONSTRAINT FK_FactSales_DimStore
        FOREIGN KEY (StoreID) REFERENCES dw.DimStore(StoreID),

    CONSTRAINT FK_FactSales_DimCustomer
        FOREIGN KEY (CustomerID) REFERENCES dw.DimCustomer(CustomerID),

    CONSTRAINT FK_FactSales_DimProduct
        FOREIGN KEY (ProductID) REFERENCES dw.DimProduct(ProductID),

    CONSTRAINT FK_FactSales_DimLoyalty
        FOREIGN KEY (LoyalityProgramID) REFERENCES dw.DimLoyaltyProgram(LoyalityProgramID)
);
END
GO


-- Truncate fact for full reload (safe in dev); in prod you'd implement incremental loads.
TRUNCATE TABLE dw.FactSales;
GO

-- Insert: join to DimDate to get DateKey
INSERT INTO dw.FactSales (DateKey, StoreID, CustomerID, ProductID, LoyalityProgramID, Quantity, OrderAmount, DiscountAmount, ShippingCost, TotalAmount, SourceFile)
SELECT
    dd.DateKey,
    s.StoreID,
    s.CustomerID,
    s.ProductID,
    s.LoyalityProgramID,
    TRY_CAST(s.Quantity AS INT),
    TRY_CAST(s.OrderAmount AS DECIMAL(18,2)),
    TRY_CAST(s.DiscountAmount AS DECIMAL(18,2)),
    TRY_CAST(s.ShippingCost AS DECIMAL(18,2)),
    TRY_CAST(s.TotalAmount AS DECIMAL(18,2)),
    s.SourceFile
FROM stg.SalesRaw s
JOIN dw.DimDate dd
  ON dd.FullDate = s.OrderDate
-- Optionally filter out rows missing required FK references:
WHERE s.StoreID IS NOT NULL AND s.CustomerID IS NOT NULL AND s.ProductID IS NOT NULL;
GO
