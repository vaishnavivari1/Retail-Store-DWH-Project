-- 02_create_staging_and_dw_tables.sql (UPDATED for alphanumeric IDs + loyalty as NVARCHAR)
-- Drop staging table if exists (safe to recreate)
IF OBJECT_ID('stg.SalesRaw', 'U') IS NOT NULL
    DROP TABLE stg.SalesRaw;
GO

CREATE TABLE stg.SalesRaw (
    OrderDate          DATE,
    OrderID            NVARCHAR(50),
    StoreID            NVARCHAR(50),
    CustomerID         NVARCHAR(50),
    ProductID          NVARCHAR(50),
    Quantity           INT,
    OrderAmount        DECIMAL(18,2),
    DiscountAmount     DECIMAL(18,2),
    ShippingCost       DECIMAL(18,2),
    TotalAmount        DECIMAL(18,2),

    -- Store
    StoreName          NVARCHAR(255),
    StoreType          NVARCHAR(100),
    StoreOpeningDate   DATE,
    StoreAddress       NVARCHAR(255),
    StoreCity          NVARCHAR(100),
    StoreState         NVARCHAR(100),
    StoreZipCode       NVARCHAR(20),
    StoreCountry       NVARCHAR(100),
    StoreRegion        NVARCHAR(50),
    StoreManagerName   NVARCHAR(255),

    -- Customer
    FirstName          NVARCHAR(100),
    LastName           NVARCHAR(100),
    Gender             NVARCHAR(20),
    DOB                DATE,
    Email              NVARCHAR(255),
    CustomerAddress    NVARCHAR(255),
    CustomerCity       NVARCHAR(100),
    CustomerState      NVARCHAR(100),
    CustomerZipCode    NVARCHAR(20),
    CustomerCountry    NVARCHAR(100),

    -- Product
    ProductName        NVARCHAR(255),
    Category           NVARCHAR(100),
    Brand              NVARCHAR(100),
    UnitPrice          DECIMAL(18,2),

    -- Loyalty Program details (denormalized in staging)
    LoyalityProgramID  NVARCHAR(50),
    ProgramName        NVARCHAR(255),
    TierLevel          NVARCHAR(50),
    PointsMultiplier   DECIMAL(9,2),
    AnnualFee          DECIMAL(18,2),

    -- Lineage
    SourceFile         NVARCHAR(255)
);
GO
