-- 04_load_dimensions.sql (UPDATED to use NVARCHAR keys for IDs)

-- 4.1 Loyalty Program (unique list)
IF OBJECT_ID('dw.DimLoyaltyProgram','U') IS NULL
BEGIN
    CREATE TABLE dw.DimLoyaltyProgram (
        LoyalityProgramID   NVARCHAR(50) NOT NULL PRIMARY KEY,
        ProgramName        NVARCHAR(255),
        TierLevel          NVARCHAR(50),
        PointsMultiplier   DECIMAL(9,2),
        AnnualFee          DECIMAL(18,2)
    );
END
GO

TRUNCATE TABLE dw.DimLoyaltyProgram;
INSERT INTO dw.DimLoyaltyProgram (LoyalityProgramID, ProgramName, TierLevel, PointsMultiplier, AnnualFee)
SELECT DISTINCT
    NULLIF(LoyalityProgramID, '') AS LoyalityProgramID,
    ProgramName,
    TierLevel,
    PointsMultiplier,
    AnnualFee
FROM stg.SalesRaw
WHERE NULLIF(LoyalityProgramID, '') IS NOT NULL;
GO

-- 4.2 Customer dimension
IF OBJECT_ID('dw.DimCustomer','U') IS NULL
BEGIN
    CREATE TABLE dw.DimCustomer (
        CustomerID         NVARCHAR(50) NOT NULL PRIMARY KEY,
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
        LoyalityProgramID   NVARCHAR(50),
        FOREIGN KEY (LoyalityProgramID) REFERENCES dw.DimLoyaltyProgram(LoyalityProgramID)
    );
END
GO

TRUNCATE TABLE dw.DimCustomer;
;WITH ranked AS (
    SELECT
        COALESCE(NULLIF(CustomerID,''), '') AS CustomerID,
        FirstName, LastName, Gender, DOB, Email,
        CustomerAddress, CustomerCity, CustomerState, CustomerZipCode, CustomerCountry,
        NULLIF(LoyalityProgramID,'') AS LoyalityProgramID,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(NULLIF(CustomerID,''), '') ORDER BY OrderDate DESC, OrderID DESC) AS rn
    FROM stg.SalesRaw
    WHERE COALESCE(NULLIF(CustomerID,''), '') <> ''
)
INSERT INTO dw.DimCustomer (
    CustomerID, FirstName, LastName, Gender, DOB, Email,
    CustomerAddress, CustomerCity, CustomerState, CustomerZipCode, CustomerCountry, LoyalityProgramID
)
SELECT CustomerID, FirstName, LastName, Gender, DOB, Email,
       CustomerAddress, CustomerCity, CustomerState, CustomerZipCode, CustomerCountry, LoyalityProgramID
FROM ranked WHERE rn = 1;
GO

-- 4.3 Product dimension
IF OBJECT_ID('dw.DimProduct','U') IS NULL
BEGIN
    CREATE TABLE dw.DimProduct (
        ProductID  NVARCHAR(50) NOT NULL PRIMARY KEY,
        ProductName NVARCHAR(255),
        Category    NVARCHAR(100),
        Brand       NVARCHAR(100),
        UnitPrice   DECIMAL(18,2)
    );
END
GO

TRUNCATE TABLE dw.DimProduct;

;WITH ranked AS (
    SELECT
        COALESCE(NULLIF(ProductID,''), '') AS ProductID,
        ProductName, Category, Brand, UnitPrice,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(NULLIF(ProductID,''), '') ORDER BY OrderDate DESC, OrderID DESC) AS rn
    FROM stg.SalesRaw
    WHERE COALESCE(NULLIF(ProductID,''), '') <> ''
)
INSERT INTO dw.DimProduct (ProductID, ProductName, Category, Brand, UnitPrice)
SELECT ProductID, ProductName, Category, Brand, UnitPrice
FROM ranked WHERE rn = 1;
GO

-- 4.4 Store dimension
IF OBJECT_ID('dw.DimStore','U') IS NULL
BEGIN
    CREATE TABLE dw.DimStore (
        StoreID NVARCHAR(50) NOT NULL PRIMARY KEY,
        StoreName NVARCHAR(255),
        StoreType NVARCHAR(100),
        StoreOpeningDate DATE,
        StoreAddress NVARCHAR(255),
        StoreCity NVARCHAR(100),
        StoreState NVARCHAR(100),
        StoreZipCode NVARCHAR(20),
        StoreCountry NVARCHAR(100),
        StoreRegion NVARCHAR(50),
        StoreManagerName NVARCHAR(255)
    );
END
GO

TRUNCATE TABLE dw.DimStore;

;WITH ranked AS (
    SELECT
        COALESCE(NULLIF(StoreID,''), '') AS StoreID,
        StoreName, StoreType, StoreOpeningDate, StoreAddress, StoreCity, StoreState, StoreZipCode, StoreCountry, StoreRegion, StoreManagerName,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(NULLIF(StoreID,''), '') ORDER BY OrderDate DESC, OrderID DESC) AS rn
    FROM stg.SalesRaw
    WHERE COALESCE(NULLIF(StoreID,''), '') <> ''
)
INSERT INTO dw.DimStore (
    StoreID, StoreName, StoreType, StoreOpeningDate, StoreAddress, StoreCity, StoreState, StoreZipCode, StoreCountry, StoreRegion, StoreManagerName
)
SELECT StoreID, StoreName, StoreType, StoreOpeningDate, StoreAddress, StoreCity, StoreState, StoreZipCode, StoreCountry, StoreRegion, StoreManagerName
FROM ranked WHERE rn = 1;
GO
