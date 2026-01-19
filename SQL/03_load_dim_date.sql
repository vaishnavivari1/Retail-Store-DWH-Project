-- 03_load_dim_date.sql
-- Populate dw.DimDate using date range found in stg.SalesRaw
-- Create table if not exists
IF OBJECT_ID('dw.DimDate', 'U') IS NULL
BEGIN
    CREATE TABLE dw.DimDate (
        DateKey     INT         NOT NULL PRIMARY KEY, -- YYYYMMDD
        FullDate    DATE        NOT NULL,
        [Year]      INT         NOT NULL,
        [Month]     INT         NOT NULL,
        MonthName   NVARCHAR(20) NOT NULL,
        Day         INT         NOT NULL,
        DayOfWeek   INT         NOT NULL, -- 1=Sunday..7=Saturday per DATEPART(dw)
        DayName     NVARCHAR(20) NOT NULL,
        Quarter     INT         NOT NULL,
        IsWeekend   BIT         NOT NULL
    );
END
GO

TRUNCATE TABLE dw.DimDate;


-- Determine min/max date from staging
DECLARE @minDate DATE, @maxDate DATE;
SELECT @minDate = MIN(OrderDate), @maxDate = MAX(OrderDate) FROM stg.SalesRaw;
IF @minDate IS NULL BEGIN
    RAISERROR ('stg.SalesRaw contains no OrderDate values; DimDate load aborted.', 16, 1);
    RETURN;
END

-- Expand a calendar between min and max (inclusive)
;WITH cte_dates AS (
    SELECT @minDate AS d
    UNION ALL
    SELECT DATEADD(day, 1, d) FROM cte_dates WHERE d < @maxDate
)
-- Insert dates that do not already exist
INSERT INTO dw.DimDate (DateKey, FullDate, [Year], [Month], MonthName, Day, DayOfWeek, DayName, Quarter, IsWeekend)
SELECT
    CONVERT(INT, FORMAT(d, 'yyyyMMdd')) AS DateKey,
    d AS FullDate,
    DATEPART(year, d) AS [Year],
    DATEPART(month, d) AS [Month],
    DATENAME(month, d) AS MonthName,
    DATEPART(day, d) AS Day,
    DATEPART(weekday, d) AS DayOfWeek,
    DATENAME(weekday, d) AS DayName,
    DATEPART(quarter, d) AS Quarter,
    CASE WHEN DATEPART(weekday, d) IN (1,7) THEN 1 ELSE 0 END AS IsWeekend
FROM cte_dates d
WHERE NOT EXISTS (SELECT 1 FROM dw.DimDate dd WHERE dd.FullDate = d)
OPTION (MAXRECURSION 0);
GO
