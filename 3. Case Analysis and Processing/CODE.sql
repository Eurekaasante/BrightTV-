----------------
--1. PEFROM THE JOIN
------------------

SELECT  A.UserID,
        A.Gender,
        A.Age,
        A.Race,.
        A.Province,
        B.Channel2,
        B.RecordDate2,
        B.`Duration 2`

FROM brightcase2.bright_c2.userpro AS A 
LEFT JOIN brightcase2.bright_c2.views AS B
ON A.UserID=B.UserID0;






--The table was joined with the left join table using the UserID column with selected columns 
------------------------------------------------
--2. CREATING A NEW TABLE FOR FURTHER ANALYSIS
-----------------------------------------------
CREATE OR REPLACE TABLE brightcase2.bright_c2.JointBTV AS
SELECT  A.UserID,
        A.Gender,
        A.Age,
        A.Race, 
        A.Province,
        B.Channel2,
        B.RecordDate2,
        B.`Duration 2` AS Duration2

FROM brightcase2.bright_c2.userpro AS A 
LEFT JOIN brightcase2.bright_c2.views AS B
ON A.UserID=B.UserID0;






------------------------------------
--3. EXPLORE THE NEWLY FORMED TABLE
------------------------------------
SELECT*
FROM brightcase2.bright_c2.jointbtv;




--The 8 column table has been created with UserID< Gender, race, Province, Channel2, RecordDate2 and Duratn2.

------------------------------------
--4. CHECKNG THE NULLS
------------------------------------
SELECT COUNT(*)
FROM brightcase2.bright_c2.jointbtv
WHERE UserID IS NULL 
   OR Age IS NULL
   OR Province IS NULL
   OR Race IS NULL
   OR Channel2 IS NULL
   OR Gender IS NULL
   OR RecordDate2 IS NULL
   OR Duration2 IS NULL;




--There are 989 NULLS spreadoit across diffeent columns of the data set

-----------------------------------------------------
--5. DATA ENRICHMENT WITH TEMPORAL AND CATEGORICAL BUCKETING
------------------------------------------------------


SELECT*,
    -- 1. TEMPORAL ENRICHMENT
    CAST(RecordDate2 AS DATE) AS Activity_Date,
    MonthName(CAST(RecordDate2 AS DATE)) AS Month_Name, 
    dayname(CAST(RecordDate2 AS DATE)) AS DayOfWeek,       
    date_format(CAST(RecordDate2 AS DATE), 'EEEE') AS FullDayName,
    hour(CAST(RecordDate2 AS TIMESTAMP)) AS HourOfDay,   
    
    -- 2. AGE BUCKETING
    CASE 
        WHEN Age < 10 THEN 'Kids'
        WHEN Age BETWEEN 11 AND 17 THEN 'Youth'
        WHEN Age BETWEEN 18 AND 34 THEN 'Young_Adult'
        WHEN Age BETWEEN 35 AND 50 THEN 'Mature_Adults'
        WHEN Age > 50 THEN '50+ Senior'
        ELSE 'Unknown'
    END AS AgeGroup,

    -- 3. DURATION BUCKETING (Binned by Minutes)
    CASE 
        WHEN TRY_CAST(Duration2 AS FLOAT) < 60 THEN 'Short (<1m)'
        WHEN TRY_CAST(Duration2 AS FLOAT) BETWEEN 60 AND 1800 THEN 'Medium (1-30m)'
        WHEN TRY_CAST(Duration2 AS FLOAT) > 1800 THEN 'Long (>30m)'
        ELSE 'Invalid Duration'
    END AS ViewType,

    -- 4. DATE DIFFERENCES (Recency & Loyalty)
    datediff(current_date(), CAST(RecordDate2 AS DATE)) AS DaysSinceRecord,
    months_between(current_date(), CAST(RecordDate2 AS DATE)) AS MonthsSinceRecord,

    -- 5. TIME OF DAY BUCKETING
    CASE 
        WHEN hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 17 AND 21 THEN 'Evening/Prime'
        WHEN hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 22 AND 23 
             OR hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 0 AND 5 THEN 'Night'
        ELSE 'Unknown'
    END AS TimeOfDay

FROM brightcase2.bright_c2.jointbtv2;



--After several attempts to coalesce with no success the trimming function was nested within the code to trim the spaces and human errors in the data, then nullif and finally the coalesce function.  The data is ready for further analysis. Additional function had to be done from the Gender to remove the charecters which the TRIM function missed.

--------------------------------------------
--6. CREATE CLEANED TABLE FOR FURTHER EXPLORATIONS
--------------------------------------------

CREATE OR REPLACE TABLE brightcase2.bright_c2.jointbtv2
SELECT 
    UserID,
    -- TRIM removes spaces, LOWER handles 'None' or 'NONE'
   COALESCE(NULLIF(REGEXP_REPLACE(LOWER(Gender), '[\\s\\p{Z}]', ''), -- Strips ALL hidden spaces/tabs
            'none'), 'Unknown') AS Gender,
    COALESCE(NULLIF(Age, 0), 0) AS Age,
    COALESCE(NULLIF(TRIM(LOWER(Race)), 'none'), 'Unknown') AS Race,
    COALESCE(NULLIF(TRIM(LOWER(Province)), 'none'), 'Unknown') AS Province,
    COALESCE(NULLIF(REGEXP_REPLACE(INITCAP(Channel2), '[\\s\\p{Z}]', ''), ''), 'Unknown') AS Channel2,
    RecordDate2,
    Duration2
FROM brightcase2.bright_c2.jointbtv;   
--NEW TABLE CREATED AS brightcase2.bright_c2.jointbtv2

-------------------------------
--7. FURTHER ANALYSIS
------------------------------

SELECT COUNT(*)
FROM brightcase2.bright_c2.jointbtv2;

--There are 10989 rows cleaned 

SELECT DISTINCT Gender
FROM brightcase2.bright_c2.jointbtv2;  
--three values are retunred from the function as Male, Female and Unknown parsed through.
------------------------------------------------------

--SELECT 
    COALESCE(NULLIF(TRIM(LOWER(Gender)), 'none'), 'Unknown') AS Gender,
    COALESCE(NULLIF(Age, 0), 'Unknown') AS Age,
    COALESCE(NULLIF(TRIM(LOWER(Race)), 'none'), 'Unknown') AS Race,
    COUNT(*) as total_rows
FROM brightcase2.bright_c2.jointbtv
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-----------------------------------------------------

SELECT DISTINCT Province
FROM brightcase2.bright_c2.jointbtv2;  
-- There are 9 provinces with 1 unknown
-----------------------------------------------------

SELECT DISTINCT Race
FROM brightcase2.bright_c2.jointbtv2; 
-- there are 4 races identified and 1 unknown and 1 as other

SELECT DISTINCT Channel2
FROM brightcase2.bright_c2.jointbtv2; 
-- there are 19 channels  and 1 unknown 
---------------------------------------------------------
SELECT DISTINCT Channel2 
FROM brightcase2.bright_c2.jointbtv2 
WHERE Channel2 LIKE 'Sawsee';

---------------------------------------------------------

--checking the MIN and MAX DATE, duration of watching, MIN AND MAX dates
SELECT 
    MIN(CAST(Duration2 AS FLOAT)) AS min_duration,
    MAX(CAST(Duration2 AS FLOAT)) AS max_duration,
    AVG(CAST(Duration2 AS FLOAT)) AS avg_duration
FROM brightcase2.bright_c2.jointbtv2;

---------------------------------------------------------
SELECT 
    MIN(CAST(RecordDate2 AS DATE)) AS start_date,
    MAX(CAST(RecordDate2 AS DATE)) AS end_date
FROM brightcase2.bright_c2.jointbtv2;  
---The start date is 2016-01-2016-03-31 which is 3 months 

DATA ENRICHED CODE FOLLOWS BELOW

SELECT
        UserID,
        Gender,
        Age,
        Race,
        Province,
        Channel2,
        RecordDate2,
        Duration2,
    -- 1. TEMPORAL ENRICHMENT
    CAST(RecordDate2 AS DATE) AS Activity_Date,
    MonthName(CAST(RecordDate2 AS DATE)) AS Month_Name, 
    dayname(CAST(RecordDate2 AS DATE)) AS DayOfWeek,       
       hour(CAST(RecordDate2 AS TIMESTAMP)) AS HourOfDay,
       Duration2 AS Duration,
     (hour(CAST(Duration2 AS TIMESTAMP)) * 3600) + 
    (minute(CAST(Duration2 AS TIMESTAMP)) * 60) + 
    second(CAST(Duration2 AS TIMESTAMP)) AS TotalSeconds_Watched,

   
    -- 2. AGE BUCKETING
    CASE 
        WHEN Age =0 THEN 'Unknown'
        WHEN Age BETWEEN 1 AND 10 THEN 'Kids'
        WHEN Age BETWEEN 11 AND 17 THEN 'Youth'
        WHEN Age BETWEEN 18 AND 34 THEN 'Young_Adult'
        WHEN Age BETWEEN 35 AND 50 THEN 'Mature_Adults'
        WHEN Age BETWEEN 51 AND 150 THEN 'Senior'
        ELSE 'Centenarian'
    END AS AgeGroup,

    -- 3. DURATION BUCKETING (Binned by Minutes)
 CASE 
       
        WHEN TRY_CAST(REGEXP_REPLACE(TotalSeconds_Watched, '[^0-9.]', '') AS FLOAT) <= 60 THEN 'Short'
        WHEN TRY_CAST(REGEXP_REPLACE(TotalSeconds_Watched, '[^0-9.]', '') AS FLOAT) BETWEEN 60.01 AND 1800 THEN 'Medium'
        WHEN TRY_CAST(REGEXP_REPLACE(TotalSeconds_Watched, '[^0-9.]', '') AS FLOAT) > 1801 THEN 'Long'
        ELSE 'Invalid Duration'
    END AS ViewType,

    -- 4. DATE DIFFERENCES (Recency & Loyalty)
    datediff(current_date(), CAST(RecordDate2 AS DATE)) AS DaysSinceRecord,
    months_between(current_date(), CAST(RecordDate2 AS DATE)) AS MonthsSinceRecord,

    -- 5. TIME OF DAY BUCKETING
    CASE 
        WHEN hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 17 AND 21 THEN 'Evening/Prime'
        WHEN hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 22 AND 23 
             OR hour(TRY_CAST(RecordDate2 AS TIMESTAMP)) BETWEEN 0 AND 5 THEN 'Night'
        ELSE 'Unknown'
    END AS TimeOfDay

FROM brightcase2.bright_c2.jointbtv2;
--Enrichd dataset for analysis on Excel
--------------------------------------------
