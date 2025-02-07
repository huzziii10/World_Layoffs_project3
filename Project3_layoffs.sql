-- Data cleaning

select 
* 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the data 
-- 3. Null values or blank values
-- 4. Remove Any Columns 

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

-- Never use the RAW data for staging or manipulating make a second version
-- to keep the RAW data intact.

INSERT INTO layoffs_staging
SELECT * 
FROM layoffs;

SELECT * 
FROM layoffs_staging

-- New functions coming ROW_NUMBER() & OVER()

SELECT * ,
ROW_NUMBER () OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;

-- CTE starts with WITH

WITH duplicate_cte AS
( 
SELECT * ,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date',stage, country,funds_raised_millions) AS row_num
FROM layoffs_staging
)

SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company='ODA'

SELECT * 
FROM layoffs_staging
WHERE company='Casper';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` bigint DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date',stage, country,funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging2;



-- Standardizing Data

SELECT company, trim(company) 
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company= trim(company) ;

SELECT industry 
FROM layoffs_staging2
ORDER BY 1;

SELECT industry 
FROM layoffs_staging2
order by 1;

SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

 UPDATE layoffs_staging2
 SET industry = 'Crypto'
 WHERE industry = 'Crypto%';
 
 SELECT industry	
 FROM layoffs_staging2
 ORDER BY 1;
 
 UPDATE layoffs_staging2
 SET industry = 'Crypto'
 WHERE industry LIKE 'Crypto%';
 
 SELECT DISTINCT location 
 FROM layoffs_staging2
 ORDER BY 1 ;
 
 SELECT DISTINCT country
 FROM layoffs_staging2
 ORDER BY 1;
 
 
 -- Trailing Options like advanced
 -- TRIM(TRAILING '.' FROM country)
 -- Where country LIKE 'United States%'
 
 
 UPDATE layoffs_staging2
 SET country = 'United States'
 WHERE country LIKE 'United%';
 
 SELECT `date`,
 STR_TO_DATE(`date`, '%m/%d/%Y') 
 FROM layoffs_staging2;
 
 SELECT `date`,
 str_to_date(`date`,'%m,%d,%Y')
 FROM layoffs_staging2;
 
 UPDATE layoffs_staging2
 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') ;
 

-- ONLY USE ALTER TABLE IN STAGING TABLE 
ALTER TABLE layoffs_staging2
MODIFY column `date` DATE;

show databases;
USE world_layoffs;
SELECT database();


-- NULL & BLANK VALUES
SHOW TABLES;
describe layoffs_staging2;
select *
FROM layoffs_staging2;

select *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry= '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Air';

SELECT * 
FROM layoffs_staging2;

select database();
SHOW DATABASES;
USE world_layoffs;


SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry LIKE '';

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT * 
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
WHERE (t1.industry = '' OR t1.industry IS NULL)
AND t2.industry IS NOT  NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE (t1.industry = '' OR t1.industry IS NULL)
AND t2.industry IS NOT  NULL;


SELECT *
FROM layoffs_staging2 
JOIN layoffs_staging2 t2;

SELECT * 
FROM layoffs_staging2;


SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- If the data is not relevant and can't be used then its better to eliminate that data--



SELECT * 
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- Exploratory Data Analysis.

SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;


SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off= 1  
AND COUNTRY LIKE 'United%'  ;

SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off= 1  
AND COUNTRY LIKE 'India%'  
ORDER BY DATE DESC;

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off= 1  
ORDER BY funds_raised_millions DESC;

SELECT company, location, funds_raised_millions 
FROM layoffs_staging2
WHERE percentage_laid_off= 1  
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off) AS GRAND_TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


SELECT MIN(`date`) , MAX(`date`)
FROM layoffs_staging2;


SELECT industry, SUM(total_laid_off) AS GRAND_TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT stage AS STAGE, SUM(total_laid_off) AS GRAND_TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY stage
ORDER BY 1 DESC;

SELECT stage , SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY stage
ORDER BY 1 DESC;


SELECT stage , AVG(percentage_laid_off) 
FROM layoffs_staging2
GROUP BY stage
ORDER BY 1 DESC;


SELECT *
FROM layoffs_staging2;

SELECT substring(`date`, 1,7) AS MONTH, SUM(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`, 1,7) IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC
;


WITH rolling_total AS 
( 
SELECT substring(`date`, 1,7) AS MONTH, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substring(`date`, 1,7) IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC

)
SELECT `MONTH`, SUM(total_off) OVER ( ORDER BY `MONTH`) AS rolling_total
FROM rolling_total;


SELECT company, SUM(total_laid_off) AS GRAND_TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


SELECT company,YEAR (`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company, YEAR (`date`)
ORDER BY company ASC;
;

WITH Company_year  ( company, years, total_laid_off) AS 
(
SELECT company,YEAR (`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company, YEAR (`date`)
) 

SELECT * 
FROM Company_year;	

SHOW databases;

USE world_layoffs;
SELECT DATABASE ();
SELECT CURRENT_USER();

SELECT * 
FROM layoffs_staging2;


SELECT YEAR(`Date`) , SUM(total_laid_off) AS TOTAL_OFF
FROM layoffs_staging2
GROUP BY  YEAR (`Date`)
ORDER BY 1 ASC	;



SELECT SUBSTRING(`Date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`Date`,1,7) IS NOT NULL
GROUP BY SUBSTRING(`Date`,1,7)
ORDER  BY 1 ASC
;

SELECT * 
FROM layoffs_staging2;

WITH rolling_total AS 
(
SELECT SUBSTRING(`Date`,1,7) AS `MONTH`, SUM(total_laid_off) AS TOTAL_OFF
FROM layoffs_staging2
WHERE SUBSTRING(`Date`,1,7) IS NOT NULL
GROUP BY SUBSTRING(`Date`,1,7)
ORDER  BY 1 ASC


)
SELECT `MONTH`, total_off,
SUM(TOTAL_OFF) OVER (ORDER BY `MONTH`) AS ROLLING_TOTAL 
FROM rolling_total;


SELECT company, YEAR (`date`), SUM(total_laid_off) AS TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY company, YEAR (`date`)
ORDER BY 3 DESC;

WITH company_year (Company, Years, Total_Laid_Off)AS 
(
SELECT company, YEAR (`date`), SUM(total_laid_off) AS TOTAL_LAID_OFF
FROM layoffs_staging2
GROUP BY company, YEAR (`date`)
),  Company_Year_Rank AS 
(select *, 
DENSE_RANK() OVER (PARTITION BY Years ORDER BY Total_Laid_Off DESC ) AS RANKING 
FROM company_year
WHERE Years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank 
WHERE RANKING<=5
;

SELECT *
FROM layoffs_staging2;

SELECT Industry, YEAR (`date`) AS Years , SUM(total_laid_off) AS Grand_Total_Laid_Off
FROM layoffs_staging2
GROUP BY industry, YEAR (`date`)
ORDER BY 2 DESC;

WITH Industry_Years (Industry , Years , Grand_Total_Laid_Off) AS 
(
SELECT Industry, YEAR (`date`) AS Years , SUM(total_laid_off) AS Grand_Total_Laid_Off
FROM layoffs_staging2
GROUP BY industry, YEAR (`date`)
),
Industry_Year_Rank  AS (
SELECT * ,
DENSE_RANK() OVER( PARTITION BY  Years ORDER BY Grand_Total_Laid_Off DESC ) AS RANKING
FROM Industry_Years
WHERE years IS NOT NULL

)
SELECT *
FROM Industry_Year_Rank 
WHERE RANKING <=5 ;


SELECT * 
FROM layoffs_staging2;

SELECT country, YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY country, YEAR(`date`)
;

WITH Country_Years (Country, Years, Grand_Total_Laid_Off) AS 
(
SELECT country, YEAR(`date`) AS Years , SUM(total_laid_off) AS Grand_Total_Laid_Off
FROM layoffs_staging2
GROUP BY country, YEAR(`date`)
), 
Country_Ranking AS (
SELECT * ,
DENSE_RANK () OVER (PARTITION BY YEARS ORDER BY Grand_Total_Laid_Off ASC) AS RANKING
FROM Country_Years
WHERE Years AND  Grand_Total_Laid_Off IS NOT NULL

)
SELECT *
FROM Country_Ranking
WHERE RANKING <=5 
;


SELECT * 
FROM layoffs_staging2;