-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT * 
FROM world_layoffs.layoffs;


CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging 
SELECT * 
FROM layoffs;


-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates


SELECT *
FROM world_layoffs.layoffs
;


WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions ORDER BY (SELECT NULL)) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM CTE
WHERE row_num > 1;


--الطريقة الاولة

WITH CTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions ORDER BY (SELECT NULL)) AS row_num
    FROM layoffs_staging
)
DELETE FROM layoffs_staging
WHERE (company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) IN (
    SELECT company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions
    FROM CTE
    WHERE row_num > 1;
    
	

--الطريقة الثانية

CREATE TABLE layoffs_staging_unique LIKE layoffs_staging;

INSERT INTO layoffs_staging_unique (company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
SELECT DISTINCT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
FROM layoffs_staging;

SELECT *
FROM layoffs_staging_unique;


DROP TABLE layoffs_staging;

ALTER TABLE layoffs_staging_unique RENAME TO layoffs_staging;







-- 2. Standardize Data

/*
1 نقوم بحذف الفواصل وا الفرغات 
2 نقوم بتعتديل التاريخ 

*/
select company,trim(company)
from layoffs_staging;

UPDATE layoffs_staging_unique 
set company = trim(companY);



SELECT *
FROM layoffs_staging;
where industry; 

select *
from layoffs_staging
where industry like 'crypto%';

update layoffs_staging
set industry = 'crypto'
where industry like 'crypto%';



select *
from layoffs_staging 
where country like 'United states%';

UPDATE layoffs_staging
set country= trim(TRAILING '.' from country)
where country like 'United states%';

UPDATE layoffs_staging
set country= trim(country);




--لتعديل التاريخ 

SELECT `date`
FROM layoffs.layoffs;

SELECT*
STR_TO_DATE(`date`, '%m/%d/%Y');
FROM layoffs.layoffs;

UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');


ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;








-- 3. Look at null values and see what 
SELECT *
FROM world_layoffs.layoffs_staging
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


SELECT *
FROM world_layoffs.layoffs_staging
WHERE company LIKE 'Bally%';

SELECT *
FROM world_layoffs.layoffs_staging
WHERE company LIKE 'airbnb%';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


select t1.industry, t2.industry
from layoffs_staging t1
join layoffs_staging t2
on t1.company =t2.company
where (t1.industry is null or t1.industry = ' ')
and t2.industry is not null ;


UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

select *
from layoffs_staging
where total_laid_off is null
and percentage_laid_off is null;

delete 
from layoffs_staging
where total_laid_off is null
and percentage_laid_off is null;



-- 4. remove any columns and rows we need to
SELECT * 
FROM layoffs_staging;

ALTER TABLE layoffs_staging
DROP COLUMN row_num;


SELECT * 
FROM layoffs_staging;
