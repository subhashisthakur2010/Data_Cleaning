-- Data Cleaning: a way of optimizing the data in such a way that it is more usable in the visualization or other products.
-- There'll be 4 stages of the process: removing duplicates, standerize data, removing NULL/blank values and removing columns (if needed).


-- Stage 1 : Removing Duplicates

select *
from layoffs;

-- creating a staging table like the original table so that it does not alter the original values
create table layoffs_staging
like layoffs;

select *
from layoffs_staging;


-- To copy all the values from the original table to the staging table
insert layoffs_staging
select *
from layoffs;


-- In this step we create a CTE to segregate duplicate values. We will use a window function row_number() to create a separate column.
-- The separate column will help us segregate the values. 
with duplicate_cte as
(select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', 
stage, country, funds_raised_millions) as row_num
from layoffs_staging)

select *
from layoffs_staging
where company = 'casper'; 


-- Since we can not do any changes in the CTE, we will need another staging table called 'layoffs_staging2'. 
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select *
from layoffs_staging2;


-- Inserting everything to this new staging table (i.e. 'layoffs_staging2')
insert into layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', 
stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- Deleting the duplicate values
select *
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;



-- Stage 2: Standerzing the data

select *
from layoffs_staging2;

-- Triming extra spaces
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);


-- Fixing the errors in the industry column
select distinct industry
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';


-- Fixing the errors in the country column
select distinct country
from layoffs_staging2;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';


-- Changing the datatype of the date column into the proper date format
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select `date`
from layoffs_staging2;

-- NOTE: Alter table should never be used on the original table, as using it in the staging table leaves the room for mistakes
alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;


-- Stage 3: Removing the NULLs/Blanks

select *
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

-- replacing the NULL values with blank
update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

select t1.industry, t2.industry 
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


update layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;


select *
from layoffs_staging2
where company like 'Bally%';

-- This is the only company that has a null for the industry, that we can not change.

-- The rest of the value that are still null (i.e. total_laid_off, percentage_laid_off etc), we can not populate them because 
-- enough data is not available, for instance if we had the total number of employees, we could have possibly populated those.



-- stage 4 (Removing rows and columns which are unnecessary)

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- Before deletion of any column or row we have to be sure of the fact that it does not affect the visualization of the table
delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;


select *
from layoffs_staging2;


