-- Data Cleaning

select *
from layoffs;


-- 1. remove duplicates
-- 2. standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns not needed

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- remove duplicates

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select *
from layoffs_staging2;

SELECT `layoffs_staging2`.`company`,
    `layoffs_staging2`.`location`,
    `layoffs_staging2`.`industry`,
    `layoffs_staging2`.`total_laid_off`,
    `layoffs_staging2`.`percentage_laid_off`,
    `layoffs_staging2`.`date`,
    `layoffs_staging2`.`stage`,
    `layoffs_staging2`.`country`,
    `layoffs_staging2`.`funds_raised_millions`
FROM `world_layoffs`.`layoffs_staging2`;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) as row_num
from layoffs;

delete 
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2
where company = 'oda';


-- Standardizing data

select company, (trim(company))
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';
 
select distinct country
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united states%';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');   # changing from text to date

select date
from layoffs_staging2;

alter TABLE layoffs_staging2
modify column `date` date;

update layoffs_staging2 
set industry = null
where industry = '';


select *
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
where t1.industry is null
and t2.industry is not null;

-- Populating null vaue

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null  # This would have worked for my data but it doesnt have another that is not null to fill the one that is null, so i will delete them
and t2.industry is not null;

select *
from layoffs_staging2
where industry is null;

-- deleting rows and columns

DELETE
FROM layoffs_staging2
where industry is null;

select company, industry
from layoffs_staging2
where industry is null;

SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

DELETE
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;











