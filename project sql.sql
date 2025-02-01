-- remove duplicates
-- standarduze data
-- null values
-- remove and columns if not nesessary

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging 
select * from layoffs;

select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off, `date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;


 
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

 
 
 select * from layoffs_staging2;
 
 insert into layoffs_staging2
 select *, row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off, `date`,stage,country,funds_raised_millions) as row_num
 from layoffs_staging;
 
  delete from layoffs_staging2
 where row_num>1;
 
 -- duplicates removed
 
 -- now standardize the data
 
update layoffs_staging2 
set company= trim(company);

select * from layoffs_staging2 
where industry like "Crypto%";
 
 
 update layoffs_staging2
 set industry="Crypto"
 where industry like "Crypto%";
 
 select distinct country
 from layoffs_staging2
 order by 1;
 
 update layoffs_staging2
 set country="United States"
 where country like "United States.";
 
 
 
 select `date`,
 str_to_date(`date`,'%m/%d/%Y ')
 from layoffs_staging2;
 
 update layoffs_staging2
 set `date`=  str_to_date(`date`,'%m/%d/%Y ');
 
 
 alter table layoffs_staging2
 modify column `date` date;
 
 select * from layoffs_staging2 t1
 join layoffs_staging2 t2
  on t1.company=t2.company
  and t1.location=t2.location

where (t1.industry is null or t1.industry="")
and t2.industry is not null;
 
 
 update layoffs_staging2 t1
 join layoffs_staging2 t2 
  on t1.company=t2.company
 set t1.industry=t2.company
 where (t1.industry is null or t1.industry="")
and t2.industry is not null;
 
 select * from layoffs_staging2
 where total_laid_off is null and percentage_laid_off is null;
 
 
 
 delete from layoffs_staging2
 where total_laid_off is null and percentage_laid_off is null;
 
 alter table layoffs_staging2
 drop column row_num;
 
 -- EDL
 select * from layoffs_staging2;
 
select max(total_laid_off)
from layoffs_staging2;

-- companies that laid off all employees
select * from 
layoffs_staging2
where percentage_laid_off=1
order by total_laid_off desc;

-- companies that laid off the maximum
select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- industries worst hit
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

-- year wise
select year(date) as `year`, sum(total_laid_off)
from layoffs_staging2
where year(date) is not null
group by year(date)
order by year(date);

-- country wise

select country, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by country
order by total_laid_off desc;

-- monthwise, total and rolling total

select month(date),year(date), sum(total_laid_off) over(order by month(date),year(date))
from layoffs_staging2
group by month(date),year(date)
order by month(date),year(date);

    

 

 
 
 