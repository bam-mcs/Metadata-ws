CREATE TABLE [dbt_gold].[dim_date] (

	[date_id] varchar(400) NULL, 
	[calendar_date] date NULL, 
	[year] int NULL, 
	[month] int NULL, 
	[day_of_month] int NULL, 
	[day_name] varchar(10) NULL, 
	[month_name] varchar(10) NULL
);