CREATE TABLE [dbt_gold].[dim_location] (

	[location_id] varchar(400) NULL, 
	[location_name] varchar(8000) NULL, 
	[latitude] float NULL, 
	[longitude] float NULL
);