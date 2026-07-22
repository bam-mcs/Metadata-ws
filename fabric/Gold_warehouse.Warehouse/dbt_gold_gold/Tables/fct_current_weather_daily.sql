CREATE TABLE [dbt_gold_gold].[fct_current_weather_daily] (

	[location_id] varchar(400) NULL, 
	[date_id] varchar(400) NULL, 
	[location_name] varchar(8000) NULL, 
	[calendar_date] date NULL, 
	[avg_temperature_c] float NULL, 
	[min_temperature_c] float NULL, 
	[max_temperature_c] float NULL, 
	[avg_relative_humidity] int NULL, 
	[avg_wind_speed] float NULL, 
	[max_uv_index] int NULL, 
	[avg_cloud_cover_pct] int NULL, 
	[reading_count] int NULL, 
	[end_of_day_condition_text] varchar(8000) NULL
);