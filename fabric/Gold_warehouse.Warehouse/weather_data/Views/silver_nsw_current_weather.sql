-- Auto Generated (Do not modify) A23F943D90ACF4445B71CB2DCA0D5CC4C33A16D96D04A503642138BE01D50240
CREATE VIEW weather_data.silver_nsw_current_weather as
SELECT [location_name],
			[latitude],
			[longitude],
			[_ingested_at],
			[_run_id],
			[current_time],
			[condition_text],
			[temperature_c_or_f],
			[feels_like_temperature],
			[relative_humidity],
			[uv_index],
			[wind_speed],
			[wind_direction_cardinal],
			[precipitation_probability_pct],
			[cloud_cover_pct]
FROM [Silver].[silver_nsw_current_weather].[silver_nsw_current_weather]