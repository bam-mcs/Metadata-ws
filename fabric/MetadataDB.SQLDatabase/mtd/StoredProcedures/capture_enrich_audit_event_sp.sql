
CREATE PROCEDURE [mtd].[capture_enrich_audit_event_sp] 
@source_type VARCHAR(20),
@event_run_id VARCHAR(50),
@event_activity_run_id VARCHAR(50) = NULL,
@item_name VARCHAR(150),
@data_read bigint,
@data_written bigint,
@files_read INT = NULL,
@files_written INT = NULL,
@rows_read BIGINT = NULL,
@rows_written BIGINT = NULL,
@data_consistency_verification VARCHAR(50) = NULL,
@copy_duration integer,
@event_start_time DATETIME2(7),
@event_end_time DATETIME2(7),
@source_cutoff_time DATETIME2(7) = NULL,
@load_type VARCHAR(100),
@status VARCHAR(20),
@event_triggered_by VARCHAR(20),
@error_details VARCHAR(1500) = NULL,
@pipeline_url VARCHAR(500) = NULL,
@spark_monitoring_url VARCHAR(500) = NULL
AS
BEGIN
SET NOCOUNT ON
INSERT INTO mtd.enrich_audit (
  source_type,
  event_run_id,
  event_activity_run_id,
  item_name,
  data_read,
  data_written,
  files_read,
  files_written,
  rows_read,
  rows_written,
  data_consistency_verification,
  copy_duration,
  event_start_time,
  event_end_time,
  source_cutoff_time,
  load_type,
  status,
  event_triggered_by,
  error_details,
  pipeline_url,
  spark_monitoring_url
)
VALUES                      (
    @source_type
  , @event_run_id
  , @event_activity_run_id
  , @item_name
  , @data_read
  , @data_written
  , @files_read
  , @files_written
  , @rows_read
  , @rows_written
  , @data_consistency_verification
  , @copy_duration
  , @event_start_time
  , @event_end_time
  , @source_cutoff_time
  , @load_type
  , @status
  , @event_triggered_by
  , @error_details
  , @pipeline_url
  , @spark_monitoring_url
)
END

GO

