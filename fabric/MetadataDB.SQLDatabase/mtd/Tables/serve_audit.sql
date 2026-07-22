CREATE TABLE [mtd].[serve_audit] (
    [run_id]                        INT            IDENTITY (1, 1) NOT NULL,
    [source_type]                   VARCHAR (20)   NULL,
    [event_run_id]                  VARCHAR (50)   NULL,
    [event_activity_run_id]         VARCHAR (50)   NULL,
    [item_name]                     VARCHAR (150)  NULL,
    [data_read]                     BIGINT         NULL,
    [data_written]                  BIGINT         NULL,
    [files_read]                    INT            NULL,
    [files_written]                 INT            NULL,
    [rows_read]                     BIGINT         NULL,
    [rows_written]                  BIGINT         NULL,
    [data_consistency_verification] VARCHAR (50)   NULL,
    [copy_duration]                 INT            NULL,
    [event_start_time]              DATETIME2 (6)  NULL,
    [event_end_time]                DATETIME2 (6)  NULL,
    [source_cutoff_time]            DATETIME2 (6)  NULL,
    [load_type]                     VARCHAR (100)  NULL,
    [status]                        VARCHAR (20)   NULL,
    [event_triggered_by]            VARCHAR (20)   NULL,
    [error_details]                 VARCHAR (1500) NULL,
    [pipeline_url]                  VARCHAR (500)  NULL,
    [spark_monitoring_url]          VARCHAR (500)  NULL
);


GO

