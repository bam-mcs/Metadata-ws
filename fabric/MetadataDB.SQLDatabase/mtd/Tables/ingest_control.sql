CREATE TABLE [mtd].[ingest_control] (
    [control_id]              INT            IDENTITY (1, 1) NOT NULL,
    [source_type]             VARCHAR (20)   NOT NULL,
    [source_container_name]   VARCHAR (50)   NULL,
    [source_folder_path]      VARCHAR (100)  NULL,
    [source_database_name]    VARCHAR (100)  NULL,
    [source_schema_name]      VARCHAR (50)   NULL,
    [source_table_name]       VARCHAR (100)  NULL,
    [source_query]            VARCHAR (MAX)  NULL,
    [source_column_list]      VARCHAR (1000) NULL,
    [source_watermark_column] VARCHAR (100)  NULL,
    [source_cutoff_time]      DATETIME2 (6)  NULL,
    [target_object]           VARCHAR (100)  NOT NULL,
    [target_table_name]       VARCHAR (100)  NULL,
    [load_type]               VARCHAR (100)  NOT NULL,
    [fabric_store]            VARCHAR (50)   NOT NULL,
    [enable_flag]             INT            NOT NULL
);


GO

