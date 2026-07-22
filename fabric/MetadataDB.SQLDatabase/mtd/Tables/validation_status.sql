CREATE TABLE [mtd].[validation_status] (
    [validation_id]       INT           NOT NULL,
    [source_type]         VARCHAR (20)  NOT NULL,
    [source_item_name]    VARCHAR (100) NOT NULL,
    [target_item_name]    VARCHAR (150) NOT NULL,
    [process_stage]       VARCHAR (100) NOT NULL,
    [validation_category] VARCHAR (100) NOT NULL,
    [validation_scope]    VARCHAR (100) NOT NULL,
    [validation_criteria] VARCHAR (500) NOT NULL,
    [validation_status]   VARCHAR (20)  NOT NULL,
    [validation_datetime] DATETIME2 (0) NOT NULL,
    FOREIGN KEY ([validation_id]) REFERENCES [mtd].[validation_config] ([validation_id])
);


GO

