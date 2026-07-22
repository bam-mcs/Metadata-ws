CREATE TABLE [mtd].[transformation_config] (
    [transformation_id]            INT             IDENTITY (1, 1) NOT NULL,
    [control_id]                   INT             NOT NULL,
    [process_stage]                VARCHAR (100)   NULL,
    [transformation_rule]          VARCHAR (500)   NULL,
    [transformation_type]          VARCHAR (50)    NULL,
    [transformation_rule_criteria] NVARCHAR (1000) NULL,
    [enable_flag]                  INT             NOT NULL
);


GO

