CREATE TABLE [mtd].[validation_config] (
    [validation_id]       INT           IDENTITY (1, 1) NOT NULL,
    [control_id]          INT           NOT NULL,
    [process_stage]       VARCHAR (100) NULL,
    [validation_category] VARCHAR (100) NULL,
    [validation_scope]    VARCHAR (100) NOT NULL,
    [validation_criteria] VARCHAR (500) NOT NULL,
    [enable_flag]         INT           NOT NULL,
    CONSTRAINT [PK_validation_config] PRIMARY KEY CLUSTERED ([validation_id] ASC)
);


GO

