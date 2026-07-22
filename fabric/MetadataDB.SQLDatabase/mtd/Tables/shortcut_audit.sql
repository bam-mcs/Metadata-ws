CREATE TABLE [mtd].[shortcut_audit] (
    [shortcut_id]                INT           IDENTITY (1, 1) NOT NULL,
    [shortcut_name]              VARCHAR (250) NULL,
    [shortcut_path]              VARCHAR (500) NULL,
    [source_type]                VARCHAR (150) NULL,
    [target_lakehouse_name]      VARCHAR (150) NULL,
    [medallion_layer]            VARCHAR (50)  NULL,
    [onelake_path]               VARCHAR (150) NULL,
    [source_item_id]             VARCHAR (50)  NULL,
    [source_item_name]           VARCHAR (150) NULL,
    [source_item_type]           VARCHAR (50)  NULL,
    [source_workspace_id]        VARCHAR (50)  NULL,
    [source_workspace_name]      VARCHAR (150) NULL,
    [subpath]                    VARCHAR (150) NULL,
    [shortcut_audit_refreshtime] VARCHAR (50)  NULL
);


GO

