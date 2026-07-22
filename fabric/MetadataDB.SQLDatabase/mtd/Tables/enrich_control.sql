CREATE TABLE [mtd].[enrich_control] (
    [control_id]                                INT           IDENTITY (1, 1) NOT NULL,
    [source_type]                               VARCHAR (50)  NULL,
    [source_item_name]                          VARCHAR (100) NULL,
    [source_connection_url]                     VARCHAR (500) NULL,
    [azure_key_vault]                           VARCHAR (50)  NULL,
    [azure_key_vault_secret_client_id_name]     VARCHAR (50)  NULL,
    [azure_key_vault_secret_client_secret_name] VARCHAR (50)  NULL,
    [load_type]                                 VARCHAR (50)  NULL,
    [target_item_name]                          VARCHAR (150) NULL,
    [transformation_flag]                       INT           NOT NULL,
    [enable_flag]                               INT           NOT NULL
);


GO

