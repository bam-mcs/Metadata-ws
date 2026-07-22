CREATE PROCEDURE [mtd].[insert_serve_control_with_transform_sp]
    @source_type NVARCHAR(100),
    @source_item_name NVARCHAR(200),
    @source_connection_url NVARCHAR(500) = NULL,
    @azure_key_vault NVARCHAR(200) = NULL,
    @azure_key_vault_secret_client_id_name NVARCHAR(200) = NULL,
    @azure_key_vault_secret_client_secret_name NVARCHAR(200) = NULL,
    @target_item_name NVARCHAR(200),  -- e.g. 'silver.dbo.Azure_Policy_Definition_Actions_2'
    @transformation_flag BIT,
    @enable_flag BIT,
    @transformation_rule NVARCHAR(200),
    @transformation_type NVARCHAR(100),
    @transformation_rule_criteria NVARCHAR(1000)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @control_id INT;

    -- Step 1: Insert into serve_control
    INSERT INTO [mtd].[serve_control] (
        source_type, source_item_name, source_connection_url,
        azure_key_vault, azure_key_vault_secret_client_id_name, azure_key_vault_secret_client_secret_name,
        target_item_name, transformation_flag, enable_flag
    )
    VALUES (
        @source_type,
        @source_item_name,
        @source_connection_url,
        @azure_key_vault,
        @azure_key_vault_secret_client_id_name,
        @azure_key_vault_secret_client_secret_name,
        @target_item_name,
        @transformation_flag,
        @enable_flag
    );

    -- Step 2: Capture identity
    SET @control_id = SCOPE_IDENTITY();

    -- Step 3: Insert into transformation_config
    INSERT INTO [mtd].[transformation_config] (
        control_id, process_stage, transformation_rule,
        transformation_type, transformation_rule_criteria, enable_flag
    )
    VALUES (
        @control_id,
        'serve',
        @transformation_rule,
        @transformation_type,
        @transformation_rule_criteria,
        @enable_flag
    );
END;

GO

