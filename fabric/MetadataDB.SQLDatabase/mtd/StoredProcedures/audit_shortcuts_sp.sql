
CREATE PROCEDURE [mtd].[audit_shortcuts_sp] 
@shortcut_name VARCHAR(250),
@shortcut_path VARCHAR(500),
@source_type VARCHAR(150),
@target_lakehouse_name VARCHAR(150),
@medallion_layer VARCHAR(50),
@onelake_path VARCHAR(150) = NULL,
@source_item_id VARCHAR(50) = NULL,
@source_item_name VARCHAR(150) = NULL,
@source_item_type VARCHAR(50) = NULL,
@source_workspace_id VARCHAR(50) = NULL,
@source_workspace_name VARCHAR(150) = NULL,
@subpath VARCHAR(150) = NULL,
@shortcut_audit_refreshtime VARCHAR(50)
AS
BEGIN
SET NOCOUNT ON
INSERT INTO mtd.shortcut_audit (
    shortcut_name,
    shortcut_path,
    source_type,
    target_lakehouse_name,
    medallion_layer,
    onelake_path,
    source_item_id,
    source_item_name,
    source_item_type,
    source_workspace_id,
    source_workspace_name,
    subpath,
    shortcut_audit_refreshtime
)
VALUES (
    @shortcut_name,
    @shortcut_path,
    @source_type,
    @target_lakehouse_name,
    @medallion_layer,
    @onelake_path,
    @source_item_id,
    @source_item_name,
    @source_item_type,
    @source_workspace_id,
    @source_workspace_name,
    @subpath,
    @shortcut_audit_refreshtime
)
END

GO

