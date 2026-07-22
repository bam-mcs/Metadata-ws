
CREATE PROCEDURE mtd.insert_validation_status_sp
(
    @validation_id         	INT,
    @source_type           	VARCHAR(20),
    @source_item_name      	VARCHAR(100),
    @target_item_name     	VARCHAR(150),
	@process_stage      	VARCHAR(100),
    @validation_category   	VARCHAR(100),
    @validation_scope      	VARCHAR(100),
    @validation_criteria   	VARCHAR(500),
    @validation_status     	VARCHAR(20)
)
AS
BEGIN
    SET NOCOUNT ON;

    ---------------------------------------------------------------------
    -- Input validation (optional but recommended)
    ---------------------------------------------------------------------
    IF @validation_id IS NULL
    BEGIN
        RAISERROR ('validation_id cannot be NULL', 16, 1);
        RETURN;
    END

    IF NOT EXISTS (
        SELECT 1 
        FROM mtd.validation_config 
        WHERE validation_id = @validation_id
    )
    BEGIN
        RAISERROR ('validation_id does not exist in validation_config', 16, 1);
        RETURN;
    END

    ---------------------------------------------------------------------
    -- Insert into validation_status
    ---------------------------------------------------------------------
    INSERT INTO mtd.validation_status (
        validation_id,
        source_type,
        source_item_name,
        target_item_name,
		process_stage,
        validation_category,
        validation_scope,
        validation_criteria,
        validation_status,
		validation_datetime
    )
    VALUES (
        @validation_id,
        @source_type,
        @source_item_name,
        @target_item_name,
		@process_stage,
        @validation_category,
        @validation_scope,
        @validation_criteria,
        @validation_status,
		SYSUTCDATETIME()
    );
END;

GO

