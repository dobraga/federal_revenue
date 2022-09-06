CREATE TEMPORARY TABLE new_result AS
SELECT "{{.Step}}" AS STEP, TIMESTAMP("{{.UpdatedAt}}") AS UPDATED_AT
;

 MERGE `{{.Project}}.{{.Dataset}}.Bookmark` AS old
 USING new_result
    ON old.STEP = new_result.STEP
  WHEN MATCHED THEN
       UPDATE SET old.UPDATED_AT = new_result.UPDATED_AT
  WHEN NOT MATCHED THEN
       INSERT (
           STEP,
           UPDATED_AT
       ) VALUES (
           new_result.STEP,
           new_result.UPDATED_AT
       )
;
