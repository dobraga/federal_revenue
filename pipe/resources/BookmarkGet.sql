SELECT
    UPDATED_AT AS UpdatedAt
FROM
    `{{.Project}}.{{.Dataset}}.Bookmark`
WHERE
    STEP = "{{.Step}}"