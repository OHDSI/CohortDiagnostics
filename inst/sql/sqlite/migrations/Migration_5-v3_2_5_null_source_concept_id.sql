-- Set cohort name to larger field value
{DEFAULT @migration = migration}
{DEFAULT @included_source_concept = included_source_concept}
{DEFAULT @table_prefix = ''}

-- FSQLITE : ALTER TABLE @database_schema.@table_prefix@included_source_concept ALTER COLUMN source_concept_id BIGINT;
ALTER TABLE @database_schema.@table_prefix@included_source_concept RENAME TO _included_source_concept_old;
CREATE TABLE @database_schema.@included_source_concept (
			database_id VARCHAR NOT NULL,
			cohort_id BIGINT NOT NULL,
			concept_set_id INT NOT NULL,
			concept_id BIGINT NOT NULL,
			source_concept_id BIGINT,
			concept_subjects BIGINT NOT NULL,
			concept_count BIGINT NOT NULL,
			PRIMARY KEY(database_id, cohort_id, concept_set_id, concept_id)
);

INSERT INTO @database_schema.@table_prefix@included_source_concept (
        database_id,
        cohort_id,
        concept_set_id,
        concept_id,
        source_concept_id,
        concept_subjects,
        concept_count
) SELECT  database_id,
        cohort_id,
        concept_set_id,
        concept_id,
        source_concept_id,
        concept_subjects,
        concept_count
FROM _included_source_concept_old;

DROP TABLE _included_source_concept_old;
