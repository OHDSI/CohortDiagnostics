-- changes incidence_rate.person_years from bigint to float
{DEFAULT @migration = migration}
{DEFAULT @incidence_rate = incidence_rate}
{DEFAULT @table_prefix = ''}

ALTER TABLE @database_schema.@table_prefix@incidence_rate ALTER COLUMN person_years FLOAT;
