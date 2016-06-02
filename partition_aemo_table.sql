-- Table: partitioned_power_station_datum

-- DROP TABLE partitioned_power_station_datum;

-- Adapted from https://blog.engineyard.com/2013/scaling-postgresql-performance-table-partitioning


CREATE TABLE partitioned_power_station_datum
(
  id serial NOT NULL,
  duid character varying NOT NULL,
  sample_time timestamp with time zone NOT NULL,
  mega_watt double precision NOT NULL,
  file bigint NOT NULL,
  CONSTRAINT partitioned_power_station_datum_pkey PRIMARY KEY (id),
  CONSTRAINT partitioned_power_station_datum_file_fkey FOREIGN KEY (file)
      REFERENCES aemo_csv_file (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE partitioned_power_station_datum
  OWNER TO aemo;



CREATE OR REPLACE FUNCTION
  public.psdatum_partition_function()
RETURNS TRIGGER AS
  $BODY$
DECLARE
  _tablename text;
  _result record;
BEGIN
  _tablename := 'power_station_datum_'||NEW.duid;

  -- Check if the partition needed for the current record exists
  PERFORM 1
  FROM   pg_catalog.pg_class c
  JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE  c.relkind = 'r'
  AND    c.relname = _tablename;
  -- AND    n.nspname = '';

  -- If the partition needed does not yet exist, then we create it:
  -- Note that || is string concatenation (joining two strings to make one)
  IF NOT FOUND THEN
    EXECUTE 'CREATE TABLE ' || quote_ident(_tablename) ||
            ' (CHECK ( duid = ' || quote_literal(NEW.duid) || ')
            ) INHERITS (partitioned_power_station_datum)';

    -- Table permissions are not inherited from the parent.
    -- If permissions change on the master be sure to change them on the child also.
    EXECUTE 'ALTER TABLE ' || quote_ident(_tablename) || ' OWNER TO aemo';
    EXECUTE 'GRANT ALL ON TABLE ' || quote_ident(_tablename) || ' TO aemo';

    -- Indexes are defined per child, so we assign a default index that uses the partition columns
    EXECUTE 'CREATE INDEX ' || quote_ident(_tablename||'_indx1') || ' ON ' || quote_ident(_tablename) || ' (sample_time)';
  END IF;

  -- Insert the current record into the correct partition, which we are sure will now exist.
  EXECUTE 'INSERT INTO ' || quote_ident(_tablename) || ' VALUES ($1.*)' USING NEW;
  RETURN NULL;
END;
  $BODY$
LANGUAGE plpgsql;


CREATE TRIGGER partitioned_power_station_datum_trigger
  BEFORE INSERT ON partitioned_power_station_datum
  FOR EACH ROW EXECUTE PROCEDURE psdatum_partition_function();

INSERT INTO partitioned_power_station_datum
  SELECT * from power_station_datum;

BEGIN;
  ALTER TABLE power_station_datum        RENAME TO power_station_datum_original;
  ALTER TABLE partitioned_power_station_datum RENAME TO power_station_datum;

  -- Note: the table which the sub-tables inherit from is now power_station_datum
  -- not partitioned_power_station_datum - this will replace the previous function so that
  -- referenced the now nonexistant table.
  CREATE OR REPLACE FUNCTION
    public.psdatum_partition_function()
  RETURNS TRIGGER AS
    $BODY$
  DECLARE
    _tablename text;
    _result record;
  BEGIN
    _tablename := 'power_station_datum_'||NEW.duid;

    -- Check if the partition needed for the current record exists
    PERFORM 1
    FROM   pg_catalog.pg_class c
    JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE  c.relkind = 'r'
    AND    c.relname = _tablename;
    -- AND    n.nspname = '';

    -- If the partition needed does not yet exist, then we create it:
    -- Note that || is string concatenation (joining two strings to make one)
    IF NOT FOUND THEN
      EXECUTE 'CREATE TABLE ' || quote_ident(_tablename) ||
              ' (CHECK ( duid = ' || quote_literal(NEW.duid) || ')
              ) INHERITS (power_station_datum)';

      -- Table permissions are not inherited from the parent.
      -- If permissions change on the master be sure to change them on the child also.
      EXECUTE 'ALTER TABLE ' || quote_ident(_tablename) || ' OWNER TO aemo';
      EXECUTE 'GRANT ALL ON TABLE ' || quote_ident(_tablename) || ' TO aemo';

      -- Indexes are defined per child, so we assign a default index that uses the partition columns
      EXECUTE 'CREATE INDEX ' || quote_ident(_tablename||'_indx1') || ' ON ' || quote_ident(_tablename) || ' (sample_time)';
    END IF;

    -- Insert the current record into the correct partition, which we are sure will now exist.
    EXECUTE 'INSERT INTO ' || quote_ident(_tablename) || ' VALUES ($1.*)' USING NEW;
    RETURN NULL;
  END;
    $BODY$
  LANGUAGE plpgsql;


COMMIT;

--  NOTE:  DROP TABLE power_station_datum_original; once data is confirmed
--         correct.




