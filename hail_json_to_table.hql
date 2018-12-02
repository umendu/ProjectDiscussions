-- Addition of Required Jar files to Hive Session
-- ADD JAR ${env:HOME}/hail-poc/csv-serde-1.1.2-0.11.0-all.jar;
ADD JAR ${env:HOME}/hail-poc/brickhouse-0.7.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION json_split AS 'brickhouse.udf.json.JsonSplitUDF';
SET hive.aux.jars.path=file://${env:HOME}/hail-poc/joda-time-2.3.jar;

-- Drop and Create HAIL_STORM_WARNINGS Table and Flume Ingested data into it
DROP TABLE IF EXISTS HAIL_STORM_WARNINGS;

CREATE TABLE HAIL_STORM_WARNINGS(VALUE STRING);

LOAD DATA INPATH '/user/${system:user.name}/hail_data/15-07-*/*/*/*' INTO TABLE HAIL_STORM_WARNINGS;

-- Structuring JSON formatted data into Rows and Columns and Extracting the Hail Storm Polygon Locations
DROP VIEW IF EXISTS HAIL_FEATURES_V1 ;

CREATE VIEW HAIL_FEATURES_V1 AS SELECT d.* FROM  
  (SELECT json_split(b.layers) as records FROM HAIL_STORM_WARNINGS a 
  LATERAL VIEW json_tuple(a.value, 'layers') b AS layers ) record_view
  LATERAL VIEW explode(record_view.records) c AS record
  LATERAL VIEW json_tuple(c.record, 'features') d AS features;

DROP VIEW IF EXISTS HAIL_POLYGON_DETAILS_V2 ;

CREATE VIEW HAIL_POLYGON_DETAILS_V2 AS SELECT d.*, e.* FROM
         (SELECT json_split(a.features) AS records FROM HAIL_FEATURES_V1 a) record_view
         LATERAL VIEW explode(record_view.records) b AS record
         LATERAL VIEW json_tuple(b.record, 'geometry', 'attributes') c AS geometry, attributes
         LATERAL VIEW json_tuple(c.geometry, 'rings') d AS rings 
         LATERAL VIEW json_tuple(c.attributes, 'OBJECTID', 'CLIENT_NAME', 'SEQUENCE_NUMBER', 'ALERT_ID', 'POLYGON_NUMBER', 'RECIPIENT_NAME',
'RECIPIENT_ID', 'ISSUE_DATE_TIME', 'START_DATE_TIME', 'END_DATE_TIME', 'DISPLAY_DATE_TIME', 'CLIENT_ID', 'EVENT_TYPE', 'SEVERITY', 'CLIENT_EVENT_TYPE', 'METEOROLOGIST_NAME', 'TEXT', 'CONTACT_PHONE_NUMBER' ) e 
         AS OBJECTID, CLIENT_NAME, SEQUENCE_NUMBER, ALERT_ID, POLYGON_NUMBER, RECIPIENT_NAME, RECIPIENT_ID, ISSUE_DATE_TIME, START_DATE_TIME, END_DATE_TIME, DISPLAY_DATE_TIME, CLIENT_ID, EVENT_TYPE, SEVERITY, CLIENT_EVENT_TYPE, METEOROLOGIST_NAME, TEXT, CONTACT_PHONE_NUMBER;

INSERT OVERWRITE DIRECTORY '/user/${system:user.name}/hail_storm/polygons/' SELECT * FROM HAIL_POLYGON_DETAILS_V2;

