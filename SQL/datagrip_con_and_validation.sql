-- Connect to the db, not the catalog

LOAD ducklake;
ATTACH 'ducklake:/Users/michaelgalo/Workspace/data-engineering/capstone/Orbital/catalog.ducklake'
AS my_ducklake (DATA_PATH '/Users/michaelgalo/Workspace/data-engineering/capstone/Orbital/data');
USE my_ducklake;

-- Initial API Data Validation
SELECT * FROM RAW.astronauts;
SELECT * FROM RAW.nasa_donki;
SELECT * FROM RAW.nasa_apod;
SELECT * FROM RAW.nasa_exoplanets;