CREATE OR REPLACE TABLE STAGED.NASA_EXOPLANETS AS
SELECT
    pl_name AS planet_name,
    disc_year AS discovery_year,
    pl_controv_flag AS controversial_flag,
    discoverymethod AS discovery_method,
    disc_facility AS discovery_facility,
    disc_instrument AS discovery_instrument,
    pl_orbper AS orbital_period_days,
    pl_rade AS radius_earth_radii,
    st_rad AS star_radius_solar_radii,
    pl_orbsmax AS orbital_semi_major_axis_in_au
FROM RAW.NASA_EXOPLANETS
WHERE pl_name IS NOT NULL;