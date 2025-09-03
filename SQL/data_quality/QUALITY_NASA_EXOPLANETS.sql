SELECT
    planet_name,
    planet_letter,
    host_star,
    system_distance,
    discovery_year,
    controversial_flag,
    discovery_method,
    discovery_facility,
    discovery_instrument,
    orbital_period_days,
    radius_earth_radii,
    star_radius_solar_radii,
    orbital_semi_major_axis_in_au
FROM STAGED.NASA_EXOPLANETS
WHERE planet_name IS NULL;