DROP TABLE IF EXISTS `jobsity-317503.raw.trips`;
CREATE TABLE `jobsity-317503.raw.trips`(
    region STRING
    ,origin_coord STRING
    ,origin_destination STRING
    ,datetime STRING
    ,datasource STRING
    ,dtinsert TIMESTAMP
);

DROP TABLE IF EXISTS `jobsity-317503.trusted.dim_region`;
CREATE TABLE `jobsity-317503.trusted.dim_region`(
    id INT64
    ,desc_region STRING
);

DROP TABLE IF EXISTS `jobsity-317503.trusted.dim_datasource`;
CREATE TABLE `jobsity-317503.trusted.dim_datasource`(
    id INT64
    ,desc_datasource STRING
);

DROP TABLE IF EXISTS `jobsity-317503.trusted.fact_trips`;
CREATE TABLE `jobsity-317503.trusted.fact_trips`(
    id INT64
    ,datetime TIMESTAMP
    ,origin_lat NUMERIC
    ,origin_lon NUMERIC
    ,destination_lat NUMERIC
    ,destination_lon NUMERIC
    ,region_id INT64
    ,datasource_id INT64
);

