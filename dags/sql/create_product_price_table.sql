CREATE SCHEMA IF NOT EXISTS analytics_objects;
CREATE TABLE IF NOT EXISTS analytics_objects.obj_product_price (
    price VARCHAR NOT NULL,
    anbieter VARCHAR NOT NULL,
    device VARCHAR NOT NULL,
    condition VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics_objects.obj_product_price_stage (
    price VARCHAR NOT NULL,
    anbieter VARCHAR NOT NULL,
    device VARCHAR NOT NULL,
    condition VARCHAR NOT NULL,
    created_at VARCHAR NOT NULL
);
--     datetime DATE NOT NULL

ALTER TABLE analytics_objects.obj_product_price_stage
ADD COLUMN IF NOT EXISTS am_pm VARCHAR;


ALTER TABLE analytics_objects.obj_product_price
ADD COLUMN IF NOT EXISTS am_pm VARCHAR;