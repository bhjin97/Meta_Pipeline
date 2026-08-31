BEGIN;

CREATE TABLE IF NOT EXISTS gold.mart_sales_daily
(LIKE gold_staging.mart_sales_daily INCLUDING ALL);

CREATE TABLE IF NOT EXISTS gold.mart_sales_monthly
(LIKE gold_staging.mart_sales_monthly INCLUDING ALL);

CREATE TABLE IF NOT EXISTS gold.mart_category_daily
(LIKE gold_staging.mart_category_daily INCLUDING ALL);

CREATE TABLE IF NOT EXISTS gold.mart_customer_summary
(LIKE gold_staging.mart_customer_summary INCLUDING ALL);

CREATE TABLE IF NOT EXISTS gold.mart_delivery_satisfaction
(LIKE gold_staging.mart_delivery_satisfaction INCLUDING ALL);

TRUNCATE gold.mart_sales_daily;

INSERT INTO gold.mart_sales_daily
SELECT * FROM gold_staging.mart_sales_daily;

TRUNCATE gold.mart_sales_monthly;

INSERT INTO gold.mart_sales_monthly
SELECT * FROM gold_staging.mart_sales_monthly;

TRUNCATE gold.mart_category_daily;

INSERT INTO gold.mart_category_daily
SELECT * FROM gold_staging.mart_category_daily;

TRUNCATE gold.mart_customer_summary;

INSERT INTO gold.mart_customer_summary
SELECT * FROM gold_staging.mart_customer_summary;

TRUNCATE gold.mart_delivery_satisfaction;

INSERT INTO gold.mart_delivery_satisfaction
SELECT * FROM gold_staging.mart_delivery_satisfaction;

DO $$
DECLARE
    staging_row_count BIGINT;
    gold_row_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO staging_row_count
    FROM gold_staging.mart_sales_daily;

    SELECT COUNT(*)
    INTO gold_row_count
    FROM gold.mart_sales_daily;

    IF staging_row_count <> gold_row_count THEN
        RAISE EXCEPTION
            'Row count mismatch for mart_sales_daily: gold_staging=%, gold=%',
            staging_row_count,
            gold_row_count;
    END IF;

    SELECT COUNT(*)
    INTO staging_row_count
    FROM gold_staging.mart_sales_monthly;

    SELECT COUNT(*)
    INTO gold_row_count
    FROM gold.mart_sales_monthly;

    IF staging_row_count <> gold_row_count THEN
        RAISE EXCEPTION
            'Row count mismatch for mart_sales_monthly: gold_staging=%, gold=%',
            staging_row_count,
            gold_row_count;
    END IF;

    SELECT COUNT(*)
    INTO staging_row_count
    FROM gold_staging.mart_category_daily;

    SELECT COUNT(*)
    INTO gold_row_count
    FROM gold.mart_category_daily;

    IF staging_row_count <> gold_row_count THEN
        RAISE EXCEPTION
            'Row count mismatch for mart_category_daily: gold_staging=%, gold=%',
            staging_row_count,
            gold_row_count;
    END IF;

    SELECT COUNT(*)
    INTO staging_row_count
    FROM gold_staging.mart_customer_summary;

    SELECT COUNT(*)
    INTO gold_row_count
    FROM gold.mart_customer_summary;

    IF staging_row_count <> gold_row_count THEN
        RAISE EXCEPTION
            'Row count mismatch for mart_customer_summary: gold_staging=%, gold=%',
            staging_row_count,
            gold_row_count;
    END IF;

    SELECT COUNT(*)
    INTO staging_row_count
    FROM gold_staging.mart_delivery_satisfaction;

    SELECT COUNT(*)
    INTO gold_row_count
    FROM gold.mart_delivery_satisfaction;

    IF staging_row_count <> gold_row_count THEN
        RAISE EXCEPTION
            'Row count mismatch for mart_delivery_satisfaction: gold_staging=%, gold=%',
            staging_row_count,
            gold_row_count;
    END IF;
END;
$$;

COMMIT;
