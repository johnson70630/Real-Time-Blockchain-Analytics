-- Run manually as the project owner/admin. Runtime code performs no DDL.
USE ROLE BLOCKCHAIN_ADMIN_ROLE;

CREATE TABLE IF NOT EXISTS BLOCKCHAIN_ANALYTICS.CONTROL.ALERT_STATE (
  ALERT_KEY VARCHAR NOT NULL,
  CHECK_NAME VARCHAR NOT NULL,
  PIPELINE_NAME VARCHAR NOT NULL,
  REASON_CODE VARCHAR NOT NULL,
  STATE VARCHAR NOT NULL,
  SEVERITY VARCHAR,
  OPENED_AT TIMESTAMP_TZ,
  LAST_OBSERVED_AT TIMESTAMP_TZ,
  RESOLVED_AT TIMESTAMP_TZ,
  INCIDENT_GENERATION NUMBER(38, 0) NOT NULL DEFAULT 0,
  OCCURRENCE_COUNT NUMBER(38, 0) NOT NULL DEFAULT 0,
  CURRENT_VALUE NUMBER(38, 0),
  WARNING_THRESHOLD NUMBER(38, 0),
  STALE_THRESHOLD NUMBER(38, 0),
  STATE_VERSION NUMBER(38, 0) NOT NULL DEFAULT 0,
  CONSTRAINT PK_ALERT_STATE PRIMARY KEY (ALERT_KEY),
  CONSTRAINT CK_ALERT_STATE_STATE CHECK (STATE IN ('OPEN', 'RESOLVED')),
  CONSTRAINT CK_ALERT_STATE_SEVERITY
    CHECK (SEVERITY IS NULL OR SEVERITY IN ('WARNING', 'CRITICAL'))
);

GRANT USAGE ON DATABASE BLOCKCHAIN_ANALYTICS
  TO ROLE BLOCKCHAIN_PIPELINE_ROLE;
GRANT USAGE ON SCHEMA BLOCKCHAIN_ANALYTICS.CONTROL
  TO ROLE BLOCKCHAIN_PIPELINE_ROLE;

INSERT INTO BLOCKCHAIN_ANALYTICS.CONTROL.ALERT_STATE (
  ALERT_KEY, CHECK_NAME, PIPELINE_NAME, REASON_CODE, STATE
)
SELECT column1, column2, column3, column4, 'RESOLVED'
FROM VALUES
  ('freshness:aave_forward', 'aave_forward_source_position', 'aave_forward', 'SOURCE_POSITION_LAG'),
  ('freshness:uniswap_forward', 'uniswap_forward_source_position', 'uniswap_forward', 'SOURCE_POSITION_LAG'),
  ('freshness:chainlink_global', 'chainlink_forward_source_position', 'chainlink_forward', 'SOURCE_POSITION_LAG'),
  ('freshness:chainlink_eth_usd', 'chainlink_eth_usd', 'chainlink_forward', 'CHAINLINK_FEED_AGE'),
  ('freshness:chainlink_usdc_usd', 'chainlink_usdc_usd', 'chainlink_forward', 'CHAINLINK_FEED_AGE'),
  ('freshness:chainlink_usdt_usd', 'chainlink_usdt_usd', 'chainlink_forward', 'CHAINLINK_FEED_AGE'),
  ('freshness:chainlink_wbtc_usd', 'chainlink_wbtc_usd', 'chainlink_forward', 'CHAINLINK_FEED_AGE'),
  ('freshness:daily_warehouse', 'daily_warehouse_load', 'daily_warehouse_pipeline', 'WAREHOUSE_REFRESH_AGE'),
  ('freshness:marts', 'mart_transformations', 'daily_warehouse_pipeline', 'MART_REFRESH_AGE'),
  ('pipeline_failure:aave_forward', 'aave_forward_execution', 'aave_forward', 'PIPELINE_FAILURE'),
  ('pipeline_failure:chainlink_forward', 'chainlink_forward_execution', 'chainlink_forward', 'PIPELINE_FAILURE'),
  ('pipeline_failure:uniswap_forward', 'uniswap_forward_execution', 'uniswap_forward', 'PIPELINE_FAILURE'),
  ('pipeline_failure:daily_warehouse_pipeline', 'daily_warehouse_pipeline_execution', 'daily_warehouse_pipeline', 'PIPELINE_FAILURE')
WHERE column1 NOT IN (
  SELECT ALERT_KEY FROM BLOCKCHAIN_ANALYTICS.CONTROL.ALERT_STATE
);

GRANT SELECT, UPDATE
  ON TABLE BLOCKCHAIN_ANALYTICS.CONTROL.ALERT_STATE
  TO ROLE BLOCKCHAIN_PIPELINE_ROLE;
