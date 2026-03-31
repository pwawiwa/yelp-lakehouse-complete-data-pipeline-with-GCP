CREATE OR REPLACE PROCEDURE `{{ project_id }}.{{ silver_dataset }}.sp_pipeline_audit_report`()
BEGIN
  /*
  =============================================================================
  PROCEDURE: sp_pipeline_audit_report
  DESCRIPTION: Provides a high-level health report across the Medallion 
               architecture for the Data Governance / Data Engineering team.
  METRICS:
    - Row counts across Bronze, Silver, and Gold layers.
    - SCD Type 2 integrity (Active vs Historical record balance).
    - Pipeline Latency (Time since last successful record sync).
    - Data Quality Audit (NULL checks on primary keys).
  =============================================================================
  */

  -- 1. Snapshot of Volume across the Lakehouse
  SELECT 
    '1. VOLUME' as category,
    'Bronze' as layer, 
    table_id as entity, 
    row_count,
    TIMESTAMP_MILLIS(creation_time) as created_at
  FROM `{{ project_id }}.{{ bronze_dataset }}.__TABLES__`
  WHERE table_id NOT LIKE '%$%'
  
  UNION ALL
  
  SELECT 
    '1. VOLUME' as category,
    'Silver' as layer, 
    table_id, 
    row_count,
    TIMESTAMP_MILLIS(creation_time)
  FROM `{{ project_id }}.{{ silver_dataset }}.__TABLES__`
  WHERE table_id NOT LIKE '%$%'
  
  UNION ALL
  
  SELECT 
    '1. VOLUME' as category,
    'Gold' as layer, 
    table_id, 
    row_count,
    TIMESTAMP_MILLIS(creation_time)
  FROM `{{ project_id }}.{{ gold_dataset }}.__TABLES__`
  WHERE table_id NOT LIKE '%$%'
  ORDER BY layer, row_count DESC;

  -- 2. SCD Type 2 Health Audit (Dimensions)
  SELECT 
    '2. SCD2 HEALTH' as category,
    'silver.businesses' as table,
    COUNTIF(is_current = TRUE) as active_records,
    COUNTIF(is_current = FALSE) as history_records,
    ROUND(SAFE_DIVIDE(COUNTIF(is_current = FALSE), COUNT(*)) * 100, 2) as overlap_pct
  FROM `{{ project_id }}.{{ silver_dataset }}.businesses`
  
  UNION ALL
  
  SELECT 
    '2. SCD2 HEALTH',
    'silver.users',
    COUNTIF(is_current = TRUE),
    COUNTIF(is_current = FALSE),
    ROUND(SAFE_DIVIDE(COUNTIF(is_current = FALSE), COUNT(*)) * 100, 2)
  FROM `{{ project_id }}.{{ silver_dataset }}.users`;

  -- 3. Pipeline Freshness & Latency
  SELECT 
    '3. LATENCY' as category,
    'Overall Pipeline' as metric,
    MAX(_processed_at) as last_ingested_record,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_processed_at), HOUR) as hours_since_sync,
    CASE 
      WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_processed_at), HOUR) > 24 THEN '🔴 LAGGING'
      WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_processed_at), HOUR) > 6 THEN '🟡 DELAYED'
      ELSE '🟢 HEALTHY'
    END as status
  FROM `{{ project_id }}.{{ silver_dataset }}.businesses`;

  -- 4. Critical Quality Audit (Null Keys)
  SELECT 
    '4. DQ AUDIT' as category,
    'Business Key Check' as audit_type,
    COUNTIF(business_id IS NULL) as null_keys,
    COUNTIF(name IS NULL) as null_names,
    COUNT(*) as total_rows
  FROM `{{ project_id }}.{{ silver_dataset }}.businesses`
  WHERE is_current = TRUE;

END;
