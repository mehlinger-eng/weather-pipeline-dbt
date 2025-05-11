{{ config(
    materialized='incremental',
    unique_key='document_id',
    incremental_strategy='merge',
    partition_by={
      "field": "firestore_created_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

with source as (
    select *
    from {{ ref('base__weather_raw') }}
)

, flattened as (
  select
    document_id                                                              as document_id
    , created_at                                                             as firestore_created_at
    , json_extract_scalar(payload, '$.timestamp')                            as forecast_timestamp
    , cast(json_extract_scalar(payload, '$.temperature_celsius') as FLOAT64) as temperature_celsius
    , cast(json_extract_scalar(payload, '$.precipitation_mm') as FLOAT64)    as precipitation_mm
    , cast(json_extract_scalar(payload, '$.latitude') as FLOAT64)            as latitude
    , cast(json_extract_scalar(payload, '$.longitude') as FLOAT64)           as longitude
    , json_extract_scalar(payload, '$.location')                             as location
    , json_extract_scalar(payload, '$.ingestion_timestamp')                  as ingestion_timestamp
    , json_extract_scalar(payload, '$.bq_ingestion_timestamp')               as bq_ingestion_timestamp
    , JSON_EXTRACT(payload, '$.operation_history')                           as operation_history
  from source
)

select *
from flattened

