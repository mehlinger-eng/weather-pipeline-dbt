{{ config(
    materialized='incremental',
    unique_key='document_id',
    incremental_strategy='merge',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

select *
from {{ source('weather_data', 'weather_raw') }}