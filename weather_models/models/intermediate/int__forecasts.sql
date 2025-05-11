{{ config(
    materialized='incremental',
    unique_key='hashed_id',
    incremental_strategy='merge',
    partition_by={
      "field": "forecast_made_day",
      "data_type": "date"
    }
) }}

with source as (
    select *
    from {{ ref('stg__weather_raw') }}
)

, normalized as (
    select 
        {{ dbt_utils.generate_surrogate_key([
            'forecast_timestamp',
            'location',
            'latitude',
            'longitude'
        ]) }} as hashed_id
        , date(ingestion_timestamp) as forecast_made_day
        , format("%02d:00", cast(extract(hour from cast(forecast_timestamp as timestamp)) / 6 as int64) * 6) as forecast_hour
        , forecast_timestamp as forecast_timestamp
        , location as location
        , latitude as latitude
        , longitude as longitude
        , temperature_celsius as temperature_celsius
        , precipitation_mm as precipitation_mm
    from source 
)

select * 
from normalized