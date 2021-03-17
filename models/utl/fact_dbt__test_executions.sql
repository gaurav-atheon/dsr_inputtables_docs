{{ config( materialized='incremental', unique_key='model_execution_id' ) }}

with model_executions as (

    select *
    from {{ ref('stg_dbt__test_executions') }}

),

model_executions_incremental as (

    select *
    from model_executions

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where artifact_generated_at > (select max(artifact_generated_at) from {{ this }})
    {% endif %}

),

fields as (

    select
        model_execution_id,
        command_invocation_id,
        artifact_generated_at,
        was_full_refresh,
        node_id,
        thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected
    from model_executions_incremental

)

select * from fields