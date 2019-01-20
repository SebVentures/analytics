with lifetime_ext_1 as (
  select 
    customer_id, period, mrr, 1 as customer_count,
    min(period) over (partition by customer_id ) as cohort,
    lag(period, 1) over (partition by customer_id order by period asc) as prev_period,
    lead(period, 1) over (partition by customer_id order by period asc) as next_period
  from analytics.lifetime
),
lifetime_ext_2 as (  
  select *,
    case when prev_period = period - interval'1 month'
      then lag(mrr, 1) over (partition by customer_id order by period asc) end as prev_mrr,
    case when next_period = period + interval'1 month'
      then lead(mrr, 1) over (partition by customer_id order by period asc) end as next_mrr,
    extract(year from age(period, cohort))::int*12+extract(month from age(period, cohort))::int as life_month
  from lifetime_ext_1
),
active as (
  select customer_id, period, 
    cohort, life_month,
    1 as customer_count, 
    case when life_month = 0 then 1 else 0 end as new_customer, 0 as lost_customer, 0 as winback_customer,
    mrr, case when cohort = period then mrr else 0 end  as new_mrr, 
    0 as lost_mrr, case when life_month > 0 and prev_mrr is null then mrr else 0 end as winback_mrr, 
    case when prev_mrr is not null and prev_mrr > mrr then prev_mrr - mrr else 0 end as reduction_mrr, 
    case when prev_mrr is not null and prev_mrr < mrr then mrr - prev_mrr else 0 end as expansion_mrr
  from lifetime_ext_2
),
churners as (
  select customer_id, (period + interval'1 month')::date as period, 
    cohort, life_month+1 as life_month,
    0 as customer_count, 0 as new_customer, 1 as lost_customer, 0 as winback_customer,
    0 as mrr, 0 as new_mrr, mrr as lost_mrr, 0 as winback_mrr, 0 as reduction_mrr, 0 as expansion_mrr 
  from lifetime_ext_2
  where next_mrr is null
),
fusion as (
  select * from active
  union all 
  select * from churners
)
select *
from fusion
where period <= (select max(period) from active)
order by customer_id, period 
