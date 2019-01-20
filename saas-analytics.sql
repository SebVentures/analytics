with lifetime_ext_1 as (
  select 
    customer_id, period, 
    mrr, 
    -- The cohort is the date of the first subscription period
    min(period) over (partition by customer_id ) as cohort,
    -- What is the previous period for this customers if any?
    lag(period, 1) over 
      (partition by customer_id order by period asc) as prev_period,
    -- What is the next  period for this customers if any?
    lead(period, 1) over 
      (partition by customer_id order by period asc) as next_period
  from saas.lifetime
),
-- We create a new CTE in order to access the new columns
lifetime_ext_2 as (  
  select *,
    -- Get the previous month MRR if any
    case when prev_period = period - interval'1 month'
        then lag(mrr, 1) 
            over (partition by customer_id order by period asc) end 
              as prev_mrr,
    -- Get the next month MRR if any
    case when next_period = period + interval'1 month'
        then lead(mrr, 1) 
             over (partition by customer_id order by period asc) end 
               as next_mrr,
    -- Nomber of month from cohort up to this period
    extract(year from age(period, cohort))::int*12
      + extract(month from age(period, cohort))::int as life_month
  from lifetime_ext_1
),
-- Active customers rows
active as (
  select customer_id, period, 
    cohort, life_month,
    1 as customer_count, 
    case when life_month = 0 then 1 else 0 end as new_customer, 
    0 as lost_customer, 
    case when life_month > 0 and prev_mrr is null 
      then 1 else 0 end as winback_customer,
    mrr, case when cohort = period then mrr else 0 end  as new_mrr, 
    0 as lost_mrr, 
    case when life_month > 0 and prev_mrr is null 
      then mrr else 0 end as winback_mrr, 
    case when prev_mrr is not null and prev_mrr > mrr 
      then prev_mrr - mrr else 0 end as reduction_mrr, 
    case when prev_mrr is not null and prev_mrr < mrr 
      then mrr - prev_mrr else 0 end as expansion_mrr
  from lifetime_ext_2
),
-- Churning customers rows (one period after their last presence)
churners as (
  select customer_id, (period + interval'1 month')::date as period, 
    cohort, 
    life_month+1 as life_month,
    0 as customer_count, 0 as new_customer, 
    1 as lost_customer, 0 as winback_customer,
    0 as mrr, 0 as new_mrr, mrr as lost_mrr, 0 as winback_mrr, 
    0 as reduction_mrr, 0 as expansion_mrr 
  from lifetime_ext_2
  where next_mrr is null
),
-- Merging active customers rows and churning rows
fusion as (
  select * from active
  union all 
  select * from churners
)
select *
from fusion
-- Avoiding to predicting that everyone will churn.
where period <= (select max(period) from active)
order by customer_id, period 
