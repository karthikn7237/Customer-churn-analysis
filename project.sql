select * from customer_churn;

-- total number of customers
select count(*) as total_customers from customer_churn;

-- churn rate
select count(*) as total_churned_customers,
       (count(*) * 100.0) / (select count(*) from customer_churn) as churn_rate_percentage
from customer_churn
where customer_status = 'churned';

-- find the average age of churned customers
select avg(age) as average_age_churned_customers from customer_churn
where customer_status = 'churned';

-- analyze the distribution of monthly charges among churned customers
select avg(monthly_charge) as avg_monthly_charge
from customer_churn
where customer_status = 'churned';

-- create a query to identify the contract types that are most prone to churn
select contract,
       count(*) as total_customers,
       sum(case when customer_status = 'churned' then 1 else 0 end) as churned_customers,
       (sum(case when customer_status = 'churned' then 1 else 0 end) * 1.0 / count(*)) as churn_rate
from customer_churn
group by contract
order by churn_rate desc;

-- identify customers with high total charges who have churned
select customer_id, total_charges
from customer_churn
where customer_status = 'churned' and total_charges > (select avg(total_charges) from customer_churn);

-- calculate the total charges distribution for churned and non-churned customers
select customer_status as churn_status,count(*) as customer_count,
	sum(total_charges) as total_charges,
	avg(total_charges) as avg_total_charges
from customer_churn
group by customer_status;

-- identify customers who have both online security and online backup services and have not churned
select customer_id, customer_status, online_security, online_backup
from customer_churn
where online_security = 'yes' and online_backup = 'yes' and customer_status = 'stayed';

-- determine the most common combinations of services among churned customers
select phone_service, internet_service, online_security, count(*) as customer_count
from customer_churn
where customer_status = 'churned'
group by phone_service, internet_service, online_security
order by customer_count desc;

-- calculate the average monthly charges for different age groups among churned customers
select 
    case 
        when age < 25 then 'under 25'
        when age between 25 and 35 then '26-35'
        when age between 36 and 45 then '36-45'
        when age between 46 and 55 then '46-55'
        when age between 56 and 65 then '56-65'
        else '65 above'
    end as age_group, 
    avg(monthly_charge) as avg_monthly_charge
from customer_churn
where customer_status = 'churned'
group by age_group;

-- determine the average age and total charges for customers with multiple lines and online backup
select avg(age) as avg_age, avg(total_charges) as avg_total_charges
from customer_churn
where multiple_lines = 'yes' and online_backup = 'yes';

-- calculate the average monthly charges for customers who have multiple lines and streaming TV
select avg(monthly_charge) as avg_monthly_charge
from customer_churn
where multiple_lines = 'yes' and streaming_tv = 'yes';

-- identify the customers who have churned and used the most online services
select customer_id, online_security, online_backup, streaming_tv, streaming_movies, streaming_music,
    (
        (case when online_security = 'yes' then 1 else 0 end) +
        (case when online_backup = 'yes' then 1 else 0 end) +
        (case when streaming_tv = 'yes' then 1 else 0 end) +
        (case when streaming_movies = 'yes' then 1 else 0 end) +
        (case when streaming_music = 'yes' then 1 else 0 end)
    ) as online_services_used
from customer_churn
where customer_status = 'churned'
order by online_services_used desc;

-- calculate the average age and total charges for customers with different combinations of streaming services
with aggregated_data as (
    select streaming_tv, streaming_movies, streaming_music,
           avg(age) as average_age, sum(total_charges) as total_charges
    from customer_churn
    group by streaming_tv, streaming_movies, streaming_music
)
select streaming_tv as service_1, streaming_movies as service_2, streaming_music as service_3, average_age, total_charges
from aggregated_data
order by service_1, service_2, service_3;

-- calculate the average monthly charges and total charges for customers who have churned, grouped by contract type and internet service type
with churned_customers as (
    select customer_id, contract, internet_service,
           avg(monthly_charge) as avg_monthly_charge,
           sum(total_charges) as total_charges
    from customer_churn
    where customer_status = 'churned'
    group by customer_id, contract, internet_service
)
select customer_id, contract, internet_service, avg_monthly_charge, total_charges
from churned_customers
order by customer_id, contract, internet_service;

-- find the customers who have churned and are not using online services, and their average total charges
select customer_id, total_charges,
       (select avg(total_charges) 
        from customer_churn
        where customer_status = 'churned' and online_security = 'no' and online_backup = 'no') as avg_total_charges_no_online_services
from customer_churn
where customer_status = 'churned' and online_security = 'no' and online_backup = 'no';

-- calculate the average monthly charges and total charges for customers who have churned, grouped by the number of dependents
select number_of_dependents, avg(monthly_charge) as avg_monthly_charge, sum(total_charges) as total_charges
from customer_churn
where customer_status = 'churned'
group by number_of_dependents
order by number_of_dependents;

-- identify the customers who have churned, and their contract duration in months (for monthly contracts)
select customer_id, contract
from customer_churn
where customer_status = 'churned' and contract = 'month-to-month';

-- determine the average age and total charges for customers who have churned, grouped by internet service and phone service
select internet_service, phone_service, avg(age) as avg_age, sum(total_charges) as total_charges
from customer_churn
where customer_status = 'churned'
group by internet_service, phone_service
order by internet_service, phone_service;

-- create a view to find the customers with the highest monthly charges in each contract type
create view customers_highest_monthly_charges as
with max_monthly_charges as (
    select contract, max(monthly_charge) as max_charge
    from customer_churn
    group by contract
)
select 
    a.customer_id, 
    a.contract, 
    a.monthly_charge
from customer_churn a
join max_monthly_charges b
on a.contract = b.contract and a.monthly_charge = b.max_charge;

select * from customers_highest_monthly_charges;

-- Create a view to find the customers who have churned and their cumulative total chargesover time
create view churned_customers_cumulative_charges as
select
    customer_id,
    tenure_in_months,
    total_charges,
    sum(total_charges) over (partition by customer_id order by tenure_in_months rows between unbounded preceding and current row) as cumulative_total_charges
from customer_churn
where customer_status = 'Churned';

select * from churned_customers_cumulative_charges;

-- stored procedure to calculate churn rate
delimiter //

create procedure calculate_churn_rate()
begin
    declare total_customers int;
    declare churned_customers int;
    declare churn_rate decimal(5, 2);

    select count(*) into total_customers from customer_churn;
    select count(*) into churned_customers from customer_churn where customer_status = 'churned';
    set churn_rate = (churned_customers / total_customers) * 100;
    select churn_rate as churn_rate;
end //
delimiter ;

call calculate_churn_rate();


-- Create a stored procedure to find high-value customers at risk of churning
delimiter //

create procedure high_value_customers_at_risk()
begin
    select
        customer_id,
        total_charges,
        monthly_charge,
        tenure_in_months,
        case
            when total_charges > 500 and tenure_in_months < 12 then 'At Risk'
            else 'Not At Risk'
        end as risk_status
    from customer_churn
    where total_charges > 500
    order by total_charges desc;
end //

delimiter ;

call high_value_customers_at_risk();

