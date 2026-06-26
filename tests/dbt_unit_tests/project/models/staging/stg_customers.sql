-- Light staging over the raw seed: normalise names and derive the email's top-level domain
-- (everything after the @), which dim_customers joins to the accepted-domain list.
select
    customer_id,
    first_name,
    last_name,
    email,
    lower(split_part(email, '@', 2)) as email_top_level_domain
from {{ ref('raw_customers') }}
