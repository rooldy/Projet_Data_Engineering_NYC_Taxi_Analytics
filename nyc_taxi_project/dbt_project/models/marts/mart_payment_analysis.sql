{{ config(materialized='table') }}

-- Analyse du revenu et du comportement de pourboire par mode de paiement,
-- fournisseur (dim_vendor) et type de tarif (dim_rate_code) - ces trois
-- dimensions n'etaient jusqu'ici utilisees que pour la validation
-- referentielle dans fct_trips, jamais pour une vraie analyse business.
select
    pt.payment_label,
    v.vendor_name,
    rc.rate_code_label,
    count(*) as total_trips,
    round(sum(t.total_amount), 2) as total_revenue,
    round(avg(t.total_amount), 2) as avg_fare,
    round(avg(t.tip_amount), 2) as avg_tip,
    round(
        sum(case when t.tip_amount > 0 then 1 else 0 end) * 100.0 / count(*),
        2
    ) as pct_trips_with_tip
from {{ ref('fct_trips') }} as t
inner join {{ ref('dim_payment_type') }} as pt
    on t.payment_type_id = pt.payment_type_id
inner join {{ ref('dim_vendor') }} as v
    on t.vendor_id = v.vendor_id
inner join {{ ref('dim_rate_code') }} as rc
    on t.rate_code_id = rc.rate_code_id
where t.is_valid_trip = true
group by pt.payment_label, v.vendor_name, rc.rate_code_label
order by total_revenue desc