with stg_orders as 
(
    select
        OrderID,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey,
    from {{source('northwind','Orders')}}
),
stg_order_details as
(
    select
        OrderID,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        quantity, 
        unitprice,
        discount
    from {{source('northwind','Order_Details')}}
),
stg_order_combined as
(
    select  
        o.*,
        od.productkey,
        od.quantity,
        (od.quantity * od.unitprice) as extendedpriceamount,
        ((od.quantity * od.unitprice) * od.discount) as discountamount
    from stg_orders o
        join stg_order_details od on o.orderid = od.orderid
)
select 
    *,
    extendedpriceamount - discountamount as soldamount
from stg_order_combined