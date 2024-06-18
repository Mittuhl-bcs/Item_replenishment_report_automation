New and updated one for class_ids:
select * from bcs_view_master_data_sales_by_class order by total_sales desc, no_of_lines desc

here in the above one use where clause on "last_price_update_supplier" for categorizing each of the suppliers

example: select * from bcs_view_master_data_sales_by_class where last_price_update_supplier = 133602 order by total_sales desc, no_of_lines desc 


Old one:
select top 10 * from bcs_view_master_data_sales_by_class order by class_id1, class_id2, class_id3



New query (different):
select * from bcs_view_master_data_sales_dimensions where last_price_update_supplier = 133602
inv_mast_uid, item_id, weight, net_weight, height, width, length, no_of_lines, total_sales, last_price_update_supplier
