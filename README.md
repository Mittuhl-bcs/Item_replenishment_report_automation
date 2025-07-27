This is an replenishment items check automation


```
python runner.py --suppliers_file "D:\Item_replenishment_report_automation\suppliers.json" --new_loop "yes"
```

## Objective:
to read the data, validate against business logics and store the cleaned data in the temporary postgres db and then later send the data as a report through mail

## Note:
- new_loop: give "yes" when starting a new automation loop, so it will delete all the existing in database before saving the processed data in the table