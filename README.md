This is an replenishment check automation


```
python runner.py --suppliers_file "D:\Item_replenishment_report_automation\suppliers.json" --new_loop "yes"
```

## Steps:

- Step 1: Switch on VPN with sonicwall netextender
- Step 2: Run the runner script.


## Note:
- new_loop: give "yes" when starting a new automation loop, so it will delete all the existing in database before saving the processed data in the table