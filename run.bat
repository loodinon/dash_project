@echo off
call python "1. Fake_data_new.bat"
call python "2. Clean data new.bat"
call python "3. dim_detach new.bat"
call python "4. export_fact new.bat"
call python "5. convert_hash new.bat"
call python "6. Join table in sql.bat"