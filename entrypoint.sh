#--to create all data
psql -h localhost -U postgres -d dbt_test -a -f data/table.sql
