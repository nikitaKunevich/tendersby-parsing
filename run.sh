source ../venv/bin/activate
time_var=date +%FT%T
scrapy crawl icetrade -o tutorials/items_$time_var.json -t json