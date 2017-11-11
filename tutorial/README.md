```scrapy crawl icetrade -o icetrade.json -t json```

"-o",
"items_2.json",
"-t",
"json"

Disable logs:
------------
LOG_ENABLED=False

Todo later:
==========
* Parse changelog - EASY
* Parse docx - IMPOSSIBRY

Avoiding getting banned:
========================
* rotate your user agent from a pool of well-known ones from browsers (google around to get a list of them)
* disable cookies (see COOKIES_ENABLED) as some sites may use cookies to spot bot behaviour
* use download delays (2 or higher). See DOWNLOAD_DELAY setting.
* if possible, use Google cache to fetch pages, instead of hitting the sites directly
* use a pool of rotating IPs. For example, the free Tor project or paid services like ProxyMesh. An open source alternative is scrapoxy, a super proxy that you can attach your own proxies to.
* use a highly distributed downloader that circumvents bans internally, so you can just focus on parsing clean pages. One example of such downloaders is Crawlera