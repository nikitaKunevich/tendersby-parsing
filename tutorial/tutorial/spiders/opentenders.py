import scrapy
import json
import codecs
import urllib

class OpenTendersSpider(scrapy.Spider):
    name = "tenders"
    main_fields_map = {
        # id info
        0: 'date',
        1: 'number',
        2: 'status',

        # common info
        3: 'field',
        4: 'item_description',

        # creator info
        5: 'who_buys',
        6: 'buyer_info',
        7: 'buyer_contact',

        8: 'creation_date',
        9: 'end_date',
        10: 'approximate_sum',
        11: 'qualification_requirement',
        12: 'time_place_show_documents',
        13: 'document_prices',
        14: 'proposition_place_terms',
        15: 'other_data',

        # 16: 'lots',

        # 17: 'files',

        # 18: 'history'
    }

    lots_fields_map = {
        0: 'lot_id',
        1: 'lot_status',
        2: 'lot_name',
        3: 'lot_quantity',
        4: 'lot_measure_unit',
        5: 'lot_order_price',
        6: 'lot_currency',
        7: 'lot_full_price_usd',
    }

    history_fields_map = {
        0: 'history_time',
        1: 'history_event'
    }

    def start_requests(self):
        limit = 100000
        url = 'http://opentenders.by/tenders/current/'
        # yield scrapy.Request(url + str(50), callback=self.parse)
        for num in xrange(limit):
            response = scrapy.Request(url = url + str(num), callback=self.parse)
            self.state['items_count'] = self.state.get('items_count', 0) + 1
            yield response

    def parse(self, response):
        tender = {}
        root = response.css('div.tenders-view')
        basic_info = root.css('.info-body tr')
        for i in xrange(len(self.main_fields_map)):
            tender[self.main_fields_map[i]] = basic_info[i].css('td::text').extract_first()
        
        # lots info
        tender['lots'] = []
        lots = root.css('.detail_lots tbody tr')
        for lot in lots:
            lot_item = {}
            lot_columns = lot.css('td a::text')
            for i in xrange(len(self.lots_fields_map)):
               lot_item[self.lots_fields_map[i]] = lot_columns[i].extract()

            tender['lots'].append(lot_item)

        # files info
        tender['files'] = []
        files = root.css('.detail_files tbody tr')
        for file in files:
            file_item = {}
            file_item['link']= file.css('td a::attr(href)').extract_first()
            file_item['name'] = file.css('td a::text').extract_first()
            tender['files'].append(file_item)

        # history info
        tender['history'] = []
        events = root.css('.detail_history tbody tr')
        for event in events:
            event_item = {}
            event_columns = event.css('td')
            event_item['time']= event_columns[0].css('::text').extract()
            if len(event_columns[1].css('a')) == 0:
                event_item['name'] = event_columns[1].css('::text').extract()
            else:
                event_item['name'] = event_columns[1].css('a::text').extract_first()
                event_item['link'] = event_columns[1].css('a::attr(href)').extract_first()            

            tender['history'].append(event_item)

        # print(json.dumps(tender))
        # with codecs.open('_{}'.format(filename), 'w', encoding="utf-8") as outfile:
        # out_file = codecs.open("out_{}.json".format(, "w", encoding="utf-8")
        # json.dump(tender, out_file, ensure_ascii=False, )
        # out_file.close()

        yield tender