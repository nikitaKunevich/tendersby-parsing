# -*- coding: utf-8 -*-

import scrapy
import json
import codecs
import urllib
import re
from datetime import datetime

from config import siteURL, paths
import consts
import pyrebase
from config import firebase_db_config


firebase = pyrebase.initialize_app(firebase_db_config)
db = firebase.database()
data = {"name": "Mortimer 'Morty' Smith"}
# Get a reference to the auth service
auth = firebase.auth()
# Log the user in
user = auth.sign_in_with_email_and_password(email, password)
db.child("users").push(data)
a = 0
'''
only first lot_item is filled last lot info - FIXED
check if results are parsed
'''

class LotParseState():
    NoResult = 0
    Info = 1
    Result = 2

class IcetradeSpider(scrapy.Spider):


    # constants
    name = "icetrade"
    tender_field_map = {
        "site_address":".af-operator_site", #Адрес сайта, обеспечивающего доступ на ЭТП
        #["method_of_negotiation"],
        #"operator_info" ".af-operator_site", #Данные оператора	
        "industry":".af-industry", #Отрасль
        "short_description":".af-title", #Краткое описание предмета закупки	

        #customer info
        "procurement_held_by" : ".af-hold_by", #Закупка проводится	
        "customer_name" : ".af-customer_data", #Полное наименование заказчика, место нахождения организации, УНП	
        "contacts" : ".af.af-customer_contacts", #Фамилии, имена и отчества, номера телефонов работников заказчика	

        #procurement_info
        "creation_date":".af-created", #Дата размещения приглашения	
        "start_date":".af-request_start",
        "end_date":".af.af-request_end", #Дата и время окончания приема предложений	
        "documentation_provisioning_info":"",
        "documentation_price":"",
        "proposal_estimated_price":".af.af-currency", #Общая ориентировочная стоимость акупки	
        "proposal_closing_time":"",
        "qualification_terms":".af.af-qualification", # Квалификационные требования
        "proposal_participators_requirements" :".af.af-participator_demand",#Требования к оставу участников	
        "proposal_submission_address":"",
        "proposal_opening_time":"",
        "proposal_opening_address" :"",
        "preliminary_qualification_terms":"",
        "other_information" :".af.af-others" #Иные сведения	
    }

    lot_field_map = {
        u"Дата и время начала торгов":"negotiations_date", #Дата и время проведения переговоров	
        u"Срок поставки" : "delivery_time",
        u"Место поставки товара, выполнения работ, оказания услуг" : "shipment_place",


        u"Результат процедуры закупки": "procurement_result",
        u"Страны нахождения участников, с которыми заключен договор": "winners_country",
        u"Места нахождения участников, с которыми заключен договор": "winners_address",
        u"УНП участников (или номера документов, удостоверяющих личность, для физических лиц), с которыми заключен договор": "winners_UNP",
        u"Страны происхождения товара": "goods_origin",
        u"Дата договора (решения об отмене/признания несостоявшейся)": "contract_date",
        u"Объем (количество) поставки": "supply_quantity",
        u"Иные участники и цены их предложений": "other_participants",



        "" : "order_execution_place",

        "" : "order_execution_term",

        "" : "negotiations_place",
        "" : "negotiations_order_of_conduct",
        "" : "terms_of_contract", #//сроки заключения,
        "" : "contract_implementation_security_amount", 
        "" : "terms_of_contract_implementation_security", 
        "" : "proposal_security_amount",
        "" : "terms_of_proposal_security",

        u"Источник финансирования" : "source_of_financing",
        u"Размер конкурсного обеспечения": "competion_bank_guarantee",
        u"Размер аукционного обеспечения": "tender_bank_guarantee",
        u"Код ОКРБ" : "okrb_code",

    }
    
    def start_requests(self):
        number = None
        # with results 400520
        starting = 400520
        limit = 10000#10000

        if number:
            tender_response = scrapy.Request(url = siteURL + paths['tender'] + str(number), callback=self.parse_tender)
            # self.state['items_count'] = self.state.get('items_count', 0) + 1
            yield tender_response       
        else:
            for num in xrange(limit):
                tender_response = scrapy.Request(url = siteURL + paths['tender'] + str(starting + num), callback=self.parse_tender)
                # self.state['items_count'] = self.state.get('items_count', 0) + 1
                yield tender_response        


    def parse_tender(self, response):
        # try:
        tender = {}
        # exit if error 
        error = response.css('.container .content .err .msg::text').extract_first()
        if error:
            yield error.strip()
        
        root_block = response.css('#auctBlockCont > table')

        #tender_id
        header_string = response.css('div.ocB.w100 h1::text').extract()[1]
        header_match = re.search(r"(\d+)-(\d+)", header_string)
        tender['id'] = header_match.group(2)
        tender['unused_headers'] = []
        tender['unused_lot_headers'] = []
        self.process_tender_info(tender, root_block)
        self.process_files(tender, root_block)
        self.process_events(tender, root_block)
        yield self.process_lots(tender, root_block)

        # except:
        #     yield {}


    def process_tender_info(self, tender, root_block):

        #tender info
        for key in self.tender_field_map:
            if self.tender_field_map[key] == "":
                continue

            value = root_block.css(self.tender_field_map[key] + " td.afv::text").extract_first()
            if not value:
                continue

            tender[key] = root_block.css(self.tender_field_map[key] + " td.afv::text").extract_first().strip()

        field_items = root_block.css('.af')
        
        for elem in field_items:
            classes = elem.root.attrib['class'].split()
            for cl in classes:
                if cl != "af" and cl.startswith("af"):
                    tender['unused_headers']
        #tender type
        tender['type'] = root_block.css('tr.fst b::text').extract_first().strip()


    def process_files(self, tender, root_block):
        files_selectors = root_block.css('td.af-files p')
        tender['files'] = []
        for file in files_selectors:
            file_object = {}
            file_object['link'] = file.css('a::attr(href)').extract_first().strip()
            file_object['name'] = file.css('a::text').extract_first().strip()

            tender['files'].append(file_object)


    def process_events(self, tender, root_block):
        # print('process_events')
        first_event_id = 0
        rows = root_block.css('tr')
        for idx, row in enumerate(rows):
            head = row.css('th::text').extract_first()
            if (not head):
                continue
            #println(row.css('th::text').extract_first() + '\n')
            if head.strip() == u"События в хронологическом порядке":
                first_event_id = idx + 1
                break

        tender['events'] = []
        for row in rows[first_event_id:]:
            event = {}
            date = row.css('td:first-child::text').extract_first().strip()
            time = row.css('td:first-child span::text').extract_first().strip()
            time_obj = datetime.strptime(date + ' ' + time, "%d.%m.%Y %H:%M:%S")
            event['date'] = time_obj.isoformat()

            link_selector = row.css('td:last-child a').extract_first()
            if link_selector:
                event['text'] = row.css('td:last-child a::text').extract_first().strip()
                event['link'] = row.css('td:last-child a::attr(href)').extract_first().strip()
            else:
                event['text'] = row.css('td:last-child::text').extract_first().strip()
                        
            tender['events'].append(event)


    def process_lots(self, tender, root_block):
        print('process_lots')
        lot_items = []
        lot_selectors = root_block.css('table#lots_list .af')
        for lot in lot_selectors:
            lot_item = {}
            lot_item['id'] = lot.css('td:nth-child(1)::text').extract_first().strip()
            # ["item_name", "td:nth-child(2)"]
            lot_item['item_name'] = lot.css('td:nth-child(2)::text').extract_first().strip()
            quantity = lot.css('td:nth-child(3) span::text').extract_first()
            lot_item['quantity'] = "".join(quantity.strip())
            lot_item['quantity_measurement'] = lot.css('td:nth-child(3)::text').extract()[1].strip()[:-1]
            try:
                approx_price = lot.css('td:nth-child(3) span::text').extract()[1]
                lot_item['approx_price'] = "".join(approx_price.strip())
                lot_item['approx_price_currency'] = lot.css('td:nth-child(3)::text').extract()[3].strip()
            except:
                lot_item['approx_price'] = ''
            lot_item['status'] = lot.css('td:nth-child(4)::text').extract_first().strip()
            lot_item['result'] = {}
            lot_items.append(lot_item)
            
        if (len(lot_selectors)):
            tender['lot_items'] = lot_items
            response = scrapy.http.FormRequest(
                url = siteURL + paths['lot'],
                formdata = {'id': str(lot_items[0]['id']), 'auction_id': str(tender['id']), 'revision_id': '0'},
                headers={'X-Requested-With': 'XMLHttpRequest'},
                dont_filter=True,
                callback=self.parse_lot_item
            )
            response.meta['tender'] = tender
            response.meta['current_index'] = 0
            response.meta['state'] = LotParseState.NoResult
            for event in reversed(tender['events']):
                if (event['text'] == consts.RESULT_EVENT_LABEL):
                    response.meta['state'] = LotParseState.Info
                    response.meta['result_id'] = event['link'].split('/')[-1]

            return response
        else:
            return tender

    '''
    Recursive function
    '''
    def parse_lot_item(self, response):

        # get current lot from meta
        current_index = response.meta['current_index']
        tender = response.meta['tender']
        parsing_state = response.meta['state']
        lot_items = tender['lot_items']
        current_lot = lot_items[current_index]

        #fill lot_item with new info
        rows = response.css('tr.lotSubRow')
        for row in rows:
            header = row.css('th::text').extract_first().strip()
            if header not in self.lot_field_map:
                tender['unused_lot_headers'].append((tender['id'], header))
                data = {"name": "Mortimer 'Morty' Smith"}
                db.child("users").push(data)
                #TODO: add headers
                continue
            # try:
            data_from_div = row.css('th+td>div::text').extract_first()

            if data_from_div:
                data = data_from_div.strip()
            else:
                data = row.css('th+td::text').extract_first().strip()
            if (not data):
                continue
            #TODO: should differently parse data of different types, e.g. time is 'date time'
            if parsing_state == LotParseState.Result:
                lot_items[current_index]['result'][self.lot_field_map[header]] = data
            else:
                lot_items[current_index][self.lot_field_map[header]] = data


        if (parsing_state == LotParseState.Info):
            next_parsing_state = LotParseState.Result

            next_response = scrapy.http.FormRequest(
                url = siteURL + paths['lots_result'],
                formdata = {'id' : str(current_index), 'auction_id': str(response.meta['result_id']), 'revision_id': '0'},
                callback=self.parse_lot_item,
                headers={'X-Requested-With': 'XMLHttpRequest'},
                dont_filter=True
            )            
            next_response.meta['current_index'] = current_index

            # response.meta['state'] = LotParseState.NoResult
                    # response.meta['result_id'] = event['link'].split('/')[-1]
            # # print('index ' + str(next_index))
            # tender['lot_items'] = lot_items
            # # next_response.meta['lot_items'] = lot_items
            # next_response.meta['current_index'] = next_index
            # next_response.meta['tender'] = tender
            # # print('yield next_response')
            # return next_response

        elif parsing_state in (LotParseState.Result, LotParseState.NoResult):
            if parsing_state == LotParseState.Result:
                next_parsing_state = LotParseState.Info
            else:
                next_parsing_state = LotParseState.NoResult
            next_index = current_index + 1
            # print('len = ' + str(len(lot_items)) + " index = " + str(next_index))
            if (len(lot_items) <= next_index):
                # tender['lots'] = lot_items
                # print('yield tender')
                return tender
                # return

            # print('post index')
            next_response = scrapy.http.FormRequest(
                url = siteURL + paths['lot'],
                formdata = {'id' : str(next_index), 'auction_id': str(tender['id']), 'revision_id': '0'},
                callback=self.parse_lot_item,
                headers={'X-Requested-With': 'XMLHttpRequest'},
                dont_filter=True
            )
            next_response.meta['current_index'] = next_index
         
        if parsing_state != LotParseState.NoResult:
            response.meta['result_id'] = tender['events'][-1]['link'].split('/')[-1]
            # next_response.meta['event_id'] = response.meta['event_id']

        tender['lot_items'] = lot_items
        next_response.meta['tender'] = tender
        next_response.meta['state'] = next_parsing_state
        return next_response