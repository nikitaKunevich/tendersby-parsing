# -*- coding: utf-8 -*-

import scrapy
import json
import codecs
import urllib
import re
from datetime import datetime

# from config import siteURL, paths
import config
import consts
import sys, os

# import firebase_admin
# from firebase_admin import credentials
# from firebase_admin import db
# from config import firebase_db_config

# cred = credentials.Certificate('../../firebase_credentials.json')
# # default_app = firebase_admin.initialize_app(cred)
# firebase_admin.initialize_app(cred, {
#     'databaseURL': firebase_db_config['databaseURL']
# })
# ref = db.reference('public_resource')
# print(ref.get())
# users_ref = ref.child('users')
# users_ref.set({
#     'alanisawesome': {
#         'date_of_birth': 'June 23, 1912',
#         'full_name': 'Alan Turing'
#     },
#     'gracehop': {
#         'date_of_birth': 'December 9, 1906',
#         'full_name': 'Grace Hopper'
#     }
# })

'''
only first lot_item is filled last lot info - FIXED
check if results are parsed
'''

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

class LotParseState():
    NoResult = 0
    Info = 1
    Result = 2

class IcetradeSpider(scrapy.Spider):

    # constants
    name = "icetrade"
    tender_field_map = {
        "af-operator_site":"site_address", #Адрес сайта, обеспечивающего доступ на ЭТП
        #["method_of_negotiation"],
        #"operator_info" ".af-operator_site", #Данные оператора	
        "af-industry":"industry", #Отрасль
        "af-title":"short_description", #Краткое описание предмета закупки	

        #customer info
        "af-hold_by":"procurement_held_by", #Закупка проводится	
        "af-customer_data": "customer_name" , #Полное наименование заказчика, место нахождения организации, УНП	
        "af-customer_contacts":"contacts" , #Фамилии, имена и отчества, номера телефонов работников заказчика	
        "af-organizer_data":"organizer_data",#Полное наименование организатора, место нахождения организации, УНП
        "af-organizer_contacts":"organizer_contacts", #Фамилии, имена и отчества, номера телефонов работников организатора
        "af-organizer_salary": "organizer_payment", #Размер оплаты услуг организатора,

        #procurement_info
        "af-created":"creation_date", #Дата размещения приглашения	
        "af-request_start":"start_date",
        "af-request_end":"end_date", #Дата и время окончания приема предложений	
        "af-documentation_data": "auction_documentation_data", #Сроки, место и порядок предоставления конкурсных документов
        # "": "documentation_provisioning_info",
        # "": "documentation_price",
        "af-proposals_data":"proposals_data",#Место и порядок представления конкурсных предложений
        "af-currency":"proposal_estimated_price", #Общая ориентировочная стоимость закупки	
        "af-documentation_price":"documentation_price", #Цена конкурсных документов
        # "": "proposal_closing_time",
        "af-qualification" :"qualification_terms", # Квалификационные требования
        "af-participator_demand": "proposal_participators_requirements",#Требования к оставу участников	
        # "": "proposal_submission_address",
        # "": "proposal_opening_time",
        # "":"proposal_opening_address",
        # "": "preliminary_qualification_terms",
        "af-others": "other_information" #Иные сведения	
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

    def __init__(self, number=None, start=None, limit=None):
        self.fl = codecs.open(config.headers_file, 'a', 'utf-8')
        self.lot_fl = codecs.open(config.lot_headers_file, 'a', 'utf-8')
        self.tender_number = number
        self.tender_start = start
        self.tender_limit = limit

    
    def start_requests(self):
        # number = None
        # self.tender_number = 339947
        self.tender_number = None
        # with results 400520
        # self.tender_start = 400520
        self.tender_start = 450008
        self.tender_limit = 2000
        # limit = 10000#10000

        if self.tender_number:
            tender_response = scrapy.Request(url = config.siteURL + config.paths['tender'] + str(self.tender_number), callback=self.parse_tender)
            # self.state['items_count'] = self.state.get('items_count', 0) + 1
            yield tender_response       
        else:
            if not self.tender_start or not self.tender_limit:
                yield {}
            for num in xrange(self.tender_limit):
                tender_response = scrapy.Request(url = config.siteURL + config.paths['tender'] + str(self.tender_start + num), callback=self.parse_tender)
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

        self.fl.write(str(tender['id']) + "\n")

        self.process_tender_info(tender, root_block)
        self.process_files(tender, root_block)
        self.process_events(tender, root_block)
        yield self.process_lots(tender, root_block)


    def process_tender_info(self, tender, root_block):

        #tender info

        # for key in self.tender_field_map:
        #     if self.tender_field_map[key] == "":
        #         continue

        #     value = root_block.css(self.tender_field_map[key] + " td.afv::text").extract_first()
        #     if not value:
        #         continue

        #     tender[key] = root_block.css(self.tender_field_map[key] + " td.afv::text").extract_first().strip()

        field_items = root_block.css('.af')
        
        for elem in field_items:
            classes = elem.root.attrib['class'].split()
            for cl in classes:
                # if cl != "af" and cl.startswith("af"):
                if cl == "af" or not cl.startswith("af-") or cl == "af-files":
                    continue

                value = elem.css('td.afv::text').extract_first()
                if not value:
                    continue

                if cl not in self.tender_field_map:
                    tender[cl] = value.strip()
                    # self.unused_headers.add(cl)
                    self.add_unused_header(cl)
                else:    
                    tender[self.tender_field_map[cl]] = value.strip()
        #tender type
        tender['type'] = root_block.css('tr.fst b::text').extract_first().strip()


    def add_unused_header(self, header):
        # fl = open(config.headers_file, 'a')
        self.fl.write(header + "\n")
    
    def add_lot_unused_header(self, header):
        self.lot_fl.write(header + "\n")
        # open(config.lot_headers_file

    def process_files(self, tender, root_block):
        files_selectors = root_block.css('td.af-files p')
        tender['files'] = []
        for file in files_selectors:
            file_object = {}
            link = file.css('a::attr(href)').extract_first()
            if link:
                file_object['link'] = link.strip()
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
        # print('process_lots')
        lot_items = []
        #common lot data
        lot_selectors = root_block.css('table#lots_list .af')
        for lot in lot_selectors:
            lot_item = {}
            lot_item['id'] = lot.css('td:nth-child(1)::text').extract_first().strip()
            lot_item['item_name'] = lot.css('td:nth-child(2)::text').extract_first().strip()
            quantity = lot.css('td:nth-child(3) span::text').extract_first()
            lot_item['quantity'] = "".join(quantity.split())
            lot_item['quantity_measurement'] = lot.css('td:nth-child(3)::text').extract()[1].strip()[:-1]
            try:
                approx_price = lot.css('td:nth-child(3) span::text').extract()[1]
                lot_item['approx_price'] = "".join(approx_price.split())
                lot_item['approx_price_currency'] = lot.css('td:nth-child(3)::text').extract()[3].strip()
            except:
                lot_item['approx_price'] = ''
                lot_item['approx_price_currency'] = ""

            lot_item['status'] = lot.css('td:nth-child(4)::text').extract_first().strip()
            lot_item['result'] = {}
            lot_items.append(lot_item)
            
        state = LotParseState.NoResult
        result_id = None
        for event in reversed(tender['events']):
            if (event['text'] == consts.RESULT_EVENT_LABEL):
                state = LotParseState.Info
                result_id = event['link'].split('/')[-1]
                break
        tender['finished'] = bool(result_id)
        if state == LotParseState.Info:
            tender['lot_items'] = lot_items
            result_response = scrapy.Request(
                url = config.siteURL + config.paths['result'] + str(result_id),
                callback=self.parse_result
            )
            result_response.meta['tender'] = tender
            result_response.meta['state'] = state
            result_response.meta['result_id'] = result_id
            return result_response
        elif (len(lot_selectors)):
            tender['lot_items'] = lot_items
            response = scrapy.http.FormRequest(
                url = config.siteURL + config.paths['lot'],
                formdata = {'id': str(lot_items[0]['id']), 'auction_id': str(tender['id']), 'revision_id': '0'},
                headers={'X-Requested-With': 'XMLHttpRequest'},
                dont_filter=True,
                callback=self.parse_lot_item
            )
            response.meta['tender'] = tender
            response.meta['current_index'] = 0
            response.meta['state'] = state
            response.meta['result_id'] = result_id
            # for event in reversed(tender['events']):
            #     if (event['text'] == consts.RESULT_EVENT_LABEL):
            #         response.meta['state'] = LotParseState.Info
            #         response.meta['result_id'] = event['link'].split('/')[-1]
            #         break

            return response
        else:
            return tender

    # to fix
    def parse_result(self, response):
        # result lot data

        tender = response.meta['tender']
        lot_items = tender['lot_items']
        lot_result_selectors = response.css('table#lots_list tr')
        i=0
        for lot_sel in lot_result_selectors:
            if 'id' not in lot_sel.root.attrib:
                continue

            lot_id = lot_sel.css('td:nth-child(1)::text').extract_first().strip()
            # for current_lot in lot_items:
            #     if current_lot['id'] != lot_id:
            #         continue
            current_lot = lot_items[i]
            assert(current_lot['id'] == lot_id)

            current_lot['contract_item'] = lot_sel.css('td:nth-child(2)::text').extract_first().strip()
            current_lot['contract_winner'] = lot_sel.css('td:nth-child(3)::text').extract_first().strip()
            price_sel_v1 = lot_sel.css('td:nth-child(4) span::text').extract_first()
            # if not price_sel_v1:
            #     price_sel = lot.css('td:nth-child(3)::text text').extract_first()
            try:
                current_lot['contract_price'] = "".join(price_sel_v1.split())
                current_lot['contract_currency'] = lot_sel.css('td:nth-child(4)::text').extract()[1].strip()
            except:
                current_lot['contract_price'] = ""
                current_lot['contract_currency'] = ""
            i+=1

        if (len(lot_result_selectors)):
            lot_response = scrapy.http.FormRequest(
                url = config.siteURL + config.paths['lot'],
                formdata = {'id': str(lot_items[0]['id']), 'auction_id': str(tender['id']), 'revision_id': '0'},
                headers={'X-Requested-With': 'XMLHttpRequest'},
                dont_filter=True,
                callback=self.parse_lot_item
            )
            lot_response.meta['tender'] = tender
            lot_response.meta['current_index'] = 0
            lot_response.meta['state'] = response.meta['state']
            lot_response.meta['result_id'] = response.meta['result_id']

            return lot_response
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

        rows = response.css('tr.lotSubRow')
        for row in rows:
            header = row.css('th::text').extract_first().strip()
            if header not in self.lot_field_map:
                # self.unused_lot_headers.add(header)
                self.add_lot_unused_header(header)
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
                current_lot['result'][self.lot_field_map[header]] = data
            else:
                current_lot[self.lot_field_map[header]] = data

        #400521
        if (parsing_state == LotParseState.Info):
            self.lot_fl.write(str(tender['id']) + ":" + str(current_index) + "\n")

            next_parsing_state = LotParseState.Result
            if 'result_id' not in response.meta:
                print("no result_id tender_id: %s, lot %s" % (tender['id'], current_index))
            next_response = scrapy.http.FormRequest(
                url = config.siteURL + config.paths['lots_result'],
                formdata = {'id' : str(current_index), 'auction_id': str(response.meta['result_id']), 'revision_id': '0'},
                callback=self.parse_lot_item,
                headers={'X-Requested-With': 'XMLHttpRequest'},
                dont_filter=True
            )            
            next_response.meta['current_index'] = current_index

        elif parsing_state in (LotParseState.Result, LotParseState.NoResult):
            if parsing_state == LotParseState.Result:
                next_parsing_state = LotParseState.Info
            else:
                next_parsing_state = LotParseState.NoResult
            next_index = current_index + 1
            if (len(lot_items) <= next_index):
                return tender

            next_response = scrapy.http.FormRequest(
                url = config.siteURL + config.paths['lot'],
                formdata = {'id' : str(next_index), 'auction_id': str(tender['id']), 'revision_id': '0'},
                callback=self.parse_lot_item,
                headers={'X-Requested-With': 'XMLHttpRequest'},
                dont_filter=True
            )
            next_response.meta['current_index'] = next_index
         
        if parsing_state != LotParseState.NoResult:
            next_response.meta['result_id'] = tender['events'][-1]['link'].split('/')[-1]

        tender['lot_items'] = lot_items
        next_response.meta['tender'] = tender
        next_response.meta['state'] = next_parsing_state
        return next_response