import pprint
import time
import sys
import urllib2
import json
import datetime
import time
import httplib
from decimal import Decimal
import big_query
import mysql

pp = pprint.PrettyPrinter(indent=4)

YAHOO_API_URL = u'http://finance.yahoo.com/webservice/v1/symbols/allcurrencies/quote?format=json'
CL_API_BASE_URL = 'http://apilayer.net/api/live'
CL_API_KEY = 'c5cf909e58313ea03dda01ffa45c01d5'
CL_CURRENCIES_TO_TRACK = ['USD', 'BTC', 'EUR', 'CAD', 'AUD', 'GBP']


def _poll_cl():
    for currency in CL_CURRENCIES_TO_TRACK:
        u = "%s?source=%s&access_key=%s" % (CL_API_BASE_URL, currency, CL_API_KEY)
        quotes = json.loads(urllib2.urlopen(u).read())['quotes']
        now = datetime.datetime.now()
        rows = map(lambda name: [name, quotes[name], now], quotes.keys())
        # big_query.insert_quotes("cl", rows)
        mysql.insert_quotes("cl", rows)

def _poll_yahoo():
    while True:
        try:
            quotes = json.loads(urllib2.urlopen(YAHOO_API_URL).read())
            now = datetime.datetime.now()
            def make_record(data):
                quote = data['resource']['fields']
                if len(quote['name']) >= 7 and quote['name'][3] == '/':
                    quote['name'] = quote['name'][:3] + quote['name'][4:]
                # pp.pprint([quote['name'], quote['price'], now])
                return [quote['name'], quote['price'], now]
            
            rows = map(make_record, quotes['list']['resources'])
            # big_query.insert_quotes("yahoo", rows)
            mysql.insert_quotes("yahoo", rows)
            break
        except urllib2.URLError:
            print 'URLError!'
            time.sleep(10)
            continue
        except urllib2.HTTPError, e:
            print 'HTTPError!'
            time.sleep(10)
        except httplib.HTTPException, e:
            print 'HTTPException!'
            time.sleep(10)
        except KeyError:
            print 'KeyError!'
            pp.pprint(quotes)
            time.sleep(10)
            continue

def cleanup(environment, start_response):
    try:
        mysql.remove_extraneous()
        start_response('200', [('Content-type', 'text/plain')])
    except RuntimeError as e:
        start_response('500', [('Content-type', 'text/plain')])
        return e
    return ''

def poll_yahoo(environment, start_response):
    try:
        _poll_yahoo()
        start_response('200', [('Content-type', 'text/plain')])
    except RuntimeError as e:
        start_response('500', [('Content-type', 'text/plain')])
        return e
    return ''

def poll_cl(environment, start_response):
    try:
        _poll_cl()
        start_response('200', [('Content-type', 'text/plain')])
    except RuntimeError as e:
        start_response('500', [('Content-type', 'text/plain')])
        return e
    return ''