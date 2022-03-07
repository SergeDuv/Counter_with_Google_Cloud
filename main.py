import datetime
import requests
from google.cloud import bigquery
from config import tg_chat_id, tg_bot_token, bq_table_name
 
def main(request, report='', page='', ip='', user_agent=''):
    if 'report' in request.args:
        report = request.args['report']
    if 'page' in request.args:
        page = request.args['page']   
    #for headers in request.headers:  header = header + str(headers)  # получить все заголовки
    ip = request.headers['X-Forwarded-For']
    user_agent = request.headers['User-Agent']
 
    if report != '':
        return stream_bq(report, page, ip, user_agent)
    else:
        url = 'https://api.telegram.org/bot' + tg_bot_token + '/sendMessage?chat_id=' + tg_chat_id + '&text=google cloud functions powerbi error: event without parameters received'
        response = requests.get(url)
        return 'event without parameters received'
    
 
def stream_bq(report, page, ip, user_agent):
    bq_client = bigquery.Client()
    bq_table = bq_client.get_table(bq_table_name)
    row = [{'report': report,
            'page': page,
            'ip': ip,
            'timestamp': datetime.datetime.utcnow(),
            'user_agent': user_agent  }]
    bq_client.insert_rows(bq_table, row)
    return 'ok'
