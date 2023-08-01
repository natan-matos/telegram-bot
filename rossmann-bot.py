import pandas as pd
import json
import requests
import os

from flask import Flask, request, Response

# get info
#https://api.telegram.org/bot6330025305:AAGVrpNJ5P6UH6KZ6hGtYjHqDUNxpMW8mgg/getMe

#local host 
#https://001ee589f017f2.lhr.life
#https://8610ba0698b982.lhr.life

# webhook
#https://api.telegram.org/bot6330025305:AAGVrpNJ5P6UH6KZ6hGtYjHqDUNxpMW8mgg/setWebhook?url=https://telegram-bot-6jqb.onrender.com
# https://0b09c2ca3edf1a.lhr.life

#constans
token = '6330025305:AAGVrpNJ5P6UH6KZ6hGtYjHqDUNxpMW8mgg'


def send_message(chat_id, text):
    url = 'https://api.telegram.org/bot{}/'.format( token )
    url = url + 'sendMessage?chat_id={}'.format( chat_id )

    r = requests.post(url, json={'text': text})
    print('Satus Code{}'.format(r.status_code))

    return None

def load_data(store_id):
    # loading test data
    df10 = pd.read_csv('test.csv')
    df_store_raw  = pd.read_csv('store.csv')
    


    # merge test dataset + store
    df_test = pd.merge( df10, df_store_raw, how='left', on='Store' )
    df_test['Store'] = df_test['Store'].astype(int)
    # choose store for prediction
    df_test = df_test[df_test['Store']==store_id]

    if not df_test.empty:
        # remove closed days
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop( 'Id', axis=1 )


        # convert DataFrame into json
        data = json.dumps( df_test.to_dict( orient='records' ) )
    else:
        data = 'error'

    return data

def predict(data):
    # API Call
    url = 'https://rossmann-api-t2qp.onrender.com'
    header = {'Content-type': 'application/json'}
    r = requests.post(url, data=data, headers=header)
    print('Status Code {}'.format(r.status_code))

    if r.status_code == 200:
        try:
            response_json = r.json()
            df_response = pd.DataFrame(response_json)  # Convert JSON to DataFrame
            return df_response
        except ValueError:
            print('Error: Invalid response from the API')
    else:
        print('Error: API request failed')
    
    return None

def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']

    store_id = store_id.replace('/', '')

    try:
        store_id = int(store_id)
    except ValueError:
        send_message(chat_id, 'Store ID is Wrong')
        store_id = 'error'

    return chat_id, store_id

# API initialize
app = Flask(__name__)
@app.route('/', methods=['GET', 'POST'])

def index():
    if request.method == 'POST':
        message = request.get_json()
        chat_id, store_id = parse_message(message)

        if store_id != 'error':
            # loading data
            data = load_data(store_id)

            if data != 'error':
                # prediction
                df_response = predict(data)

                if df_response is not None:
                    # calculation
                    d2 = df_response[['store', 'prediction']].groupby('store').sum().reset_index()

                    # send message
                    msg = f'Store Number {d2["store"].values[0]} will sell ${d2["prediction"].values[0]:,.2f} in the next 6 weeks'
                    send_message(chat_id, msg)
                    return Response('Ok', status=200)

                else:
                    send_message(chat_id, 'Error: Invalid response from the API')
                    return Response('OK', status=200)

            else:
                send_message(chat_id, 'Store Not Available')
                return Response('OK', status=200)
        else:
            send_message(chat_id, 'Store ID is Wrong')
            return Response('OK', status=200)
    else:
        return '</h1> Rossmann Telegram Bot </h1>'
    
if __name__ == '__main__':
    port = os.environ.get('PORT', 500)
    app.run(host='0.0.0.0', port=port)