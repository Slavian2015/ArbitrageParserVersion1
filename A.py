import socketio
import json
import os
import time
import sqlite3
from collections import OrderedDict

main_path_data = os.path.abspath("./data")

def my_Alfabit():
    sio = socketio.Client(logger=False, engineio_logger=False)


    @sio.on('book_PZM_BTC')
    def on_message(data):
        # print("PZM_BTC")
        if 'cancel_order_id' in data:
            if data['type'] == 'buy':
                new_regims_f = open(main_path_data + "\\alfa_buy.json", 'r')
                alfa_buy = json.load(new_regims_f)

                if data['price'] in alfa_buy:
                    alfa_buy[data['price']] = alfa_buy[data['price']] - data['amount']
                    my_data = OrderedDict(sorted(alfa_buy.items(), key=lambda t: t[0], reverse=True))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    result = dict(list(result.items())[0: 10])

                    f = open(main_path_data + "\\alfa_buy.json", "w")
                    json.dump(result, f)
                    f.close()

                    for k, v in alfa_buy.items():
                        conn = sqlite3.connect("kurses.db")
                        cursor = conn.cursor()
                        cursor.execute(
                            "UPDATE kurses SET price=?, amount=? WHERE para_valut = 'PZM/BTC' AND birga = 'alfa' AND direction = ?",
                            (k, v, data['type']))
                        conn.commit()
                        conn.close()
                        break
                else:
                    pass
            else:
                new_regims_f = open(main_path_data + "\\alfa_sell.json", 'r')
                alfa_sell = json.load(new_regims_f)

                if data['price'] in alfa_sell:

                    alfa_sell[data['price']] = alfa_sell[data['price']] - data['amount']
                    my_data = OrderedDict(sorted(alfa_sell.items(), key=lambda t: t[0], reverse=True))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    result = dict(list(result.items())[0: 10])

                    f = open(main_path_data + "\\alfa_sell.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    pass
        elif not data['trades']:
            print("PZM_BTC no trades")
            print(data)
            if data['type'] == 'buy':
                new_regims_f = open(main_path_data + "\\alfa_buy.json", 'r')
                alfa_buy = json.load(new_regims_f)
                if data['price'] in alfa_buy:
                    alfa_buy[data['price']] = alfa_buy[data['price']] + float(data['amount'])
                    my_data = OrderedDict(sorted(alfa_buy.items(), key=lambda t: t[0]), reverse=True)
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    result = dict(list(result.items())[0: 10])
                    f = open(main_path_data + "\\alfa_buy.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    alfa_buy[data['price']] = float(data['amount'])  # adata new price
                    my_data = OrderedDict(sorted(alfa_buy.items(), key=lambda t: t[0], reverse=True))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    f = open(main_path_data + "\\alfa_buy.json", "w")
                    result = dict(list(result.items())[0: 10])
                    json.dump(result, f)
                    f.close()
            else:
                new_regims_f = open(main_path_data + "\\alfa_sell.json", 'r')
                alfa_sell = json.load(new_regims_f)

                if data['price'] in alfa_sell:
                    alfa_sell[data['price']] = alfa_sell[data['price']] + float(data['amount'])
                    my_data = OrderedDict(sorted(alfa_sell.items(), key=lambda t: t[0]))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    result = dict(list(result.items())[0: 10])
                    f = open(main_path_data + "\\alfa_sell.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    alfa_sell[data['price']] = float(data['amount'])  # adata new price
                    my_data = OrderedDict(sorted(alfa_sell.items(), key=lambda t: t[0]))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    result = dict(list(result.items())[0: 10])
                    f = open(main_path_data + "\\alfa_sell.json", "w")
                    json.dump(result, f)
                    f.close()

        else:
            print("PZM_BTC")
            if data['type'] == 'sell':
                l = []
                for i in data['trades']:
                    l.append(float(i['amount']))
                summ = (sum(l))
                new_regims_f = open(main_path_data + "\\alfa_buy.json", 'r')
                alfa_buy = json.load(new_regims_f)

                if data['trades'][0]['price'] in alfa_buy:
                    alfa_buy[data['trades'][0]['price']] = alfa_buy[data['trades'][0]['price']] - summ
                    my_data = OrderedDict(sorted(alfa_buy.items(), key=lambda t: t[0], reverse=True))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    result = dict(list(result.items())[0: 10])

                    f = open(main_path_data + "\\alfa_buy.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    pass
            else:
                l = []
                for i in data['trades']:
                    l.append(float(i['amount']))
                summ = (sum(l))
                new_regims_f = open(main_path_data + "\\alfa_sell.json", 'r')
                alfa_sell = json.load(new_regims_f)

                if data['trades'][0]['price'] in alfa_sell:
                    alfa_sell[data['trades'][0]['price']] = alfa_sell[data['trades'][0]['price']] - summ
                    my_data = OrderedDict(sorted(alfa_sell.items(), key=lambda t: t[0]))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    result = dict(list(result.items())[0: 10])

                    f = open(main_path_data + "\\alfa_sell.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    pass

    @sio.on('book_PZM_USD')
    def on_message(data):
        # print("PZM_USD")
        if 'cancel_order_id' in data:
            print("CANCELED :", '\n', data)
            if data['type'] == 'buy':
                new_regims_f = open(main_path_data + "\\alfa_buy_PU.json", 'r')
                alfa_buy = json.load(new_regims_f)

                if data['price'] in alfa_buy:

                    alfa_buy[data['price']] = alfa_buy[data['price']] - data['amount']
                    my_data = OrderedDict(sorted(alfa_buy.items(), key=lambda t: t[0], reverse=True))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    result = dict(list(result.items())[0: 10])

                    f = open(main_path_data + "\\alfa_buy_PU.json", "w")
                    json.dump(result, f)
                    f.close()

                else:
                    pass
            else:
                new_regims_f = open(main_path_data + "\\alfa_sell_PU.json", 'r')
                alfa_sell = json.load(new_regims_f)

                if data['price'] in alfa_sell:

                    alfa_sell[data['price']] = alfa_sell[data['price']] - data['amount']
                    my_data = OrderedDict(sorted(alfa_sell.items(), key=lambda t: t[0], reverse=True))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    result = dict(list(result.items())[0: 10])

                    f = open(main_path_data + "\\alfa_sell_PU.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    pass
        elif not data['trades']:
            print("PZM_USD no trades")
            print(data)
            if data['type'] == 'buy':
                new_regims_f = open(main_path_data + "\\alfa_buy_PU.json", 'r')
                alfa_buy = json.load(new_regims_f)
                if data['price'] in alfa_buy:
                    alfa_buy[data['price']] = alfa_buy[data['price']] + float(data['amount'])
                    my_data = OrderedDict(sorted(alfa_buy.items(), key=lambda t: t[0], reverse=True))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    f = open(main_path_data + "\\alfa_buy_PU.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    alfa_buy[data['price']] = float(data['amount'])  # adata new price
                    my_data = OrderedDict(sorted(alfa_buy.items(), key=lambda t: t[0], reverse=True))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    f = open(main_path_data + "\\alfa_buy_PU.json", "w")
                    json.dump(result, f)
                    f.close()
            else:
                new_regims_f = open(main_path_data + "\\alfa_sell_PU.json", 'r')
                alfa_sell = json.load(new_regims_f)

                if data['price'] in alfa_sell:
                    alfa_sell[data['price']] = alfa_sell[data['price']] + float(data['amount'])
                    my_data = OrderedDict(sorted(alfa_sell.items(), key=lambda t: t[0]))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    f = open(main_path_data + "\\alfa_sell_PU.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    alfa_sell[data['price']] = float(data['amount'])  # adata new price
                    my_data = OrderedDict(sorted(alfa_sell.items(), key=lambda t: t[0]))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)
                    f = open(main_path_data + "\\alfa_sell_PU.json", "w")
                    json.dump(result, f)
                    f.close()

            # json_object["alfa"]['PZM/BTC'][data['type']]['price'] = data['price']
            # json_object["alfa"]['PZM/BTC'][data['type']]['amount'] = data['amount']
        else:
            print("PZM_USD")
            if data['type'] == 'sell':
                l = []
                for i in data['trades']:
                    l.append(float(i['amount']))
                summ = (sum(l))
                new_regims_f = open(main_path_data + "\\alfa_buy_PU.json", 'r')
                alfa_buy = json.load(new_regims_f)

                if data['trades'][0]['price'] in alfa_buy:
                    alfa_buy[data['trades'][0]['price']] = alfa_buy[data['trades'][0]['price']] - summ
                    my_data = OrderedDict(sorted(alfa_buy.items(), key=lambda t: t[0], reverse=True))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)

                    f = open(main_path_data + "\\alfa_buy_PU.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    pass
            else:
                l = []
                for i in data['trades']:
                    l.append(float(i['amount']))
                summ = (sum(l))
                new_regims_f = open(main_path_data + "\\alfa_sell_PU.json", 'r')
                alfa_sell = json.load(new_regims_f)

                if data['trades'][0]['price'] in alfa_sell:
                    alfa_sell[data['trades'][0]['price']] = alfa_sell[data['trades'][0]['price']] - summ
                    my_data = OrderedDict(sorted(alfa_sell.items(), key=lambda t: t[0]))
                    result = dict((k, v) for k, v in my_data.items() if v > 0.1)

                    f = open(main_path_data + "\\alfa_sell_PU.json", "w")
                    json.dump(result, f)
                    f.close()
                else:
                    pass


    @sio.event()
    def stream(event):
        print('GOT DATA:')
        print(event)

    @sio.event()
    def connect():
        print("CONNECTED")
        # print(book_BTC_USD)

    @sio.event()
    def disconnect():
        print("DISCONNECTED")

    while True:
        try:
            print('CONNECTING')
            sio.connect('wss://btc-alpha.com', transports='websocket', namespaces='/')
            sio.wait()

        except Exception as e:
            print(e)
            time.sleep(1)


my_Alfabit()
