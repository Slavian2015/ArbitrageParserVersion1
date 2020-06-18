import json
import hashlib
import requests
import time
from urllib.parse import urlencode
import os
import pandas as pd

main_path_data = os.path.abspath(r"C:/inetpub/wwwroot/Arbitrage/data")
s_path_data = os.path.abspath("./data")

a_file = open(main_path_data + "\\keys.json", "r")
json_object = json.load(a_file)
a_file.close()

input_hot_key = json_object["3"]['key']
input_hot_api = json_object["3"]['api']


def hot_w(para):

    now = int(round(time.time()))
    if input_hot_key != "Api key" and input_hot_api != "Api secret":
        d ={
            'api_key': str(input_hot_key),
            'market': str(para),
            'start_time': int('0000000001'),
            'end_time': int(now),
            'offset': int(0),
            'limit': int(10),
            'side': int(1),
        }

        L_b = {}
        for b in sorted(d, reverse=False):
            L_b.update({b: d[b]})
        L_b.update({'secret_key': input_hot_api})

        er = urlencode(L_b)

        er = er.replace('%2F','/')
        result = hashlib.md5(er.encode())
        sign = result.hexdigest().upper()


        L_b2 = {}
        for b in sorted(d, reverse=False):
            L_b2.update({b: d[b]})

        L_b2.update({'sign': sign})

        url = urlencode(L_b2)

        url = url.replace('%2F', '/')
        url='https://api.hotbit.io/api/v1/order.finished?'+url

        return url
    else:
        return ''
def hot_open(para):

    if input_hot_key != "Api key" and input_hot_api != "Api secret":
        d ={
            'api_key': str(input_hot_key),
            'market': str(para),
            'offset': int(0),
            'limit': int(10),
        }

        L_b = {}
        for b in sorted(d, reverse=False):
            L_b.update({b: d[b]})
        L_b.update({'secret_key': input_hot_api})

        er = urlencode(L_b)
        er = er.replace('%2F','/')
        result = hashlib.md5(er.encode())
        sign = result.hexdigest().upper()


        L_b2 = {}
        for b in sorted(d, reverse=False):
            L_b2.update({b: d[b]})

        L_b2.update({'sign': sign})

        url = urlencode(L_b2)

        url = url.replace('%2F', '/')
        url='https://api.hotbit.io/api/v1/order.pending?'+url

        return url
    else:
        return ''
def hot_finished(para, order_id):
    response = requests.request("GET", hot_w(para))
    obj = json.loads(response.text)
    check = []
    for i in obj['result']['records']:
        if i['id'] == int(order_id):
            check.append("yes")
            file = open(s_path_data + "\\finished_orders.json", "r")
            data = json.load(file)
            file.close()
            if str(i['id']) in data['hot']:
                data['hot'][str(i['id'])] = i['amount']
            else:
                data['hot'].update({str(i['id']): i['amount']})
            f = open(s_path_data + "\\finished_orders.json", "w")
            json.dump(data, f)
            f.close()
        else:
            pass
    if not check:
        response = requests.request("GET", hot_open(para))
        obj = json.loads(response.text)
        print('RESP :', obj)

        file = open(s_path_data + "\\finished_orders.json", "r")
        data = json.load(file)
        file.close()

        for k,v in obj['result']:
            if not v['records']:
                pass
            else:
                for dict in v['records']:
                    finished = float(dict['amount']) - float(dict['left'])
                    if finished > 0:
                        if str(dict['id']) in data['hot']:
                            data['hot'][str(dict['id'])] = finished
                        else:
                            data['hot'].update({str(dict['id']):finished})
                    else:
                        pass

        f = open(s_path_data + "\\finished_orders.json", "w")
        json.dump(data, f)
        f.close()
    else:
        pass
    return

def live_finished(para, order_id):
    import hmac
    import json
    import hashlib
    from collections import OrderedDict



    a_file = open(main_path_data + "\\keys.json", "r")
    json_object = json.load(a_file)
    a_file.close()

    input1 = json_object["2"]['key']
    input2 = json_object["2"]['api']


    if input1 != "Api key" and input2 != "Api secret":
        # Свой класс исключений
        class ScriptError(Exception):
            pass

        class ScriptQuitCondition(Exception):
            pass

        order = OrderedDict([('currencyPair', para), ('endRow', 10)])

        encoded_data = urlencode(order)

        sign = hmac.new(input2.encode(), msg=encoded_data.encode(), digestmod=hashlib.sha256).hexdigest().upper()
        headers = {"Api-key": input1, "Sign": sign}
        url = 'https://api.livecoin.net/exchange/client_orders' + '?' + encoded_data

        response = requests.request("GET", url, headers=headers)
        obj = json.loads(response.text)

        for i in obj['data']:
            if i['id'] == int(order_id):
                if i['orderStatus'] == 'EXECUTED':

                    file = open(s_path_data + "\\finished_orders.json", "r")
                    data = json.load(file)
                    file.close()
                    if str(i['id']) in data['live']:
                        data['live'][str(i['id'])] = i['quantity']
                    else:
                        data['live'].update({str(i['id']): i['quantity']})
                    f = open(s_path_data + "\\finished_orders.json", "w")
                    json.dump(data, f)
                    f.close()
                else:
                    file = open(s_path_data + "\\finished_orders.json", "r")
                    data = json.load(file)
                    file.close()


                    finished = float(i['quantity']) - float(i['remainingQuantity'])
                    if finished > 0:
                        if str(i['id']) in data['live']:
                            data['live'][str(i['id'])] = finished
                        else:
                            data['live'].update({str(i['id']): finished})
                    else:
                        pass

                    f = open(s_path_data + "\\finished_orders.json", "w")
                    json.dump(data, f)
                    f.close()
            else:
                pass
        return
    else:
        return
def alfa_finished(Vol3,order_id):
    import hmac
    from time import time

    a_file = open(main_path_data + "\\keys.json", "r")
    json_object = json.load(a_file)
    a_file.close()

    input1 = json_object["1"]['key']
    input2 = json_object["1"]['api']

    if input1 != "Api key" and input2 != "Api secret":
        # Свой класс исключений
        class ScriptError(Exception):
            pass

        class ScriptQuitCondition(Exception):
            pass


        def get_auth_headers(self):
            msg = input1
            sign = hmac.new(input2.encode(), msg.encode(), digestmod='sha256').hexdigest()

            return {
                'X-KEY': input1,
                'X-SIGN': sign,
                'X-NONCE': str(int(time() * 1000)),
            }

        response = requests.get('https://btc-alpha.com/api/v1/orders/own/', headers=get_auth_headers({}))
        obj = json.loads(response.text)
        obj = obj[:10]
        print(obj)


        for i in obj:
            if i['id'] == order_id:
                if i['status'] == 3:
                    file = open(s_path_data + "\\finished_orders.json", "r")
                    data = json.load(file)
                    file.close()
                    if str(i['id']) in data['alfa']:
                        data['alfa'][str(i['id'])] = Vol3
                    else:
                        data['alfa'].update({str(i['id']): Vol3})
                    f = open(s_path_data + "\\finished_orders.json", "w")
                    json.dump(data, f)
                    f.close()
                elif i['status'] == 1:
                    file = open(s_path_data + "\\finished_orders.json", "r")
                    data = json.load(file)
                    file.close()


                    finished = Vol3 - float(i['amount'])
                    if finished > 0:
                        if str(i['id']) in data['alfa']:
                            data['alfa'][str(i['id'])] = finished
                        else:
                            data['alfa'].update({str(i['id']): finished})
                    else:
                        pass

                    f = open(s_path_data + "\\finished_orders.json", "w")
                    json.dump(data, f)
                    f.close()

                else:
                    pass




    else:
        return ["ОШИБКА"]



def main():
    vilki2 = pd.read_csv(main_path_data + "\\vilki2.csv")

    for ind in vilki2.index:
        if vilki2['birga_y'][ind] == 'live':
            para = str(vilki2['valin_y'][ind])+str(vilki2['valout_y'][ind])
            live_finished(para, str(vilki2['order_id'][ind]))
        elif vilki2['birga_y'][ind] == 'hot':
            para = str(vilki2['valin_y'][ind]) + '/' +str(vilki2['valout_y'][ind])
            hot_finished(para,str(vilki2['order_id'][ind]))
        else:
            alfa_finished(vilki2['Vol3'][ind],str(vilki2['order_id'][ind]))
    return
