import json
import concurrent.futures
import requests
import time
from random import choice
from fake_useragent import UserAgent
import os
import sqlite3


main_path_data = os.path.abspath("./data")


url2 = 'https://btc-alpha.com/api/v1/orderbook/PZM_USD'
url3 = 'https://btc-alpha.com/api/v1/orderbook/PZM_BTC'


urls = [
    url2,
    url3,
]

def refresh():

    def kurs():
        out = dict()
        CONNECTIONS = 100
        TIMEOUT = 2

        pro = ['94.154.208.248:80', '89.252.12.88:80', '13.66.220.17:80', '104.45.11.83:80']
        ua = UserAgent()
        proxy = choice(pro)
        PARAMS = {'User-Agent': ua.random, 'http': proxy, 'https': proxy}

        def load_url(url, timeout, params):
            ans = requests.get(url, data=params, timeout=timeout)
            return url, ans.json()

        with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            future_to_url = (executor.submit(load_url, url, TIMEOUT, PARAMS) for url in urls)

            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    data = future.result()
                except Exception as exc:
                    data = str(type(exc))
                finally:
                    out.update({data[0]:data[1]})

        # print(out)

        return out


    # dictionary = json.loads(kurs())

    dictionary2 = json.dumps(kurs())
    dictionary = json.loads(dictionary2)


    def ord(link, val1, val2, n):
        Alfa_sell = {}
        for i in dictionary[link]['sell'][:n]:
            if not Alfa_sell:
                Alfa_sell.update({i['price']: float(i['amount'])})
            else:
                # print(float(i['amount']))
                # print(float([*Alfa_sell.values()][-1]))
                sump = float(i['amount']) + float([*Alfa_sell.values()][-1])
                Alfa_sell.update({i['price']: sump})

        Alfa_buy = {}
        for i in dictionary[link]['buy'][:n]:
            if not Alfa_buy:
                Alfa_buy.update({i['price']: float(i['amount'])})
            else:
                sump = float(i['amount']) + float([*Alfa_buy.values()][-1])
                Alfa_buy.update({i['price']: sump})

        Alfa_sell = [*Alfa_sell.items()][:n]
        Alfa_buy = [*Alfa_buy.items()][:n]

        alfa_PU = []
        for i in Alfa_sell:
            alfa_PU.append(('alfa', val1, val2, 'buy', i[0], i[1]))
        for i in Alfa_buy:
            alfa_PU.append(('alfa', val2, val1, 'sell', i[0], i[1]))




        return alfa_PU



    conn = sqlite3.connect(main_path_data + "\\alfa.db")
    cursor = conn.cursor()

    sql = 'DELETE FROM PZMUSD'
    cursor.execute(sql)

    sql2 = 'DELETE FROM PZMBTC'
    cursor.execute(sql2)

    list = [('https://btc-alpha.com/api/v1/orderbook/PZM_USD', 'USD', 'PZM', 5),('https://btc-alpha.com/api/v1/orderbook/PZM_BTC', 'BTC', 'PZM', 5)]

    for i in list:
        if i[1] == 'USD':
            cursor.executemany("INSERT INTO PZMUSD VALUES (?,?,?,?,?,?)", ord(i[0],i[1],i[2],i[3]))
        else:
            cursor.executemany("INSERT INTO PZMBTC VALUES (?,?,?,?,?,?)", ord(i[0],i[1],i[2],i[3]))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    while True:
        try:
            t1 = time.time()
            refresh()
            t2 = time.time()
            print("ALL TIME :", t2-t1)
            time.sleep(1)
        except Exception as e:
            print(e)
            time.sleep(5)