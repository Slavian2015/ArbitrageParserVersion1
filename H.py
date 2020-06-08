import websocket
import time
import zlib
import json
import os
import sqlite3
main_path_data = os.path.abspath("./data")

try:
    import thread
except ImportError:
    import _thread as thread

data = b'{"method":"depths.subscribe","params":[["PZMUSDT",100,"0.0000001"],["PZMBTC",100,"0.0000001"]],"id":100}'

def my_hotbit():
    def on_message(ws, message):
        def receiv(*args):
            while True:
                rep2 = zlib.decompress(message, 16 + zlib.MAX_WBITS)
                rep2 = str(rep2, 'utf-8')
                rep2 = json.loads(rep2)
                print(rep2, '\n', '\n')
                if rep2['method'] == 'depth.update':
                    print('#################',rep2['params'][2])
                    if rep2['params'][2] == 'PZMBTC':
                        b = rep2['params'][1]["bids"][:5]
                        a = rep2['params'][1]["asks"][:5]

                        Hot_buy = {}
                        for i in b:
                            if not Hot_buy:
                                Hot_buy.update({i[0]: float(i[1])})
                            else:
                                sump = float(i[1]) + float(list(Hot_buy.values())[-1])
                                Hot_buy.update({i[0]: float(sump)})

                        Hot_sell = {}
                        for i in a:
                            if not Hot_sell:
                                Hot_sell.update({i[0]: float(i[1])})
                            else:
                                sump = float(i[1]) + float(list(Hot_sell.values())[-1])
                                Hot_sell.update({i[0]: float(sump)})

                        Hot_PU = []
                        for k, v in Hot_sell.items():
                            Hot_PU.append(('hot', 'BTC', 'PZM', 'buy', k, v))

                        for k, v in Hot_buy.items():
                            Hot_PU.append(('hot', 'PZM', 'BTC', 'sell', k, v))

                        conn = sqlite3.connect(main_path_data + "\\hot.db")
                        cursor = conn.cursor()
                        sql = 'DELETE FROM PZMBTC'
                        cursor.execute(sql)

                        cursor.executemany("INSERT INTO PZMBTC VALUES (?,?,?,?,?,?)", Hot_PU)
                        conn.commit()
                        conn.close()

                    elif rep2['params'][2] == 'PZMUSDT':
                        b = rep2['params'][1]["bids"][:5]
                        a = rep2['params'][1]["asks"][:5]

                        Hot_buy = {}
                        for i in b:
                            if not Hot_buy:
                                Hot_buy.update({i[0]: float(i[1])})
                            else:
                                sump = float(i[1]) + float(list(Hot_buy.values())[-1])
                                Hot_buy.update({i[0]: float(sump)})

                        Hot_sell = {}
                        for i in a:
                            if not Hot_sell:
                                Hot_sell.update({i[0]: float(i[1])})
                            else:
                                sump = float(i[1]) + float(list(Hot_sell.values())[-1])
                                Hot_sell.update({i[0]: float(sump)})

                        Hot_PU = []
                        for k, v in Hot_sell.items():
                            Hot_PU.append(('hot', 'USDT', 'PZM', 'buy', k, v))

                        for k, v in Hot_buy.items():
                            Hot_PU.append(('hot', 'PZM', 'USDT', 'sell', k, v))

                        conn = sqlite3.connect(main_path_data + "\\hot.db")
                        cursor = conn.cursor()
                        sql = 'DELETE FROM PZMUSDT'
                        cursor.execute(sql)

                        cursor.executemany("INSERT INTO PZMUSDT VALUES (?,?,?,?,?,?)", Hot_PU)
                        conn.commit()
                        conn.close()
                    else:
                        pass
                else:
                    pass
                time.sleep(0.4)

        thread.start_new_thread(receiv, ())


        # # print(message)
        # print(zlib.decompress(message, 16 + zlib.MAX_WBITS))
        # # print('Compressed data: ', json.load(message))

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("### closed ###")

    def on_open(ws):
        def run(*args):
            msg = data
            ws.send(msg)
            print('#################  SEND   ##################')
            while True:
                time.sleep(1)
            ws.close()
            print("thread terminating...")

        thread.start_new_thread(run, ())


    # if __name__ == "__main__":
    while True:
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp('wss://ws.hotbit.io',
                                  on_message = on_message,
                                  on_error = on_error,
                                  on_close = on_close)
        ws.on_open = on_open
        ws.run_forever()
        time.sleep(1)

my_hotbit()