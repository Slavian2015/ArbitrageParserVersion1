import websocket
import time
import zlib
import json
import os
import pandas as pd
from collections import OrderedDict
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
                # print(rep2, '\n', '\n')
                if rep2['method'] != 'depth.update':
                    pass
                else:
                    print('#################',rep2['params'][2])
                    if rep2['params'][0] == 'False':
                        pass
                    else:
                        if 'bids' not in rep2['params'][1]:
                            pass
                        elif 'asks' not in rep2['params'][1]:
                            pass
                        else:
                            if rep2['params'][2] == 'PZMBTC':
                                b = rep2['params'][1]["bids"][:5]
                                a = rep2['params'][1]["asks"][:5]
                                print('############################', rep2['params'][1]["asks"][:2])
                                print(rep2, '\n', '\n')
                                # print('###########   BBB :',b)
                                # print('###########   BBB full:', rep2['params'][1]["bids"])
                                #
                                # hb = OrderedDict(sorted(b.items(), key=lambda t: t[1]), reverse=False)
                                # hbb = dict((k, v) for k, v in hb.items() if v > 0.1)
                                #
                                # ha = OrderedDict(sorted(a.items(), key=lambda t: t[0]), reverse=True)
                                # haa = dict((k, v) for k, v in ha.items() if v > 0.1)

                                # bh = float(list(hbb.keys())[0])
                                # ba = float(list(haa.keys())[0])
                                #
                                # # print(bh)
                                # # print(ba)
                                #
                                # if bh > ba:
                                #     del hbb[bh]
                                # else:
                                #     pass

                                Hot_buy = {}
                                for i in b:
                                    if not Hot_buy:
                                        Hot_buy.update({i[0]: float(i[1])})
                                    else:
                                        if i[0]== 'reverse':
                                            pass
                                        else:
                                            sump = float(i[1]) + float(list(Hot_buy.values())[-1])
                                            Hot_buy.update({i[0]: float(sump)})

                                Hot_sell = {}
                                for i in a:
                                    if not Hot_sell:
                                        Hot_sell.update({i[0]: float(i[1])})
                                    else:
                                        if i[0]== 'reverse':
                                            pass
                                        else:
                                            sump = float(i[1]) + float(list(Hot_sell.values())[-1])
                                            Hot_sell.update({i[0]: float(sump)})

                                Hot_PU = []
                                for k, v in Hot_sell.items():
                                    Hot_PU.append(('hot', 'BTC', 'PZM', 'buy', k, v))

                                for k, v in Hot_buy.items():
                                    Hot_PU.append(('hot', 'PZM', 'BTC', 'sell', k, v))

                                columns = ['birga', 'valin', 'valout', 'direction', 'rates', 'volume']
                                df = pd.DataFrame(Hot_PU, columns=columns)
                                # print(df)
                                os.remove(main_path_data + "\\hot_bd_PB.csv")
                                df.to_csv(main_path_data + "\\hot_bd_PB.csv", index=False)
                            elif rep2['params'][2] == 'PZMUSDT':
                                b = rep2['params'][1]["bids"][:5]
                                a = rep2['params'][1]["asks"][:5]
                                # print('############################', rep2['params'][1]["asks"][:2])
                                # print(rep2, '\n', '\n')
                                # print('###########  USD  BBB :', b)
                                # print('###########  USD  BBB full:', rep2['params'][1]["bids"])
                                #
                                # hb = OrderedDict(sorted(b.items(), key=lambda t: t[1]), reverse=False)
                                # hbb = dict((k, v) for k, v in hb.items() if v > 0.1)
                                #
                                # ha = OrderedDict(sorted(a.items(), key=lambda t: t[0]), reverse=True)
                                # haa = dict((k, v) for k, v in ha.items() if v > 0.1)
                                #
                                # bh = float(list(hbb.keys())[0])
                                # ba = float(list(haa.keys())[0])
                                #
                                # print(bh)
                                # print(ba)
                                #
                                #
                                # if bh > ba:
                                #     del hbb[bh]
                                # else:
                                #     pass


                                Hot_buy = {}
                                for i in b:
                                    if not Hot_buy:
                                        if float(i[1])<0.1:
                                            pass
                                        else:
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

                                columns = ['birga', 'valin', 'valout', 'direction', 'rates', 'volume']
                                df = pd.DataFrame(Hot_PU, columns=columns)
                                # print(df)

                                os.remove(main_path_data + "\\hot_bd_PU.csv")
                                df.to_csv(main_path_data + "\\hot_bd_PU.csv", index=False)
                            else:
                                pass

                time.sleep(0.5)

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