import websocket
import time
import zlib
import json
import os
import hashlib
import datetime as dt
from urllib.parse import urlencode

main_path_data = os.path.abspath("./data")
sec_path_data = os.path.abspath(r"C:/inetpub/wwwroot/Arbitrage/data")

a_file = open(sec_path_data + "\\keys.json", "r")
json_object = json.load(a_file)
a_file.close()

input_hot_key = json_object["3"]['key']
input_hot_api = json_object["3"]['api']

try:
    import thread
except ImportError:
    import _thread as thread

d = {
    'method': 'server.auth',
    'params': [input_hot_key, input_hot_api],
    'id':99
}

# L_b = {}
# for b in sorted(d, reverse=False):
#     L_b.update({b: d[b]})
# L_b.update({'secret_key': input_hot_api})


er = urlencode(d)
result = hashlib.md5(er.encode('utf-8'))
sign = result.hexdigest().upper()
print(sign)

variables = {
    'method': 'server.auth',
    'params': [input_hot_key, sign],
    'id':99
}

data = json.dumps(variables).encode('utf-8')

# data = b'{"method":"server.auth","params":["c8021159-6450-0282-6a0ca9cc171afd9c","d6bd7c9ea762f3a19b54f8d3b14cb92c"],"id":99}'

def my_hotbit():
    def on_message(ws, message):
        def receiv(*args):
            while True:
                rep2 = zlib.decompress(message, 16 + zlib.MAX_WBITS)
                rep2 = str(rep2, 'utf-8')
                rep2 = json.loads(rep2)
                now = dt.datetime.now()
                print(now.strftime("%H:%M:%S"))
                print(rep2, '\n', '\n')

                time.sleep(1)
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