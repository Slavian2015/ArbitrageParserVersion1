import json
import os
import sqlite3
import time

main_path_data = os.path.abspath("./data")


def alfa():
    new_regims_f = open(main_path_data + "\\alfa_sell_PU.json", 'r')
    alfa_sell = json.load(new_regims_f)
    new_regims_f.close()

    new_regims_f2 = open(main_path_data + "\\alfa_buy_PU.json", 'r')
    alfa_buy = json.load(new_regims_f2)
    new_regims_f2.close()

    ww = list(alfa_buy.keys())
    qq = list(alfa_sell.keys())

    if not ww:
        pass
    elif not qq:
        pass
    else:
        if ww[0] >= qq[0]:
            print("bigger")
            del alfa_buy[ww[0]]
            f = open(main_path_data + "\\alfa_buy_PU.json", "w")
            json.dump(alfa_buy, f)
            f.close()
        else:
            pass

    Alfa_sell = {}
    for k, v in alfa_sell.items():
        if not Alfa_sell:
            Alfa_sell.update({k: float(v)})
        else:
            sump = float(v) + float(list(Alfa_sell.values())[-1])
            Alfa_sell.update({k: sump})

    Alfa_buy = {}
    for k, v in alfa_buy.items():
        if not Alfa_buy:
            Alfa_buy.update({k: float(v)})
        else:
            sump = float(v) + float(list(Alfa_buy.values())[-1])
            Alfa_buy.update({k: sump})
    alfa_PU = []
    for k, v in Alfa_sell.items():
        alfa_PU.append(('alfa', 'USD', 'PZM', 'buy', k, v))
    for k, v in Alfa_buy.items():
        alfa_PU.append(('alfa', 'PZM', 'USD', 'sell', k, v))

    ####################  BTC  ###########################

    new_regims_f12 = open(main_path_data + "\\alfa_sell.json", 'r')
    alfa_sell12 = json.load(new_regims_f12)
    new_regims_f12.close()

    new_regims_f22 = open(main_path_data + "\\alfa_buy.json", 'r')
    alfa_buy22 = json.load(new_regims_f22)
    new_regims_f22.close()

    ww22 = list(alfa_buy22.keys())
    qq22 = list(alfa_sell12.keys())


    if not ww22:
        pass
    elif not qq22:
        pass
    else:
        if ww22[0] >= qq22[0]:
            print("bigger")
            del alfa_buy22[ww22[0]]
            f2 = open(main_path_data + "\\alfa_buy.json", "w")
            json.dump(alfa_buy22, f2)
            f2.close()
        else:
            pass

    Alfa_sell22 = {}
    for k, v in alfa_sell12.items():
        if not Alfa_sell22:
            Alfa_sell22.update({k: float(v)})
        else:
            sump = float(v) + float(list(Alfa_sell22.values())[-1])
            Alfa_sell22.update({k: sump})

    Alfa_buy22 = {}
    for k, v in alfa_buy22.items():
        if not Alfa_buy22:
            Alfa_buy22.update({k: float(v)})
        else:
            sump = float(v) + float(list(Alfa_buy22.values())[-1])
            Alfa_buy22.update({k: sump})
    alfa_PU22 = []
    for k, v in Alfa_sell22.items():
        alfa_PU22.append(('alfa', 'BTC', 'PZM', 'buy', k, v))
    for k, v in Alfa_buy22.items():
        alfa_PU22.append(('alfa', 'PZM', 'BTC', 'sell', k, v))

    #####################################################


    conn = sqlite3.connect(main_path_data + "\\alfa.db")
    cursor = conn.cursor()

    sql = 'DELETE FROM PZMUSD'
    cursor.execute(sql)

    sql2 = 'DELETE FROM PZMBTC'
    cursor.execute(sql2)

    cursor.executemany("INSERT INTO PZMUSD VALUES (?,?,?,?,?,?)", alfa_PU)
    cursor.executemany("INSERT INTO PZMBTC VALUES (?,?,?,?,?,?)", alfa_PU22)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    while True:
        try:
            alfa()
            time.sleep(0.2)
        except Exception as e:
            print(e)
            time.sleep(1)






