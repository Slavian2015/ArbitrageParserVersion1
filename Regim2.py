import os
import json
import pandas as pd
import datetime as dt
import time
import requests
import Reg2_Orders
import Finished_orders



#################################   SHOW ALL ROWS & COLS   ####################################
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)


main_path_data = os.path.abspath("./data")
second_path_data = os.path.abspath(r"C:/inetpub/wwwroot/Arbitrage/data")



a_file1 = open(second_path_data + "\\rools.json", "r")
rools = json.load(a_file1)
a_file1.close()


def bot_sendtext(bot_message):
    ##########################    Telegram    ################################

    ad = open(second_path_data + "\\keys.json", "r")
    js_object = json.load(ad)
    ad.close()
    input1 = js_object["4"]['key']
    input2 = js_object["4"]['api']

    ### Send text message
    bot_token = input1
    bot_chatID = input2
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    requests.get(send_text)
    return

def regim():
    with open(main_path_data + "\\live_bd_PU.csv", 'r') as f:
        live_bd = pd.read_csv(f)
        f.close()
    with open(main_path_data + "\\alfa_bd_PU.csv", 'r') as f:
        alfa_bd = pd.read_csv(f)
        f.close()
    with open(main_path_data + "\\hot_bd_PU.csv", 'r') as f:
        hot_bd = pd.read_csv(f)
        f.close()
    with open(main_path_data + "\\live_bd_PB.csv", 'r') as f:
        live_bd2 = pd.read_csv(f)
        f.close()
    with open(main_path_data + "\\alfa_bd_PB.csv", 'r') as f:
        alfa_bd2 = pd.read_csv(f)
        f.close()
    with open(main_path_data + "\\hot_bd_PB.csv", 'r') as f:
        hot_bd2 = pd.read_csv(f)
        f.close()
    with open(main_path_data + "\\vilki_all.csv", 'r') as f:
        vilki_all = pd.read_csv(f)
        f.close()



    regims_f = open(second_path_data + "\\regims2.json", 'r')  # second_path
    regims2 = json.load(regims_f)
    regims_f.close()


    hot = hot_bd[hot_bd['direction']=='buy'].head(1)
    hs_PU = hot_bd[hot_bd['direction']=='sell'].head(1)

    alfa = alfa_bd[alfa_bd['direction']=='buy'].head(1)
    as_PU = alfa_bd[alfa_bd['direction']=='sell'].head(1)

    live = live_bd[live_bd['direction']=='buy'].head(1)
    ls_PU = live_bd[live_bd['direction']=='sell'].head(1)



    fo = open(main_path_data + "\\finished_orders.json", 'r')
    finished_orders = json.load(fo)
    fo.close()




    def regim_filter():
        vilki2 = pd.read_csv(second_path_data + "\\vilki2.csv")
        all_vilki2 = pd.read_csv(second_path_data + "\\vilki2_all.csv")
        fids = pd.DataFrame()
        for k,v in regims2.items():
            if v['avtomat'] == 'on':
                dft = vilki_all[(vilki_all["birga_x"] == v["birga1"]) &
                          (vilki_all["birga_y"] == v["birga2"]) &
                          (vilki_all["valin_x"] == v["val1"]) &
                          (vilki_all["valout_x"] == v["val2"]) &
                          (vilki_all["valout_y"] == v["val3"]) ]

                bir = v["birga2"]
                dft = dft[dft['percent'] == dft['percent'].max()]
                dft['regim'] = k
                dft['min_A'] = v["order"]

                if bir == 'hot':
                    dft['new_kurs'] = hot.iloc[0]['rates']
                elif bir == 'live':
                    dft['new_kurs'] = live.iloc[0]['rates']
                elif bir == 'alfa':
                    dft['new_kurs'] = alfa.iloc[0]['rates']
                else:
                    pass

                dft.loc[:, 'My_kurs'] = dft['new_kurs'] - (dft['new_kurs'] * v['per'] / 100)
                dft.loc[:, 'Vol5'] = dft['Vol3'] * dft['My_kurs'] - (dft['Vol3'] * dft['My_kurs'] * dft['Com_y'] / 100)
                dft.loc[:, 'prof'] = dft['Vol5'] - dft['Vol1']
                dft.loc[:, 'per'] = dft['prof'] / dft['Vol1'] * 100
                dft['New_per'] = float(v["profit"])
                dft.drop(['index'], axis=1, inplace=True)


                df_vilki = vilki2[vilki2["regim"] == int(k)]
                # print('df_vilki :',k,'\n',df_vilki.shape[0])

                if df_vilki.shape[0] > 0:
                    print("VILKA")
                    adf = dft[(dft['per'] >= dft['New_per']) &
                              (dft['Vol2'] >= dft['min_A']) &
                              (dft['Vol2'] >= df_vilki.iloc[0]['Vol2'])]
                    if adf.shape[0]>0:
                        print("BIGGER ADF")
                        my_order = df_vilki.iloc[0]['order_id']
                        my_dict = finished_orders[df_vilki.iloc[0]['birga_y']]
                        if my_order in my_dict:
                            if my_dict[my_order]>=adf.iloc[0]['min_A']:
                                print("Buy B1 my_dict[my_order] and cancel B2")
                                if my_dict[my_order] >= adf.iloc[0]['Vol3']:
                                    birga = adf.iloc[0]['birga_x']
                                    val1 = adf.iloc[0]['valin_x']
                                    val2 = adf.iloc[0]['valout_x']
                                    rate1 = str(adf.iloc[0]['rates_x'])
                                    val2_vol = adf.iloc[0]['Vol2']

                                    if birga == 'alfa':
                                        reponse = Reg2_Orders.alfa(val1, val2, rate1, val2_vol)
                                    elif birga == 'live':
                                        reponse = Reg2_Orders.live(val1, val2, rate1, val2_vol)
                                    elif birga == 'hot':
                                        reponse = Reg2_Orders.hot(val1, val2, rate1, val2_vol)
                                    else:
                                        reponse = "No such BIRGA"
                                        pass

                                    now = dt.datetime.now()
                                    timer2 = now.strftime("%Y-%m-%d %H:%M:%S")
                                    profit = (adf.iloc[0]['Vol4'] - adf.iloc[0]['Vol1']) / adf.iloc[0]['Vol1'] * 100

                                    dw2 = {'regim': adf.iloc[0]['regim'],
                                           'timed': [timer2],
                                           'b1': [adf.iloc[0]['birga_x']],
                                           'b2': [adf.iloc[0]['birga_y']],
                                           'val1': [adf.iloc[0]['valin_x']],
                                           'val2': [adf.iloc[0]['valout_x']],
                                           'val3': [adf.iloc[0]['valout_y']],
                                           'kurs1': [adf.iloc[0]['rates_x']],
                                           'kurs2': [adf.iloc[0]['rates_y']],
                                           'Vol1': [adf.iloc[0]['Vol1']],
                                           'Vol2': [adf.iloc[0]['Vol2']],
                                           'Vol3': [adf.iloc[0]['Vol3']],
                                           'Vol4': [adf.iloc[0]['Vol4']],
                                           'profit': [profit],
                                           'rep1': [reponse],
                                           'rep2': [my_order],
                                           }
                                    df2 = pd.DataFrame(data=dw2, index=[0])
                                    df2.drop_duplicates(inplace=True)
                                    all_vilki2 = pd.concat([dft, all_vilki2], ignore_index=True, join='outer')
                                    nl = '\n'
                                    bot_sendtext(
                                        f" ЕСТЬ ВИЛКА: {nl} РЕЖИМ : {adf.iloc[0]['regim']} {nl} {adf.iloc[0]['birga_x']} / {adf.iloc[0]['birga_y']} {nl} "
                                        f"{reponse} / {my_order} {nl} {adf.iloc[0]['valin_x']} -> {adf.iloc[0]['valout_x']} -> {adf.iloc[0]['valout_y']} {nl} "
                                        f"PROFIT: {profit} {nl} ")

                                    vilki2.drop(vilki2.index[vilki2["regim"] == int(k)], inplace=True)


                                    ###############  delete from finished json  #############################

                                    a_file = open(main_path_data + "\\finished_orders.json", "r")
                                    json_object = json.load(a_file)
                                    a_file.close()

                                    del json_object[adf.iloc[0]['birga_y']][my_order]

                                    a_file = open(main_path_data + "\\finished_orders.json", "w")
                                    json.dump(json_object, a_file)
                                    a_file.close()



                                else:
                                    birga = adf.iloc[0]['birga_x']
                                    val1 = adf.iloc[0]['valin_x']
                                    val2 = adf.iloc[0]['valout_x']
                                    rate1 = adf.iloc[0]['rates_x']
                                    birga2 = adf.iloc[0]['birga_y']
                                    nl = '\n'
                                    if birga == 'alfa':
                                        val2_vol = my_dict[my_order]+(my_dict[my_order] * adf.iloc[0]['Com_x'] / 100)
                                        val1_vol = val2_vol * rate1
                                        val4_vol = (my_dict[my_order] * adf.iloc[0]['rates_y']) + (my_dict[my_order] * adf.iloc[0]['rates_y'] * adf.iloc[0]['Com_y'] / 100)
                                    else:
                                        val2_vol = my_dict[my_order]
                                        val1_vol = (val2_vol * rate1) + (val2_vol * rate1 * adf.iloc[0]['Com_x'] / 100)
                                        val4_vol = (my_dict[my_order] * adf.iloc[0]['rates_y']) + (my_dict[my_order] * adf.iloc[0]['rates_y'] * adf.iloc[0]['Com_y'] / 100)

                                    if birga == 'alfa':
                                        reponse = Reg2_Orders.alfa(val1, val2, str(rate1), val2_vol)
                                    elif birga == 'live':
                                        reponse = Reg2_Orders.live(val1, val2, str(rate1), val2_vol)
                                    elif birga == 'hot':
                                        reponse = Reg2_Orders.hot(val1, val2, str(rate1), val2_vol)
                                    else:
                                        reponse = "No such BIRGA"
                                        pass

                                    if birga2 == 'alfa':
                                        reponse2 = Reg2_Orders.alfa_cancel(my_order)
                                    elif birga2 == 'live':
                                        reponse2 = Reg2_Orders.live_cancel(adf.iloc[0]['valout_x'], adf.iloc[0]['valout_y'], my_order)
                                    elif birga2 == 'hot':
                                        reponse2 = Reg2_Orders.hot_cancel(adf.iloc[0]['valout_x'], adf.iloc[0]['valout_y'], my_order)
                                    else:
                                        reponse2 = "No such BIRGA"
                                        pass
                                    now = dt.datetime.now()
                                    timer2 = now.strftime("%Y-%m-%d %H:%M:%S")

                                    profit = (adf.iloc[0]['Vol4'] - adf.iloc[0]['Vol1']) / adf.iloc[0]['Vol1'] * 100
                                    dw2 = {'regim': adf.iloc[0]['regim'],
                                           'timed': [timer2],
                                           'b1': [adf.iloc[0]['birga_x']],
                                           'b2': [adf.iloc[0]['birga_y']],
                                           'val1': [adf.iloc[0]['valin_x']],
                                           'val2': [adf.iloc[0]['valout_x']],
                                           'val3': [adf.iloc[0]['valout_y']],
                                           'kurs1': [adf.iloc[0]['rates_x']],
                                           'kurs2': [adf.iloc[0]['rates_y']],
                                           'Vol1': [val1_vol],
                                           'Vol2': [val2_vol],
                                           'Vol3': [my_dict[my_order]],
                                           'Vol4': [val4_vol],
                                           'profit': [profit],
                                           'rep1': [reponse],
                                           'rep2': [my_order],
                                           }
                                    df2 = pd.DataFrame(data=dw2, index=[0])
                                    df2.drop_duplicates(inplace=True)
                                    all_vilki2 = pd.concat([dft, all_vilki2], ignore_index=True, join='outer')

                                    bot_sendtext(
                                        f" ЕСТЬ ВИЛКА: {nl} РЕЖИМ : {adf.iloc[0]['regim']} {nl} {adf.iloc[0]['birga_x']} / {adf.iloc[0]['birga_y']} {nl} "
                                        f"{reponse} / {my_order} {nl} {adf.iloc[0]['valin_x']} -> {adf.iloc[0]['valout_x']} -> {adf.iloc[0]['valout_y']} {nl} "
                                        f"PROFIT: {profit} {nl} ")

                                    vilki2.drop(vilki2.index[vilki2["regim"] == int(k)], inplace=True)

                                    ###############  delete from finished json  #############################

                                    a_file = open(main_path_data + "\\finished_orders.json", "r")
                                    json_object = json.load(a_file)
                                    a_file.close()

                                    del json_object[adf.iloc[0]['birga_y']][my_order]

                                    a_file = open(main_path_data + "\\finished_orders.json", "w")
                                    json.dump(json_object, a_file)
                                    a_file.close()
                            else:
                                pass
                        else:
                            pass
                    else:
                        bir = df_vilki.iloc[0]['birga_y']
                        print("Smaller ADF", bir)
                        if bir == 'alfa':
                            reponse2 = Reg2_Orders.alfa_cancel(df_vilki.iloc[0]['order_id'])
                        elif bir == 'live':
                            reponse2 = Reg2_Orders.live_cancel(df_vilki.iloc[0]['valout_x'],df_vilki.iloc[0]['valout_y'],df_vilki.iloc[0]['order_id'])
                        elif bir == 'hot':
                            reponse2 = Reg2_Orders.hot_cancel(df_vilki.iloc[0]['valout_x'],df_vilki.iloc[0]['valout_y'],df_vilki.iloc[0]['order_id'])
                        else:
                            reponse2 = "No such BIRGA"
                            pass
                        vilki2.drop(vilki2.index[vilki2["regim"] == int(k)], inplace=True)

                        ###############  delete from finished json  #############################

                        a_file = open(main_path_data + "\\finished_orders.json", "r")
                        json_object = json.load(a_file)
                        a_file.close()

                        del json_object[bir][df_vilki.iloc[0]['order_id']]

                        a_file = open(main_path_data + "\\finished_orders.json", "w")
                        json.dump(json_object, a_file)
                        a_file.close()
                else:
                    print("NO VILKA")
                    dft = dft[(dft["per"] > dft["New_per"]) &
                    (dft["Vol2"] > dft["min_A"])]

                    if dft.shape[0]>0:
                        val3 = dft.iloc[0]['valin_y']
                        val4 = dft.iloc[0]['valout_y']
                        rate2 = str(dft.iloc[0]['My_kurs'])
                        val3_vol = dft.iloc[0]['Vol3']

                        if bir == 'alfa':
                            reponse = Reg2_Orders.alfa(val3, val4, rate2, val3_vol)
                        elif bir == 'live':
                            reponse = Reg2_Orders.live(val3, val4, rate2, val3_vol)
                        elif bir == 'hot':
                            reponse = Reg2_Orders.hot(val3, val4, rate2, val3_vol)
                        else:
                            reponse = "No such BIRGA"
                            pass
                        dft.loc[:,'order_id'] = reponse
                        vilki2 = pd.concat([dft, vilki2], ignore_index=True, join='outer')
                    else:
                        pass
                fids = pd.concat([dft, fids], ignore_index=True, join='outer')
            else:
                pass


        vilki2.to_csv(second_path_data + "\\vilki2.csv", header=True, index=False)
        all_vilki2.to_csv(second_path_data + "\\vilki2_all.csv", header=True, index=False)
        return fids


    all_df = regim_filter()
    # all_df.to_csv(main_path_data + "\\vilki2.csv", header=True, index=False)
    # print(all_df)




if __name__ == "__main__":
    while True:
        try:
            t1 = time.time()
            Finished_orders.main()
            t2 = time.time()
            regim()
            t3 = time.time()
            print("open orders :", t2 - t1)
            print("regim 2 :", t3 - t2)
            print("ALL TIME :", t3 - t1)
            time.sleep(0.4)

        except Exception as e:
            print(e)
            time.sleep(0.2)
