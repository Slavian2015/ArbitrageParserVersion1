import os
import json
import pandas as pd
import datetime as dt
import time
# import Reg2_Orders



#################################   SHOW ALL ROWS & COLS   ####################################
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)
main_path_data = os.path.abspath("./data")
second_path_data = os.path.abspath(r"C:/inetpub/wwwroot/Arbitrage/data")


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





    regims_f = open(main_path_data + "\\regims2.json", 'r')  # second_path
    regims2 = json.load(regims_f)
    regims_f.close()


    hot = hot_bd[hot_bd['direction']=='buy'].head(1)
    hs_PU = hot_bd[hot_bd['direction']=='sell'].head(1)

    alfa = alfa_bd[alfa_bd['direction']=='buy'].head(1)
    as_PU = alfa_bd[alfa_bd['direction']=='sell'].head(1)

    live = live_bd[live_bd['direction']=='buy'].head(1)
    ls_PU = live_bd[live_bd['direction']=='sell'].head(1)

    # print(hb_PU)
    # pdList = [hb_PU, hs_PU, ab_PU, as_PU, lb_PU, ls_PU]
    # dfs = pd.concat(pdList, ignore_index=True)
    #
    # dfs2 = pd.merge(dfs, dfs, left_on=dfs['valout'], right_on=dfs['valin'], how='outer')
    # dfs3 = pd.merge(dfs2, dfs2, left_on=dfs2['birga_y'], right_on=dfs2['birga_x'], how='outer')
    # print('dfs :', '\n', dfs3)
    #
    #
    #
    #
    # # Select the ones you want
    # dft = dfs3[['birga_x_x',
    #           'birga_y_x',
    #           'birga_x_y',
    #           'birga_y_y',
    #           'valin_x_x',
    #           'valout_x_x',
    #           'valout_y_x',
    #           'rates_x_x',
    #           'rates_x_y',
    #           'volume_x_x',
    #           'volume_x_y',
    #           'rates_y_x',
    #           'rates_y_y',
    #           'volume_y_x',
    #           'volume_y_y',
    #             'com_x_x',
    #             'com_x_y',
    #             'com_y_x',
    #             'com_y_y',
    #           'direction_x_x',
    #           'direction_x_y',
    #           'direction_y_x',
    #           'direction_y_y']]
    #
    # # print('result :', '\n', dft)
    #
    # dft.loc[:, 'profit'] = (dft['rates_x_y'] - dft['rates_x_x'])/dft['rates_x_x'] *100
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    # def item(reg,b1,b2,d1,d2,d3,d4,val1,val2,val3):
    #     result = dft[
    #         (dft['birga_x_x'] == b1) &
    #         (dft['birga_y_x'] == b2) &
    #         (dft['birga_y_y'] == b1) &
    #         (dft['direction_x_x'] == d1) &
    #         (dft['direction_x_y'] == d2) &
    #         (dft['direction_y_x'] == d3) &
    #         (dft['direction_y_y'] == d4) &
    #         (dft['valin_x_x'] == val1) &
    #         (dft['valout_x_x'] == val2) &
    #         (dft['valout_y_x'] == val3)]
    #     result.loc[:,'reg_profit'] = regims2[reg]['profit']
    #     # result.loc[:,'profit'] = regims2[reg]['profit']
    #
    #     vilki2 = pd.read_csv(main_path_data + "\\vilki2.csv")
    #
    #     vilki = vilki2[
    #         (vilki2['b1'] == b1) &
    #         (vilki2['b2'] == b2) &
    #         (vilki2['val1'] == val1) &
    #         (vilki2['val2'] == val2) &
    #         (vilki2['val3'] == val3)]
    #
    #     # if vilki.shape[0]>0:
    #     #     if result.iloc[0]['profit'] < vilki.iloc[0]['profit']:
    #     #     # change vilki if necessary
    #     #
    #     # else:
    #     #     # make order
    #     #     # append result to vilki2
    #
    #
    #
    #     return result
    #
    # final = item('1',
    #              'alfa',
    #              'hot',
    #              'buy',
    #              'buy',
    #              'sell',
    #              'sell',
    #              'USD',
    #              'PZM',
    #              'USDT',
    #              )
    #
    #
    # print('final :', '\n', final)
    #
    #
    #
    #


    def regim_filter():
        vilki2 = pd.read_csv(main_path_data + "\\vilki2.csv")
        fids = pd.DataFrame()
        for k,v in regims2.items():
            dft = vilki_all[(vilki_all["birga_x"] == v["birga1"]) &
                      (vilki_all["birga_y"] == v["birga2"]) &
                      (vilki_all["valin_x"] == v["val1"]) &
                      (vilki_all["valout_x"] == v["val2"]) &
                      (vilki_all["valout_y"] == v["val3"])
            ]

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

            dft.loc[:, 'My_kurs'] = dft['new_kurs'] * 0.995
            dft.loc[:, 'Vol5'] = dft['Vol3'] * dft['My_kurs'] - (dft['Vol3'] * dft['My_kurs'] * dft['Com_y'] / 100)
            dft.loc[:, 'prof'] = dft['Vol5'] - dft['Vol1']
            dft.loc[:, 'per'] = dft['prof'] / dft['Vol1'] * 100
            dft.loc[:, 'New_per'] = v["profit"]
            dft.drop(['index'], axis=1, inplace=True)


            df_vilki = vilki2[vilki2["regim"] == int(k)]



            if df_vilki.shape[0] > 0:
                print("VILKA")
                # check if dft.iloc[0]['per'] <> df_vilki.iloc[0]['per']
                # check if dft.iloc[0]['per'] <> df_vilki.iloc[0]['New_per']
                # check if dft.iloc[0]['Vol3'] <> df_vilki.iloc[0]['Vol3']
                # check if dft.iloc[0]['Vol3'] <> df_vilki.iloc[0]['min_A']

            else:
                print("NO VILKA")
                dft = dft[(dft["per"] > dft["New_per"]) &
                (dft["Vol3"] > dft["min_A"])]
                if dft.shape[0]>0:
                    val3 = dft.iloc[0]['valin_y']
                    val4 = dft.iloc[0]['valout_y']
                    rate2 = dft.iloc[0]['My_kurs']
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
        vilki2.to_csv(main_path_data + "\\vilki2.csv", header=True, index=False)
        return fids


    all_df = regim_filter()
    # all_df.to_csv(main_path_data + "\\vilki2.csv", header=True, index=False)
    print(all_df)




if __name__ == "__main__":
    # while True:
        # try:
            t1 = time.time()
            regim()
            t2 = time.time()
            print("ALL TIME :", t2 - t1)
        #     time.sleep(0.4)
        #
        # except Exception as e:
        #     print(e)
        #     time.sleep(0.2)
