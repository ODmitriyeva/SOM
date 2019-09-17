
# coding: utf-8

import  pandas as pd
import numpy as np
from tqdm import tqdm
import datetime

import sys
#
date = sys.argv[1]

#month = sys.argv[2]
#
#calendar = {'Jan': ['1','01'],
#            'Feb': ['2','02'],
#            'Mar': ['3','03'],
#            'Apr': ['4','04'],
#            'May': ['5','05'],
#            'Jun': ['6','06'],
#            'Jul': ['7','07'],
#            'Aug': ['8','08'], 
#            'Sep': ['9','09'], 
#            'Oct': ['10','10'],
#            'Nov': ['11','11'],
#            'Dec': ['12','12']}


Date = str(date)+'%'
#Date = str(year) + '-' + str(calendar[month][1]) + '%'
#date = str(calendar[month][0])+'/1/'+str(year)
#date_price = datetime.datetime(year,calendar[month][0],1,0,0)
#print (Date)
#print(date)
#print(date_price)

#Date = "2019-05%"
#date = '5/1/2019'
#date_price =  datetime.datetime(2019, 5, 1, 0, 0)
# In[2]:


import requests
r = requests.get('http://ocsp.digicert.com/')
r.text


# In[3]:


ACCOUNT =  "po91914.east-us-2.azure"
USER =  "_sdatabricks_service"
# In[1]:


PASSWORD = "DBDcpSCab@2018!2"

import snowflake.connector
sfDatabase="DEV_DATAWAREHOUSE_DB",
sfSchema="SYSTEM_OPTIMIZATION",
sfWarehouse="DATABRICKS_WH"


# In[344]:


ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    insecure_mode=True
    )
ctx.cursor().execute('USE warehouse DATABRICKS_WH')
ctx.cursor().execute('USE DEV_DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION')


cur = ctx.cursor()
ROW = []
try:
    cur.execute("SELECT * FROM DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION.V_PRICING_COEFFICIENTS where DATE like '"+str(Date)+"'")#'2019-05%'")
    reference_price = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    cur.execute("SELECT * FROM DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION.V_METER_COEFFICIENTS where DATE like '"+str(Date)+"'")
    coeff = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description]) 
    
    cur.execute("SELECT * FROM DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION.V_BOOSTER_COEFFICIENTS where DATE like '"+str(Date)+"'")
    reference_booster = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    cur.execute("SELECT * FROM DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION.V_PLANT_COEFFICIENTS where DATE like '"+str(Date)+"'")
    reference_plant = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    
    cur.execute('SELECT * FROM "DATAWAREHOUSE_DB"."SYSTEM_OPTIMIZATION"."V_STATIC_REFERENCE_DATA"')
    reference_static = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    cur.execute("SELECT * FROM DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION.V_ROUTING where DATE like '"+str(Date)+"'")
    reference_routing = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
#    
    cur.execute("SELECT * FROM DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION.V_OPEX where DATE like '"+str(Date)+"'")
    reference_opex = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        
finally:
    cur.close()
# reference_plant[reference_plant.PLANT == 'Pegasus']



# In[395]:

#
#Date = '2019-02-01'
#date = '2/1/2019'
#months = ['Feb2019']


# In[396]:

#
#Coeff = coeff[coeff.DATE == Date].reset_index(drop = True)


# In[315]:


routes = pd.unique(coeff.ROUTE_NUMBER)
#Route[Route.ROUTE_NUMBER == 30021]


# In[316]:


#def asset_from_route(route):
#    plant = Route[Route.ROUTE_NUMBER == route].PLANT_NAME.values[0]
#    booster = Route[Route.ROUTE_NUMBER == route].BOOSTER_NAME.values[0]
#    return plant, booster

def asset_from_route(route):
    plant = reference_routing[reference_routing.ROUTE_NUMBER == route].PLANT_NAME.values[0]
    booster = reference_routing[reference_routing.ROUTE_NUMBER == route].BOOSTER_NAME.values[0]
    return plant, booster

# In[317]:


def Converting_to_float(df):
    for column in df.columns:
        if column!= 'TIPS_METER':
            try:
                df[column] = pd.to_numeric(df[column],  errors='ignore')
 
            except:
                pass
      
    return df


# In[318]:


# d = {'col1': [1, 2], 'col2': [3, 0],'col3': [0, 0]}
# df = pd.DataFrame(data=d)
# print (df.head())

def average(row):
    count = 0
    for element in row:
        if element !=0:
            count += 1
    return row.sum(axis = 0)/count


# In[401]:
import datetime

OUTPUT = pd.DataFrame()
k = 0
r = []
for route in tqdm(routes):
    
#    Coeff = coeff[coeff.DATE == Date].reset_index(drop = True)
#    Booster = reference_booster[reference_booster.DATE == date].reset_index(drop = True)
#    Plant = reference_plant[reference_plant.DATE == date].reset_index(drop = True)
#    Price = reference_price[(reference_price.DATE == date_price)].reset_index(drop = True)
#    Static = reference_static
#    Opex = reference_opex[reference_opex.DATE ==date].reset_index(drop = True)
#    Route = reference_routing[(reference_routing.DATE == date)].reset_index(drop = True)
    plant, booster = asset_from_route(route)
   
#    Coeff = Coeff[Coeff.ROUTE_NUMBER == route].reset_index(drop = True)
#    Booster = Booster[Booster.BOOSTER == booster].reset_index(drop = True)
#    Plant = Plant[Plant.PLANT == plant].reset_index(drop = True)
    
    Coeff = coeff[coeff.ROUTE_NUMBER == route].reset_index(drop = True)
    Booster = reference_booster[reference_booster.BOOSTER == booster].reset_index(drop = True)
    Plant = reference_plant[reference_plant.PLANT == plant].reset_index(drop = True)
    Price = reference_price
    Static = reference_static
    Opex = reference_opex
    Route = reference_routing
    
    Coeff = Coeff.replace(np.nan, 0)
    Plant = Plant.replace(np.nan, 0)
    Price = Price.replace(np.nan, 0)
    Booster = Booster.replace(np.nan, 0)
    
    Coeff = Converting_to_float(Coeff)
    Booster = Converting_to_float(Booster)
    Price = Converting_to_float(Price)
    Plant = Converting_to_float(Plant)
    Static = Converting_to_float(Static)
    
    Output = pd.DataFrame()
    
    Output['ROUTE_NUMBER'] = Coeff.ROUTE_NUMBER
    Output['TIPS_METER'] = Coeff.TIPS_METER
    Output['MCF'] = Coeff.MCF
    Output['MMBTU'] = Coeff.MMBTU
    Output['DCP_LU'] = Booster['ACTUAL_LU_TO_BOOSTER_PCT'].values[0]
                              
    Output['DCP_Field_Fuel'] = Booster['ACTUAL_FUEL_TO_BOOSTER_PCT'].values[0]
    
    Output['ND_mcf'] = Output.MCF*(1 - Booster['ACTUAL_LU_TO_BOOSTER_PCT'].values - Booster['ACTUAL_FUEL_TO_BOOSTER_PCT'].values)
    Output['ND_mmbtu'] = Output.MMBTU*(1 - Booster['ACTUAL_LU_TO_BOOSTER_PCT'].values - Booster['ACTUAL_FUEL_TO_BOOSTER_PCT'].values)
    
    fractions = ['C2', 'C3', 'IC4', 'NC4', 'C5', 'C6','HE']
    Output['DCP_total_Theoretical_G'] = 0
    Output['DCP_total_recovered_G'] = 0
    Output['DCP_Shrink'] = 0
# # fractions_dict = {}
    for frac in fractions:
        #print (Plant[frac+'_PCT_RECOVERY'].values)
        Output['Recovery_pct_'+frac]=float(Plant[frac+'_PCT_RECOVERY'].values)
  
   
        if frac == 'HE':
            Output['DCP_Theoretical_'+frac] = Output.ND_mcf*Coeff[frac+'_MOL_PCT']/100.0#Fraction(meter,'He')[0]#*1000
            Output['DCP_Recovered_Gallons_'+frac] = Output['DCP_Theoretical_'+frac]*Output['Recovery_pct_'+frac]
            shrink = 0.0
        else:
            Output['DCP_Theoretical_'+frac] = Output.ND_mcf*Coeff[frac+'_GPM']
            Output['DCP_Recovered_Gallons_'+frac] = Output['DCP_Theoretical_'+frac]*Output['Recovery_pct_'+frac]
            shrink =  Output['DCP_Recovered_Gallons_'+frac]*Static[Static['PRODUCT'] == frac]['SHRINK_FACTOR'].values

        Output['DCP_total_Theoretical_G'] += Output['DCP_Theoretical_'+frac]
        Output['DCP_total_recovered_G'] += Output['DCP_Recovered_Gallons_'+frac]
        Output['DCP_Shrink'] = Output['DCP_Shrink'] + shrink
        
        
        
    fractions_NGL = ['C2', 'C3', 'IC4','NC4', 'C5']
    DCP_market_index = []
    price = []

    for frac in fractions_NGL:

        index1 = Plant['DCP_NGL_MARKET'+str(1)+'_'+str(frac)].values[0]
        index2 = Plant['DCP_NGL_MARKET'+str(2)+'_'+str(frac)].values[0]   

        price1 = Price[Price['INDEX'] == index1]['PRICE'].values
        price2 = Price[Price['INDEX'] == index2]['PRICE'].values
       
        pct1 = Plant['DCP_NGL_MARKET_'+str(1)+'_PCT'].values
        pct2 = Plant['DCP_NGL_MARKET_'+str(2)+'_PCT'].values

        tf1 = Plant['DCP_TF_ON_NGL_MARKET_'+str(1)].values
        tf2 = Plant['DCP_TF_ON_NGL_MARKET_'+str(2)].values
        
        m1 = (price1+tf1)*pct1
        m2 = (price2+tf2)*pct2
    
        if m1.size == 0:
            if m2.size == 0:
                m = 0
            else:
                m = m2
        elif m2.size == 0:
            m = m1
        else:
            m = m1+m2

        #### How do we calulate overall TF out of 2 markets?

        Output['NGL_TF_'+frac] = float(tf1)
        Output['NGL_NTP_'+frac] = float(m)
    
    ## DCP NGL Revenue
    DCP_NGL_summation = 0

    for frac in ['C2', 'C3', 'IC4', 'NC4', 'C5']:

        DCP_NGL_summation += Output['NGL_NTP_'+frac]*Output['DCP_Recovered_Gallons_'+frac]

    DCP_NGL_summation += Output['NGL_NTP_C5']*Output['DCP_Recovered_Gallons_C6']

    Output['DCP_NGL_Revenue'] = DCP_NGL_summation
    
    ## Uplift
    Uplift_index = Plant['UPLIFT_PIPELINE'].values[0]
    if Uplift_index != 0:
        Uplift_price = Price[Price['INDEX'] == Uplift_index]['PRICE'].values[0]
    else:
        Uplift_price = 0

    uplift = Output.DCP_total_recovered_G*Uplift_price
    Output['DCP_Uplift'] = uplift


    Output['Fees'] = Coeff.FEES
    
    
    ##DCP RES
     ########################### !!!!!!!!!!! ############################
    Output['DCP_plant_fuel']  = Output['ND_mmbtu']*Plant['PLANT_FUEL_PCT_MMBTU'].values
    Output['DCP_RES_AFS'] = Output['ND_mmbtu']-Output['DCP_Shrink']-Output['DCP_plant_fuel']

    price_res = 0
    for i in range(5):

        index = Plant['DCP_RES_INDEX_'+str(i+1)].values[0]
        deduct = Plant['DCP_RES_DEDUCT_INDEX_'+str(i+1)].values
        pct = Plant['DCP_RES_INDEX_'+str(i+1)+'_PCT'].values
        if index != 0:
            price = Price[Price['INDEX'] == index]['PRICE'].values[0]
            for_each_index = (price+deduct)*pct#*RES_AFS[0]
            price_res += for_each_index

    Output['DCP_RES_Price'] =  float(price_res)
    Output['DCP_RES_Revenue'] = Output['DCP_RES_AFS']*price_res
    
    ## DCP Condensate

    condensate_recovered_G = Output['DCP_Recovered_Gallons_C5']+Output['DCP_Recovered_Gallons_C6']
    condensate_recovery = Plant['CONDENSATE_PCT_RECOVERY'].values[0]

    DCP_condensate_G = condensate_recovered_G*condensate_recovery  

    Output['DCP_Produced_Condensate'] = DCP_condensate_G/42.
    price = Price[Price['INDEX'] == 'NYMEX - Crude']['PRICE'].values[0]
    diff = Plant['DCP_CONDENSATE_DIFF'].values[0]
    Output['DCP_Condensate_Price'] = float(price+diff)
    Output['DCP_Condensate_Revenue'] = Output['DCP_Condensate_Price']*Output['DCP_Produced_Condensate']

    ## He Revenue

 
    index_He = Plant['HE_INDEX'].values[0]

    Output['DCP_He_Price'] = float(Price[Price['INDEX']==index_He]['PRICE'].values)
    Output['DCP_He_Production'] = Output['DCP_Recovered_Gallons_HE']
    Output['DCP_He_Revenue'] = Output['DCP_He_Production']*Output['DCP_He_Price']

    Output['DCP_Revenue'] = Output['DCP_NGL_Revenue']+Output['DCP_RES_Revenue'] +Output['DCP_He_Revenue'] +Output['DCP_Uplift']+ Output['DCP_Condensate_Revenue'] + Output['Fees']
    
    
    ##Producer LU and FUEL
    Coeff['LU'].str.contains("ACTUAL")

    Output.loc[Coeff['LU'].str.contains("ACTUAL"), 'Producer_LU'] = Output['DCP_LU']
    Output.loc[Coeff['LU'].str.contains("FIXED"), 'Producer_LU'] = Coeff['PRODUCER_LU_PCT']

    Output.loc[Coeff['FIELD_FUEL'].str.contains("ACTUAL"), 'Producer_Field_Fuel'] = Output['DCP_Field_Fuel']
    Output.loc[Coeff['FIELD_FUEL'].str.contains("FIXED"), 'Producer_Field_Fuel'] = Coeff['PRODUCER_FIELD_FUEL_PCT']
    ## Producer Net delivered

    Output['Producer_ND_mcf']=Output['MCF']*(1-Output['Producer_LU']- Output['Producer_Field_Fuel'])
    Output['Producer_ND_mmbtu']=Output['MMBTU']*(1-Output['Producer_LU']- Output['Producer_Field_Fuel'])
    
    
    ##Producer Recoveries

    fractions = fractions = ['C2', 'C3', 'IC4', 'NC4', 'C5', 'C6','HE']
    Output['Producer_Theoretical_total'] = 0
    Output['Producer_Recovered_total'] = 0
    Output['Producer_Shrink'] = 0
    # fractions_dict = {}
    for frac in fractions:

        if frac == 'HE':
            Output['Producer_Recovery_HE']=Coeff['PRODUCER_HE_PCT_RECOVERY']
            Output['Producer_Theoretical_HE'] = Output.Producer_ND_mcf*Coeff['HE_MOL_PCT']/100.0#Fraction(meter,'He')[0]#*1000
            Output['Producer_Recovered_HE'] = Output['DCP_Theoretical_HE']*Output['Producer_Recovery_HE']
            shrink = 0.0
        else:
            Output.loc[Coeff['RECOVERIES'].str.contains("ACTUAL"), ('Producer_Recovery_'+frac)] = Output['Recovery_pct_'+frac]
            Output.loc[Coeff['RECOVERIES'].str.contains("FIXED"), ('Producer_Recovery_'+frac)] = Coeff['PRODUCER_'+frac+'_PCT_RECOVERY']

            Output.loc[Coeff['RECOVERIES'].str.contains("ACTUAL"), ('Producer_Theoretical_'+frac)] = Output['DCP_Theoretical_'+frac]
            Output.loc[Coeff['RECOVERIES'].str.contains("FIXED"), ('Producer_Theoretical_'+frac)] = Output['Producer_ND_mcf']*Coeff[frac+'_GPM']    

            Output.loc[Coeff['RECOVERIES'].str.contains("ACTUAL"), ('Producer_Recovered_'+frac)] = Output['DCP_Recovered_Gallons_'+frac]
            Output.loc[Coeff['RECOVERIES'].str.contains("FIXED"), ('Producer_Recovered_'+frac)] = Output['Producer_Theoretical_'+frac]*Output['Producer_Recovery_'+frac]
            shrink = Output['Producer_Recovered_'+frac]*Static[Static['PRODUCT'] == frac]['SHRINK_FACTOR'].values


        Output['Producer_Theoretical_total'] += Output['Producer_Theoretical_'+frac]
        Output['Producer_Recovered_total'] += Output['Producer_Recovered_'+frac]
        Output['Producer_Shrink'] = Output['Producer_Shrink'] + shrink

    
    Output['Producer_Settled_G']=0
    Output['NGL_POP'] = Coeff['NGL_POP']
    for frac in fractions:
        Output['Producer_Settled_'+frac] = Output['Producer_Recovered_'+frac]*Output['NGL_POP']
        Output['Producer_Settled_G'] += Output['Producer_Settled_'+frac]
    ############################ !!!!!!!!!!!!!!!!!!!! ##########################
    Output['Producer_Settled_C56'] = Output['Producer_Settled_C5']+Output['Producer_Settled_C6']
    ############ >>>>>>>>><<<<<<<<<<<<<############
    #Output['Producer_Settled_C5'] = Output['Producer_Settled_C56']
        
    ## Producer Plant Fuel
    Output.loc[Coeff['PLANT_FUEL'].str.contains("ACTUAL"), 'Producer_plant_fuel'] = Plant['PLANT_FUEL_PCT_MMBTU'].values[0]
    Output.loc[Coeff['PLANT_FUEL'].str.contains("FIXED"), 'Producer_plant_fuel'] = Coeff['PRODUCER_PLANT_FUEL_PCT']
  
    ## Producer He
    Output['He_POP'] = Coeff['HE_POP']
    Output['Producer_He_Price'] = Coeff['HE_PRODUCER_USD']
    Output['Producer_Settled_He'] = Output['Producer_Recovered_HE']*Output['He_POP']
    Output['Producer_He_Production'] = Output['Producer_Recovered_HE']
    Output['Producer_He_Revenue'] = Output['Producer_Settled_He']*Output['Producer_He_Price']
    
    
    #Producer Condensate
    Output['Condensate_POP'] = Coeff['COND_POP']
    Output['Producer_Condensate_Price'] = Coeff['PRODUCER_COND_PRICE']

    Output['Producer_condensateG'] = Output['Producer_Recovered_C5']+Output['Producer_Recovered_C6']

    Output.loc[Coeff['RECOVERIES'].str.contains("FIXED"), 'Producer_Produced_Condensate'] = Output['Producer_condensateG']*Coeff['PRODUCER_CONDENSATE_PCT_RECOVERY']/42.
    Output.loc[Coeff['RECOVERIES'].str.contains("ACTUAL"), 'Producer_Produced_Condensate'] =  Output['Producer_condensateG']*Plant['CONDENSATE_PCT_RECOVERY'].values[0]/42.
    Output['Producer_Settled_Condensate'] = Output['Producer_Produced_Condensate']*Output['Condensate_POP']
    Output['Producer_Condensate_Revenue'] = Output['Producer_Settled_Condensate']*Output['Producer_Condensate_Price']
    
    fractions_NGL = ['C2', 'C3', 'IC4','NC4', 'C5']
    Output['Producer_NGL_Revenue'] = 0
    ## Producer NGL

    for frac in fractions_NGL:

        index = Coeff['PROD_'+frac+'_MARKET'].values
        price = {}
        for ind in pd.unique(index):
            if ind !=0:
                price[ind] = Price[Price['INDEX'] == ind]['PRICE'].values[0]

                Output.loc[ Coeff['PROD_'+frac+'_MARKET'].values == ind, ('_prod_price_'+frac)] = price[ind]
            else:
                Output.loc[Coeff['PROD_'+frac+'_MARKET'].values == ind, ('_prod_price_'+frac)] = 0
        Output['_prod_TF_'+frac] = Coeff['PRODUCER_'+str(frac)+'_TF'].values
        Output['Producer_NGL_NTP_'+frac] = Output['_prod_price_'+frac]+ Output['_prod_TF_'+frac]


        Output['Producer_NGL_Revenue_'+frac] = Output['Producer_NGL_NTP_'+frac]*Output['Producer_Settled_'+frac]


        Output['Producer_NGL_Revenue'] += Output['Producer_NGL_Revenue_'+frac]
    ##################### !!!!!!!!!!!!!! ###########################
    Output['Producer_NGL_Revenue_C56'] = Output['Producer_NGL_NTP_C5']*(Output['Producer_Settled_C5']+Output['Producer_Settled_C6'])
    #################### 
    Output['Producer_NGL_Revenue'] += Output['Producer_NGL_NTP_C5']*Output['Producer_Settled_C6']    
    ################## >>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<< ####################
    Output['Producer_Settled_C5'] = Output['Producer_Settled_C56']
    
    
    ## Producer RES
    Output['RES_POP'] = Coeff['RES_POP']
    Output['Producer_RES_AFS'] = Output['Producer_ND_mmbtu'] -Output['Producer_Shrink'] -Output['Producer_ND_mmbtu']*Output['Producer_plant_fuel']
    Output['Producer_Settled_allocated'] = Output["Producer_RES_AFS"]*Output['RES_POP']

    Output['_deduct'] = Coeff['PROD_RES_INDEX_DEDUCT']
    total_Price = []
    price = []
    for i in range(3):
        pct = Coeff['PROD_RES_INDEX_'+str(i+1)+'_PCT']
        index = Coeff['PROD_RES_INDEX_'+str(i+1)]      

        price ={}
        for ind in pd.unique(index):
            #print (i, ind)
            if ind !=0 and len(ind)>1:
                price = Price[Price['INDEX']==ind]['PRICE'].values[0]
                Output.loc[Coeff['PROD_RES_INDEX_'+str(i+1)].values == ind, ('_prod_res_price_pct_'+str(i+1))] = price*Coeff['PROD_RES_INDEX_'+str(i+1)+'_PCT']
            else:

                Output.loc[Coeff['PROD_RES_INDEX_'+str(i+1)].values == ind, ('_prod_res_price_pct_'+str(i+1))] = 0


    Output['_price_pct_average'] = Output[['_prod_res_price_pct_1','_prod_res_price_pct_2','_prod_res_price_pct_3']].apply(average,axis = 1)
    Output.loc[Coeff['PROD_RES_OVERALL_PCT'].values != 0.0, 'Producer_RES_price'] = (Output['_price_pct_average']+Output['_deduct'])*Coeff['PROD_RES_OVERALL_PCT'] + Coeff['PROD_RES_OVERALL_DEDUCT']
    Output.loc[Coeff['PROD_RES_OVERALL_PCT'].values == 0.0, 'Producer_RES_price'] = Output['_price_pct_average']+Output['_deduct']+Coeff['PROD_RES_OVERALL_DEDUCT']
    Output['Producer_RES_Revenue'] = Output['Producer_RES_price']*Output['Producer_Settled_allocated']    

    Output['Producer_Revenue'] = Output['Producer_NGL_Revenue'] + Output['Producer_RES_Revenue']+Output['Producer_He_Revenue']+ Output['Producer_Condensate_Revenue']
    Output['Margin'] = Output['DCP_Revenue']-Output['Producer_Revenue']
   ############################ !!!!!!!!!!!!!!!!!! ###########################
    Output['DCP_plant_fuel']  = Plant['PLANT_FUEL_PCT_MMBTU'].values[0]
    OUTPUT = OUTPUT.append(Output)
    #print (k, route, len(Output), len(OUTPUT))
#     r.append(route)
#     k+=1


# In[322]:


#print ( len(pd.unique(OUTPUT.ROUTE_NUMBER)), len(pd.unique(OUTPUT.TIPS_METER)))


# ## Opex

# In[407]:


#Coeff = coeff[coeff.DATE == Date].reset_index(drop = True)
#Booster = reference_booster[reference_booster.DATE == date].reset_index(drop = True)
#Plant = reference_plant[reference_plant.DATE == date].reset_index(drop = True)
##Price = reference_price[(reference_price.DATE == date_price)].reset_index(drop = True)
##Static = reference_static
#Opex = reference_opex[reference_opex.DATE ==date].reset_index(drop = True)
#Route = reference_routing[(reference_routing.DATE == date)].reset_index(drop = True)
    
Coeff = coeff
#Booster = reference_booster
#Plant = reference_plant
#Price = reference_price
#Static = reference_static
Opex = reference_opex
Route = reference_routing


Route = Route.replace(np.nan,0)
#Static = Static.replace(np.nan, 0)
Opex = Opex.replace(np.nan, 0)
Coeff = Coeff.replace(np.nan, 0)
#Plant = Plant.replace(np.nan, 0)
#Price = Price.replace(np.nan, 0)
#Booster = Booster.replace(np.nan, 0)

Coeff = Converting_to_float(Coeff)
#Booster = Converting_to_float(Booster)
#Price = Converting_to_float(Price)
#Plant = Converting_to_float(Plant)
#Static = Converting_to_float(Static)
Opex = Converting_to_float(Opex)


# In[409]:


Opex.columns


# In[410]:


def check_routing(number_row):
    r = Route.loc[number_row]['ROUTE_NUMBER']
    b1 = Route.loc[number_row]['BOOSTER_NAME']
    b2 = Route.loc[number_row]['SECONDARY_BOOSTER']
    b3 = Route.loc[number_row]['TERTIARY_BOOSTER']
    p = Route.loc[number_row]['PLANT_NAME']
    gs = Route.loc[number_row]['GATHERING_SYSTEM']
    a = Route.loc[number_row]['AREA']
    return [r,b1,b2,b3,p,gs,a]
  

  ### Return table which consists of all the meters and their MCF that belong to given route

def virtual_meter_table(route):
    v_m = Coeff[Coeff['ROUTE_NUMBER'] == route][['TIPS_METER','MCF','ROUTE_NUMBER']]
    v_m.MCF = v_m.MCF.astype('int64', copy=False)

    return v_m
  
def Opex_Total(df):
    Total_Base_list = ['LABOR_AND_BENEFITS', 'MATERIALS_AND_SUPPLIES' , 'CHEMICAL_AND_LUBE_PURCHASES', 
                   'IT_PURCHASES', 'EMPLOYEE_EXPENSES', 'TRANSPORTATION', 'CONTRACT_SERVICES' , 
                   'LEASE_EXPENSE','UTILITIES', 'FEES_AND_OTHER_COSTS' , 'INSURANCE', 'TAXES', 
                   'ACCOUNTING_ACCRUALS']
    Total_Reliability_list = ['OVERHAUL_BREAKDOWN','OVERHAUL_CONDITION_BASED','TURNAROUND_TURNARO',
                          'PSV_TESTING','MECHANICAL_INTEGRITY_INSPECT']
    Total_Pipeline_Integrity_list = ['LU', 'PIPE_INTEGRITY_COMPLIANCE' ,'CP_CORROSION_PROGRAM']

    df['Total_Base'] = df[Total_Base_list].sum(axis=1)
    df['Total_Reliability'] =df[Total_Reliability_list].sum(axis=1)
    df['Total_Pipeline_Integrity'] = df[Total_Pipeline_Integrity_list].sum(axis=1)

    Total_list = ['Total_Base','Total_Reliability', 'Total_Pipeline_Integrity', 'CORPORATE_ENTRIES_MANAGED']
    df['Total'] = df[Total_list].sum(axis = 1)
    df.Total = df.Total.astype('int64', copy=False)
    return df

def opex_asset(allocation_name, df, categories):
    ## find all routs where allocation name is found
    
    a_type = df[df['ALLOCATION_NAME'] == allocation_name]['ALLOCATION_TYPE'].values[0]
    a_total =  df[df['ALLOCATION_NAME'] == allocation_name][categories].values[0]
    a_total = a_total.astype('int64', copy=False)
    Temp = pd.DataFrame()
    for i in range(len(Route.ROUTE_NUMBER)):
        
        if allocation_name in check_routing(i):
 
            #find all meters that belong to this route and contain the allocation name
            #get $total of this allocation_name
            
            #get the table for all meter associated with this allocation_name
            temp_table = virtual_meter_table(check_routing(i)[0]).reset_index(drop = 'True')   
            
             #Assign "Allocation_name"
            temp_table['Allocation_name']=allocation_name
            
            #Assign route number
            temp_table['route'] = check_routing(i)[0] 
            
            
            #append the table
            
            Temp = Temp.append(temp_table, ignore_index = True, sort=False)
            
            #route_in_coeff = check_routing(i)[0] in coeff
    
    #Temp[a_type+'_opex'] = Temp.MCF/Temp.MCF.sum()*a_total
    if len(Temp) >0:
    #Getting an Opex load split between meters based on their MCF contribution%
        for k in range(len(categories)):
            Temp[categories[k]+'_opex'] = Temp.MCF/Temp.MCF.sum()*a_total[k]
        
        Temp[a_type+'_opex'] = Temp.MCF/Temp.MCF.sum()*df[df['ALLOCATION_NAME'] == allocation_name]['Total'].values[0]
        return Temp


# In[411]:



Opex = Opex.drop(['DATE'], axis=1)
opex = Opex.groupby(['ALLOCATION_NAME', 'ALLOCATION_TYPE']).sum().reset_index()
opex = Opex_Total(opex)
opex.head()


# In[412]:


#columns = list(opex.columns)
categories = ['LABOR_AND_BENEFITS',
       'MATERIALS_AND_SUPPLIES', 'CHEMICAL_AND_LUBE_PURCHASES', 'IT_PURCHASES',
       'EMPLOYEE_EXPENSES', 'TRANSPORTATION', 'CONTRACT_SERVICES',
       'LEASE_EXPENSE', 'UTILITIES', 'FEES_AND_OTHER_COSTS', 'INSURANCE',
       'TAXES', 'ACCOUNTING_ACCRUALS', 'OVERHAUL_BREAKDOWN',
       'OVERHAUL_CONDITION_BASED', 'TURNAROUND_TURNARO', 'PSV_TESTING',
       'MECHANICAL_INTEGRITY_INSPECT', 'LU', 'PIPE_INTEGRITY_COMPLIANCE',
       'CP_CORROSION_PROGRAM', 'CORPORATE_ENTRIES_MANAGED', 'Total_Base','Total_Reliability','Total_Pipeline_Integrity','Total']
#categories  = columns[2:len(columns)]
print (categories)

opexDF = pd.DataFrame()

for line in tqdm(opex['ALLOCATION_NAME']):
    opexDF = opexDF.append(opex_asset(line, opex, categories), ignore_index = True, sort=False)


# In[413]:


opexDF = opexDF.drop(['MCF','ROUTE_NUMBER'], 1)
opexDF = opexDF.groupby(['TIPS_METER']).sum().reset_index()
len(opexDF.TIPS_METER)
opexDF.head()


# In[415]:


new_opex = pd.merge(opexDF, Coeff[['TIPS_METER', 'ROUTE_NUMBER']], on = 'TIPS_METER', how = 'inner')


# In[416]:


new_opex.head()


# In[418]:


output_1 = pd.merge(OUTPUT, new_opex, on='TIPS_METER', how='inner')
output_1.head()


# In[421]:


# set(pd.unique(OUTPUT.TIPS_METER)).symmetric_difference((pd.unique(opexDF.TIPS_METER)))


# ## UI calcs

# In[423]:


df1 = output_1
date1 = date
df1['CNTR_LEVERAGE_RESIDUE_USD'] = df1.Producer_RES_AFS*(1-df1.RES_POP)*df1.DCP_RES_Price



df1['LEVERAGE_CONDENSATE_USD'] = (df1.DCP_Produced_Condensate-df1.Producer_Settled_Condensate)*df1.DCP_Condensate_Price

df1['LEVERAGE_HELIUM_USD'] = (df1.DCP_He_Production-df1.Producer_Settled_He)*df1.DCP_He_Price

df1['LEVERAGE_FEE_USD'] = df1.Fees
df1['LEVERAGE_RESIDUE_PRICE']= (df1.DCP_RES_Price - df1.Producer_RES_price)*df1.Producer_Settled_allocated
df1['LEVERAGE_TF_PRICE'] = df1.Producer_Settled_C2*df1.NGL_NTP_C2- df1.Producer_NGL_Revenue_C2 + df1.Producer_Settled_C3*df1.NGL_NTP_C3 - df1.Producer_NGL_Revenue_C3 +df1.Producer_Settled_IC4*df1.NGL_NTP_IC4 - df1.Producer_NGL_Revenue_IC4 + df1.Producer_Settled_NC4*df1.NGL_NTP_NC4 - df1.Producer_NGL_Revenue_NC4+df1.Producer_Settled_C5*df1.NGL_NTP_C5-  df1.Producer_NGL_Revenue_C56

df1['LEVERAGE_HELIUM_PRICE'] = (df1.DCP_He_Price-df1.Producer_He_Price)*df1.Producer_Settled_He

df1['LEVERAGE_CONDENSATE_PRICE'] = (df1.DCP_Condensate_Price -df1.Producer_Condensate_Price)*df1.Producer_Settled_Condensate

df1['LEVERAGE_UPLIFT'] = df1.DCP_Uplift
df1['DIRECT_COST'] = df1.Booster_opex

df1['OP_LEVERAGE_C5_RECOVERY'] = (df1.Producer_Theoretical_C5+df1.Producer_Theoretical_C6)*(df1.Recovery_pct_C5-df1.Producer_Recovery_C5)*df1.NGL_POP*(df1.NGL_NTP_C5 +(df1.NGL_NTP_C5-df1.Producer_NGL_NTP_C5))

df1['OP_LEVERAGE_NC4_RECOVERY'] = df1.Producer_Theoretical_NC4*(df1.Recovery_pct_NC4-df1.Producer_Recovery_NC4)*df1.NGL_POP*(df1.NGL_NTP_NC4+(df1.NGL_NTP_NC4-df1.Producer_NGL_NTP_NC4))

df1['OP_LEVERAGE_IC4_RECOVERY'] = df1.Producer_Theoretical_IC4*(df1.Recovery_pct_IC4-df1.Producer_Recovery_IC4)*df1.NGL_POP*(df1.NGL_NTP_IC4+(df1.NGL_NTP_IC4-df1.Producer_NGL_NTP_IC4))

df1['OP_LEVERAGE_C3_RECOVERY'] = df1.Producer_Theoretical_C3*(df1.Recovery_pct_C3-df1.Producer_Recovery_C3)*df1.NGL_POP*(df1.NGL_NTP_C3+(df1.NGL_NTP_C3-df1.Producer_NGL_NTP_C3))

df1['OP_LEVERAGE_C2_RECOVERY'] = df1.Producer_Theoretical_C2*(df1.Recovery_pct_C2-df1.Producer_Recovery_C2)*df1.NGL_POP*(df1.NGL_NTP_C2+(df1.NGL_NTP_C2-df1.Producer_NGL_NTP_C2))

df1['OP_LEVERAGE_FIELD_FUEL'] = (df1.Producer_Field_Fuel*df1.MMBTU - df1.DCP_Field_Fuel*df1.MMBTU)*df1.DCP_RES_Price

df1['OP_LEVERAGE_PLANT_FUEL'] = (df1.Producer_plant_fuel * df1.Producer_ND_mmbtu - df1.DCP_plant_fuel*df1.ND_mmbtu)*df1.DCP_RES_Price

df1['OP_LEVERAGE_SHRINK_FUEL'] = (df1.Producer_Shrink-df1.DCP_Shrink)*df1.DCP_RES_Price

df1['OP_LEVERAGE_LU_FUEL'] = df1.MMBTU*(df1.Producer_LU-df1.DCP_LU)*df1.DCP_RES_Price
df1['LEVERAGE_NGL_USD'] = df1.DCP_NGL_Revenue- df1.Producer_NGL_Revenue-df1.LEVERAGE_TF_PRICE-(df1.OP_LEVERAGE_C5_RECOVERY+df1.OP_LEVERAGE_NC4_RECOVERY+df1.OP_LEVERAGE_IC4_RECOVERY+df1.OP_LEVERAGE_C3_RECOVERY+df1.OP_LEVERAGE_C2_RECOVERY)


df1['WH_MM_CFD'] = df1.MCF/pd.to_datetime(date1).days_in_month
df1['EBITDA'] = df1.Margin- df1.Total_opex
df1['DATE'] = date1
df1['SUPERSYSTEM'] = 'NO'
# df1.columns


# In[424]:


column_order = ['DATE','TIPS_METER','MCF','MMBTU','DCP_Field_Fuel','DCP_LU','ND_mmbtu','ND_mcf','DCP_NGL_Revenue',
                'Recovery_pct_C2','Recovery_pct_C3','Recovery_pct_IC4','Recovery_pct_NC4','Recovery_pct_C5',
                'Recovery_pct_C6','DCP_Theoretical_C2','DCP_Theoretical_C3','DCP_Theoretical_IC4','DCP_Theoretical_NC4',
                'DCP_Theoretical_C5','DCP_Theoretical_C6','DCP_total_Theoretical_G','DCP_Recovered_Gallons_C2',
                'DCP_Recovered_Gallons_C3','DCP_Recovered_Gallons_IC4','DCP_Recovered_Gallons_NC4','DCP_Recovered_Gallons_C5',
                'DCP_Recovered_Gallons_C6','DCP_total_recovered_G','NGL_NTP_C2','NGL_NTP_C3','NGL_NTP_IC4',
                'NGL_NTP_NC4','NGL_NTP_C5','NGL_TF_C2','NGL_TF_C3','NGL_TF_IC4','NGL_TF_NC4','NGL_TF_C5',
                'DCP_RES_Revenue','DCP_Shrink','DCP_plant_fuel','DCP_RES_AFS','DCP_RES_Price','Fees','DCP_Condensate_Revenue',
                'DCP_Produced_Condensate','DCP_Condensate_Price','DCP_He_Revenue','DCP_He_Production','DCP_He_Price',
                'DCP_Uplift','DCP_Revenue','Producer_Field_Fuel','Producer_LU','Producer_ND_mmbtu','Producer_ND_mcf',
                'Producer_Recovery_C2','Producer_Recovery_C3','Producer_Recovery_IC4','Producer_Recovery_NC4','Producer_Recovery_C5',
                'Producer_Recovery_C6','Producer_Recovery_HE','Producer_NGL_Revenue','Producer_Settled_C2',
                'Producer_Settled_C3','Producer_Settled_IC4','Producer_Settled_NC4','Producer_Settled_C5',
                'Producer_Settled_G','Producer_NGL_NTP_C2','Producer_NGL_NTP_C3','Producer_NGL_NTP_IC4',
                'Producer_NGL_NTP_NC4','Producer_NGL_NTP_C5','Producer_NGL_Revenue_C2','Producer_NGL_Revenue_C3','Producer_NGL_Revenue_IC4','Producer_NGL_Revenue_NC4',
                'Producer_NGL_Revenue_C56','NGL_POP','Producer_Recovered_C2','Producer_Recovered_C3','Producer_Recovered_IC4','Producer_Recovered_NC4','Producer_Recovered_C5',
                'Producer_Recovered_C6','Producer_Theoretical_C2','Producer_Theoretical_C3','Producer_Theoretical_IC4','Producer_Theoretical_NC4','Producer_Theoretical_C5',
                'Producer_Theoretical_C6','Producer_Theoretical_total','Producer_Recovered_total','Producer_RES_Revenue','Producer_Shrink','Producer_plant_fuel','Producer_RES_AFS',
                'Producer_Settled_allocated','Producer_RES_price','RES_POP','Producer_Condensate_Revenue','Producer_Produced_Condensate','Producer_Settled_Condensate',
                'Producer_Condensate_Price','Condensate_POP','Producer_He_Revenue','Producer_He_Production','Producer_Settled_He','Producer_He_Price','He_POP','Producer_Revenue',
                'Margin','route','LABOR_AND_BENEFITS_opex','MATERIALS_AND_SUPPLIES_opex','CHEMICAL_AND_LUBE_PURCHASES_opex','IT_PURCHASES_opex','EMPLOYEE_EXPENSES_opex',
                'TRANSPORTATION_opex','CONTRACT_SERVICES_opex','LEASE_EXPENSE_opex','UTILITIES_opex','FEES_AND_OTHER_COSTS_opex','INSURANCE_opex','TAXES_opex','ACCOUNTING_ACCRUALS_opex',
                'OVERHAUL_BREAKDOWN_opex','OVERHAUL_CONDITION_BASED_opex','TURNAROUND_TURNARO_opex','PSV_TESTING_opex','MECHANICAL_INTEGRITY_INSPECT_opex','LU_opex',
                'PIPE_INTEGRITY_COMPLIANCE_opex','CP_CORROSION_PROGRAM_opex','CORPORATE_ENTRIES_MANAGED_opex','Total_Base_opex','Total_Reliability_opex','Total_Pipeline_Integrity_opex',
                'Total_opex','Booster_opex','Gathering_opex','Plant_opex','Area_opex','CNTR_LEVERAGE_RESIDUE_USD','LEVERAGE_CONDENSATE_USD','LEVERAGE_HELIUM_USD','LEVERAGE_FEE_USD',
                'LEVERAGE_RESIDUE_PRICE','LEVERAGE_TF_PRICE','LEVERAGE_HELIUM_PRICE','LEVERAGE_CONDENSATE_PRICE','LEVERAGE_UPLIFT','DIRECT_COST','OP_LEVERAGE_C5_RECOVERY',
                'OP_LEVERAGE_NC4_RECOVERY','OP_LEVERAGE_IC4_RECOVERY','OP_LEVERAGE_C3_RECOVERY','OP_LEVERAGE_C2_RECOVERY','OP_LEVERAGE_FIELD_FUEL','OP_LEVERAGE_PLANT_FUEL',
                'OP_LEVERAGE_SHRINK_FUEL','OP_LEVERAGE_LU_FUEL','LEVERAGE_NGL_USD','WH_MM_CFD','EBITDA','SUPERSYSTEM']


# In[425]:


df_margin_opex_ui = df1[column_order]
#file_name = 'output/Margin-OPEX-UI-'+str(months[0])+'.csv'
#df_margin_opex_ui.to_csv(file_name, index=False)


#In[427]:

ctx.cursor().execute('USE DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION')
cur = ctx.cursor()
try:
#     df = pandas.read_sql_query("SELECT DATE1, INDEX1, PRICE FROM DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION.PRICING_COEFFICIENTS ORDER BY DATE1",con)
    df_margin_opex_ui.to_csv(r'C:\Data\output.csv', sep='|', encoding='utf-8', header=False, index=False)
    
    ctx.cursor().execute("DELETE FROM STAGE_SOM_METER_CALC_OUTPUT_V2 WHERE MONTH_DATE LIKE '"+str(Date)+"'")

    #con.cursor().execute("CREATE OR REPLACE TABLE test_python_load(ID integer, DATE1 timestamp_ntz, INDEX1 varchar(128), PRICE FLOAT )")
    ctx.cursor().execute("PUT file://C:/Data/output.csv @%STAGE_SOM_METER_CALC_OUTPUT_V2")
    ctx.cursor().execute("COPY INTO STAGE_SOM_METER_CALC_OUTPUT_V2 file_format = (format_name = 'XLS_PIPE')")

    ctx.cursor().execute(" delete from SYSTEM_OPTIMIZATION.SOM_METER_CALC_OUTPUT_V2 where month_date like '"+str(Date)+"'")
    ctx.cursor().execute(" INSERT INTO SYSTEM_OPTIMIZATION.SOM_METER_CALC_OUTPUT_V2 ( MONTH_DATE,TIPS_METER,MCF,MMBTU,DCP_Field_Fuel,DCP_LU,ND_mmbtu,ND_mcf,DCP_NGL_Revenue, Recovery_pct_C2,Recovery_pct_C3,Recovery_pct_IC4,Recovery_pct_NC4,Recovery_pct_C5, Recovery_pct_C6,DCP_Theoretical_C2,DCP_Theoretical_C3,DCP_Theoretical_IC4,DCP_Theoretical_NC4, DCP_Theoretical_C5,DCP_Theoretical_C6,DCP_total_Theoretical_G,DCP_Recovered_Gallons_C2, DCP_Recovered_Gallons_C3,DCP_Recovered_Gallons_IC4,DCP_Recovered_Gallons_NC4,DCP_Recovered_Gallons_C5, DCP_Recovered_Gallons_C6,DCP_total_recovered_G,NGL_NTP_C2,NGL_NTP_C3,NGL_NTP_IC4, NGL_NTP_NC4,NGL_NTP_C5,NGL_TF_C2,NGL_TF_C3,NGL_TF_IC4,NGL_TF_NC4,NGL_TF_C5, DCP_RES_Revenue,DCP_Shrink,DCP_plant_fuel,DCP_RES_AFS,DCP_RES_Price,Fees,DCP_Condensate_Revenue, DCP_Produced_Condensate,DCP_Condensate_Price,DCP_He_Revenue,DCP_He_Production,DCP_He_Price, DCP_Uplift,DCP_Revenue,Producer_Field_Fuel,Producer_LU,Producer_ND_mmbtu,Producer_ND_mcf, Producer_Recovery_C2,Producer_Recovery_C3,Producer_Recovery_IC4,Producer_Recovery_NC4,Producer_Recovery_C5, Producer_Recovery_C6,Producer_Recovery_HE,Producer_NGL_Revenue,Producer_Settled_C2, Producer_Settled_C3,Producer_Settled_IC4,Producer_Settled_NC4,Producer_Settled_C5, Producer_Settled_G,Producer_NGL_NTP_C2,Producer_NGL_NTP_C3,Producer_NGL_NTP_IC4, Producer_NGL_NTP_NC4,Producer_NGL_NTP_C5,Producer_NGL_Revenue_C2,Producer_NGL_Revenue_C3,Producer_NGL_Revenue_IC4,Producer_NGL_Revenue_NC4, Producer_NGL_Revenue_C56,NGL_POP,Producer_Recovered_C2,Producer_Recovered_C3,Producer_Recovered_IC4,Producer_Recovered_NC4,Producer_Recovered_C5, Producer_Recovered_C6,Producer_Theoretical_C2,Producer_Theoretical_C3,Producer_Theoretical_IC4,Producer_Theoretical_NC4,Producer_Theoretical_C5, Producer_Theoretical_C6,Producer_Theoretical_total,Producer_Recovered_total,Producer_RES_Revenue,Producer_Shrink,Producer_plant_fuel,Producer_RES_AFS, Producer_Settled_allocated,Producer_RES_price,PRODUCER_RES_POP,Producer_Condensate_Revenue,Producer_Produced_Condensate,Producer_Settled_Condensate, Producer_Condensate_Price,Condensate_POP,Producer_He_Revenue,Producer_He_Production,Producer_Settled_He,Producer_He_Price,He_POP,Producer_Revenue, Margin,route,LABOR_AND_BENEFITS_opex,MATERIALS_AND_SUPPLIES_opex,CHEMICAL_AND_LUBE_PURCHASES_opex,IT_PURCHASES_opex,EMPLOYEE_EXPENSES_opex, TRANSPORTATION_opex,CONTRACT_SERVICES_opex,LEASE_EXPENSE_opex,UTILITIES_opex,FEES_AND_OTHER_COSTS_opex,INSURANCE_opex,TAXES_opex,ACCOUNTING_ACCRUALS_opex, OVERHAUL_BREAKDOWN_opex,OVERHAUL_CONDITION_BASED_opex,TURNAROUND_TURNARO_opex,PSV_TESTING_opex,MECHANICAL_INTEGRITY_INSPECT_opex,LU_opex, PIPE_INTEGRITY_COMPLIANCE_opex,CP_CORROSION_PROGRAM_opex,CORPORATE_ENTRIES_MANAGED_opex,Total_Base_opex,Total_Reliability_opex,Total_Pipeline_Integrity_opex, Total_opex,Booster_opex,Gathering_opex,Plant_opex,Area_opex,CNTR_LEVERAGE_RESIDUE_USD,LEVERAGE_CONDENSATE_USD,LEVERAGE_HELIUM_USD,LEVERAGE_FEE_USD, LEVERAGE_RESIDUE_PRICE,LEVERAGE_TF_PRICE,LEVERAGE_HELIUM_PRICE,LEVERAGE_CONDENSATE_PRICE,LEVERAGE_UPLIFT,DIRECT_COST,OP_LEVERAGE_C5_RECOVERY, OP_LEVERAGE_NC4_RECOVERY,OP_LEVERAGE_IC4_RECOVERY,OP_LEVERAGE_C3_RECOVERY,OP_LEVERAGE_C2_RECOVERY,OP_LEVERAGE_FIELD_FUEL,OP_LEVERAGE_PLANT_FUEL, OP_LEVERAGE_SHRINK_FUEL,OP_LEVERAGE_LU_FUEL,LEVERAGE_NGL_USD,WH_MM_CFD,EBITDA,SUPERSYSTEM) select  MONTH_DATE,TIPS_METER,MCF,MMBTU,DCP_Field_Fuel,DCP_LU,ND_mmbtu,ND_mcf,DCP_NGL_Revenue, Recovery_pct_C2,Recovery_pct_C3,Recovery_pct_IC4,Recovery_pct_NC4,Recovery_pct_C5, Recovery_pct_C6,DCP_Theoretical_C2,DCP_Theoretical_C3,DCP_Theoretical_IC4,DCP_Theoretical_NC4, DCP_Theoretical_C5,DCP_Theoretical_C6,DCP_total_Theoretical_G,DCP_Recovered_Gallons_C2, DCP_Recovered_Gallons_C3,DCP_Recovered_Gallons_IC4,DCP_Recovered_Gallons_NC4,DCP_Recovered_Gallons_C5, DCP_Recovered_Gallons_C6,DCP_total_recovered_G,NGL_NTP_C2,NGL_NTP_C3,NGL_NTP_IC4, NGL_NTP_NC4,NGL_NTP_C5,NGL_TF_C2,NGL_TF_C3,NGL_TF_IC4,NGL_TF_NC4,NGL_TF_C5, DCP_RES_Revenue,DCP_Shrink,DCP_plant_fuel,DCP_RES_AFS,DCP_RES_Price,Fees,DCP_Condensate_Revenue, DCP_Produced_Condensate,DCP_Condensate_Price,DCP_He_Revenue,DCP_He_Production,DCP_He_Price, DCP_Uplift,DCP_Revenue,Producer_Field_Fuel,Producer_LU,Producer_ND_mmbtu,Producer_ND_mcf, Producer_Recovery_C2,Producer_Recovery_C3,Producer_Recovery_IC4,Producer_Recovery_NC4,Producer_Recovery_C5, Producer_Recovery_C6,Producer_Recovery_HE,Producer_NGL_Revenue,Producer_Settled_C2, Producer_Settled_C3,Producer_Settled_IC4,Producer_Settled_NC4,Producer_Settled_C5, Producer_Settled_G,Producer_NGL_NTP_C2,Producer_NGL_NTP_C3,Producer_NGL_NTP_IC4, Producer_NGL_NTP_NC4,Producer_NGL_NTP_C5,Producer_NGL_Revenue_C2,Producer_NGL_Revenue_C3,Producer_NGL_Revenue_IC4,Producer_NGL_Revenue_NC4, Producer_NGL_Revenue_C56,NGL_POP,Producer_Recovered_C2,Producer_Recovered_C3,Producer_Recovered_IC4,Producer_Recovered_NC4,Producer_Recovered_C5, Producer_Recovered_C6,Producer_Theoretical_C2,Producer_Theoretical_C3,Producer_Theoretical_IC4,Producer_Theoretical_NC4,Producer_Theoretical_C5, Producer_Theoretical_C6,Producer_Theoretical_total,Producer_Recovered_total,Producer_RES_Revenue,Producer_Shrink,Producer_plant_fuel,Producer_RES_AFS, Producer_Settled_allocated,Producer_RES_price,PRODUCER_RES_POP,Producer_Condensate_Revenue,Producer_Produced_Condensate,Producer_Settled_Condensate, Producer_Condensate_Price,Condensate_POP,Producer_He_Revenue,Producer_He_Production,Producer_Settled_He,Producer_He_Price,He_POP,Producer_Revenue, Margin,route,LABOR_AND_BENEFITS_opex,MATERIALS_AND_SUPPLIES_opex,CHEMICAL_AND_LUBE_PURCHASES_opex,IT_PURCHASES_opex,EMPLOYEE_EXPENSES_opex, TRANSPORTATION_opex,CONTRACT_SERVICES_opex,LEASE_EXPENSE_opex,UTILITIES_opex,FEES_AND_OTHER_COSTS_opex,INSURANCE_opex,TAXES_opex,ACCOUNTING_ACCRUALS_opex, OVERHAUL_BREAKDOWN_opex,OVERHAUL_CONDITION_BASED_opex,TURNAROUND_TURNARO_opex,PSV_TESTING_opex,MECHANICAL_INTEGRITY_INSPECT_opex,LU_opex, PIPE_INTEGRITY_COMPLIANCE_opex,CP_CORROSION_PROGRAM_opex,CORPORATE_ENTRIES_MANAGED_opex,Total_Base_opex,Total_Reliability_opex,Total_Pipeline_Integrity_opex, Total_opex,Booster_opex,Gathering_opex,Plant_opex,Area_opex,CNTR_LEVERAGE_RESIDUE_USD,LEVERAGE_CONDENSATE_USD,LEVERAGE_HELIUM_USD,LEVERAGE_FEE_USD, LEVERAGE_RESIDUE_PRICE,LEVERAGE_TF_PRICE,LEVERAGE_HELIUM_PRICE,LEVERAGE_CONDENSATE_PRICE,LEVERAGE_UPLIFT,DIRECT_COST,OP_LEVERAGE_C5_RECOVERY, OP_LEVERAGE_NC4_RECOVERY,OP_LEVERAGE_IC4_RECOVERY,OP_LEVERAGE_C3_RECOVERY,OP_LEVERAGE_C2_RECOVERY,OP_LEVERAGE_FIELD_FUEL,OP_LEVERAGE_PLANT_FUEL, OP_LEVERAGE_SHRINK_FUEL,OP_LEVERAGE_LU_FUEL,LEVERAGE_NGL_USD,WH_MM_CFD,EBITDA,SUPERSYSTEM from SYSTEM_OPTIMIZATION.STAGE_SOM_METER_CALC_OUTPUT_V2 where month_date LIKE '" + str(Date) + "'")

finally:
    cur.close()
