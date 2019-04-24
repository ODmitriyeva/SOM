
# coding: utf-8

# In[1]:


import snowflake.connector
# Gets the version
import  pandas as pd
import numpy as np
from tqdm import tqdm


# In[2]:


import requests
r = requests.get('http://ocsp.digicert.com/')
r.text


# In[3]:


ACCOUNT =  "po91914.east-us-2.azure"
USER =  "_sdatabricks_service"
PASSWORD = "DBDcpSCab@2018!2"

sfDatabase="DATAWAREHOUSE_DB",
sfSchema="SYSTEM_OPTIMIZATION",
sfWarehouse="DATABRICKS_WH"


# In[4]:


ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    insecure_mode=True
    )
ctx.cursor().execute('USE warehouse DATABRICKS_WH')
ctx.cursor().execute('USE DATAWAREHOUSE_DB.SYSTEM_OPTIMIZATION')


cur = ctx.cursor()
ROW = []
try:
    cur.execute('SELECT * FROM "DATAWAREHOUSE_DB"."SYSTEM_OPTIMIZATION"."V_METER_COEFFICIENTS"')
    coeff = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    cur.execute('SELECT * FROM "DATAWAREHOUSE_DB"."SYSTEM_OPTIMIZATION"."V_BOOSTER_COEFFICIENTS"')
    reference_booster = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    cur.execute('SELECT * FROM "DATAWAREHOUSE_DB"."SYSTEM_OPTIMIZATION"."V_PLANT_COEFFICIENTS"')
    reference_plant = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    cur.execute('SELECT * FROM "DATAWAREHOUSE_DB"."SYSTEM_OPTIMIZATION"."V_PRICING_COEFFICIENTS"')
    reference_price = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    cur.execute('SELECT * FROM "DATAWAREHOUSE_DB"."SYSTEM_OPTIMIZATION"."V_STATIC_REFERENCE_DATA"')
    reference_static = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    cur.execute('SELECT * FROM "DATAWAREHOUSE_DB"."SYSTEM_OPTIMIZATION"."V_ROUTING"')
    reference_routing = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    
    cur.execute('SELECT * FROM "DATAWAREHOUSE_DB"."SYSTEM_OPTIMIZATION"."V_OPEX"')
    reference_opex = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        
finally:
    cur.close()
reference_plant[reference_plant.PLANT == 'Pegasus']


# In[5]:


coeff = coeff[~coeff.ROUTE_NUMBER.isnull()]


# In[6]:


coeff = coeff.reset_index(drop = True)
len(coeff['TIPS_METER'])


# In[7]:


coeff = coeff.replace(np.nan, 0)
reference_plant = reference_plant.replace(np.nan, 0)
reference_price = reference_price.replace(np.nan, 0)
reference_booster = reference_booster.replace(np.nan, 0)
reference_opex = reference_opex.replace(np.nan, 0)
reference_static = reference_static.replace(np.nan, 0)

coeff['DATE'] = pd.to_datetime(coeff['DATE'])
reference_price['DATE'] = pd.to_datetime(reference_price['DATE'])
reference_plant['DATE'] = pd.to_datetime(reference_plant['DATE'])
reference_booster['DATE'] = pd.to_datetime(reference_booster['DATE'])
reference_routing['DATE'] = pd.to_datetime(reference_routing['DATE'])
reference_opex['DATE'] = pd.to_datetime(reference_opex['DATE'])


# In[8]:


rec_dcp_dict = {}
rec_prod_dict={}


# In[9]:


TIPs_Meter = '701632-0000'
#'13135198'#'6375711'#'13135172'#'75118'
date1 = '2019-01-01 00:00:00'
#date2 = '2019-01-01 00:00:00'
print()
Coeff = coeff[coeff['DATE'] == date1].reset_index(drop = True)
Price = reference_price[reference_price['DATE'] == date1].reset_index(drop = True)
Plant = reference_plant[reference_plant['DATE'] == date1].reset_index(drop = True)
Booster = reference_booster[reference_booster['DATE'] == date1].reset_index(drop = True)
Route = reference_routing[reference_routing['DATE'] == date1].reset_index(drop = True)
Opex = reference_opex[reference_opex['DATE'] == date1].reset_index(drop = True)
Static = reference_static


# In[10]:


print (len(pd.unique(Coeff['ROUTE_NUMBER'])))
print (len(Coeff))
Coeff.ROUTE_NUMBER.values


# In[11]:


def Converting_to_float(df):
    for column in df.columns:
        try:
            df[column] = pd.to_numeric(df[column],  errors='ignore')
 
        except:
            pass
      
    return df


# In[14]:


Coeff = Converting_to_float(Coeff)
Booster = Converting_to_float(Booster)
Price = Converting_to_float(Price)
Route = Converting_to_float(Route)
Opex = Converting_to_float(Opex)
Static = Converting_to_float(Static)
Plant = Converting_to_float(Plant)


# In[15]:


def Asset_from_Routing(meter):
    route = int(Coeff.loc[meter]['ROUTE_NUMBER'])
   
    b1 = Route[Route['ROUTE_NUMBER'] == route]['BOOSTER_NAME'].values[0]
    b2 = Route[Route['ROUTE_NUMBER'] == route]['SECONDARY_BOOSTER'].values[0]
    b3 = Route[Route['ROUTE_NUMBER'] == route]['TERTIARY_BOOSTER'].values[0]
    p = Route[Route['ROUTE_NUMBER'] == route]['PLANT_NAME'].values[0]
    gs = Route[Route['ROUTE_NUMBER'] == route]['GATHERING_SYSTEM'].values[0]
    a = Route[Route['ROUTE_NUMBER'] == route]['AREA'].values[0]
    return [route,b1,b2,b3,p,gs,a]
    


# ## Choosing meter only for one AREA

# In[16]:


len(Coeff[Coeff.AREA == 'SENM'])
area = pd.unique(Coeff.AREA)
n=0
for a in area:
    print(a,len(Coeff[Coeff.AREA == a]) )
    n = n+len(Coeff[Coeff.AREA == a])
print (n)


# In[17]:


Coeff = Coeff[Coeff.AREA == 'SENM'].reset_index(drop = True)
print (len(Coeff))


# ## DCP side

# In[18]:


def LU(meter):
    route,b1,b2,b3,p,gs,a = Asset_from_Routing(meter)
    
    if (b1!=0) and (b1 in Booster['BOOSTER'].values):
        return Booster[Booster['BOOSTER']==b1]['ACTUAL_LU_TO_BOOSTER_PCT'].values[0]
    else:
        return 0.0

def Fuel(meter):
    route,b1,b2,b3,p,gs,a = Asset_from_Routing(meter)
    if (b1!=0) and (b1 in Booster['BOOSTER'].values):
        return Booster[Booster['BOOSTER']== b1]['ACTUAL_FUEL_TO_BOOSTER_PCT'].values[0]
    else: 
        return 0.0

def total_MCF(meter):
    return Coeff.loc[meter]['MCF']

def total_MMBTU(meter):
    #return coeff.loc[meter]['MCF']*coeff.loc[meter]['BTU']
    return Coeff.loc[meter]['MMBTU']


# In[19]:


# print (Asset_from_Routing(2)[1])
# Fuel(2)
# Booster[Booster.BOOSTER == 'Bell Lake Bstr']
Fuel(3)
LU(3)


# In[20]:


def Net_delivered_mcf(meter):
    LU_mcf = total_MCF(meter)*LU(meter)
    Fuel_mcf = total_MCF(meter)*Fuel(meter)
    Net_deliv_mcf = total_MCF(meter)-LU_mcf-Fuel_mcf
    return (Net_deliv_mcf)
  
def Net_delivered_mmbtu(meter):
    LU_mmbtu = total_MMBTU(meter)*LU(meter)
    Fuel_mmbtu = total_MMBTU(meter)*Fuel(meter)
    Net_deliv_mmbtu = total_MMBTU(meter)- LU_mmbtu-Fuel_mmbtu
    return (Net_deliv_mmbtu)


# In[21]:


Net_delivered_mcf(82)


# In[22]:


def Recovery_DCP(meter,frac):
    #route,b1,b2,b3,p,gs,a = Asset_from_Routing(meter)
    if frac in rec_dcp_dict:
        recovery = rec_dcp_dict[frac][0]
        theoreticalG = rec_dcp_dict[frac][1]
        recoveredG = rec_dcp_dict[frac][2]
        shrink = rec_dcp_dict[frac][3]
        
        #print("     Recovery_DCP done (from dict)")    
        return recovery,theoreticalG, recoveredG, shrink
        
    else:
        p = Asset_from_Routing(meter)[4]
        recovery = Plant[Plant['PLANT']==p][frac+'_PCT_RECOVERY'].values
        rec_dcp_dict[frac] = [recovery]

        if frac == 'HE':
            theoreticalG = Net_delivered_mcf(meter)*Coeff.loc[meter][frac+'_MOL_PCT']/100.0#Fraction(meter,'He')[0]#*1000
            recoveredG = theoreticalG*recovery
            shrink = 0.0
        else:
            theoreticalG = Net_delivered_mcf(meter)*Coeff.loc[meter][str(frac)+'_GPM'] 
            recoveredG = theoreticalG*recovery
            shrink = recoveredG*Static[Static['PRODUCT'] == frac]['SHRINK_FACTOR'].values
        rec_dcp_dict[frac].append(theoreticalG)
        rec_dcp_dict[frac].append(recoveredG)
        rec_dcp_dict[frac].append(shrink)
        #print("     Recovery_DCP done")    
        return recovery,theoreticalG, recoveredG, shrink
        
        
    return recovery,theoreticalG, recoveredG, shrink
    
def DCP_Recovery_Total(meter):
    fractions = ['C2', 'C3', 'IC4', 'NC4', 'C5', 'C6']

    net_theoreticalG =[]
    net_recoveredG = []
    net_shrink =[]
   
    for frac in fractions:
        recovery,theoreticalG, recoveredG, shrink = Recovery_DCP(meter, frac)
   
        net_theoreticalG.append(theoreticalG)
        net_recoveredG.append(recoveredG)
        net_shrink.append(shrink)
    
        #print (frac, 'recovery %', recovery*100, '  Theoretical G', theoreticalG, '   Recovered G', recoveredG)
    total_net_theoretical_G = sum(net_theoreticalG)
    total_net_recovered_G= sum(net_recoveredG)
    total_shrink = sum(net_shrink)
    
  
    return total_net_theoretical_G, total_net_recovered_G, total_shrink


# In[23]:


print (Asset_from_Routing(175))
print (Coeff.TIPS_METER[175])
print (DCP_Recovery_Total(1))


# In[24]:


def DCP_NGL_market(meter,market):
    #route,b1,b2,b3,plant,gs,a = Asset_from_Routing(meter)
    plant = Asset_from_Routing(meter)[4]
    fractions_NGL = ['C2', 'C3', 'IC4','NC4', 'C5']
    DCP_market_index = []
    price = []
    if plant in Plant['PLANT'].values and plant != 0:
        for frac in fractions_NGL:
            index = Plant[Plant['PLANT']==plant]['DCP_NGL_MARKET'+str(market)+'_'+str(frac)].values[0]
            DCP_market_index.append(index)
            if index!= 0.0:
#             print(index)
                p = Price[Price['INDEX'] == index]['PRICE'].values
                price.append(p)
            else:
                price.append(0.0)
        pct = Plant[Plant['PLANT']==plant]['DCP_NGL_MARKET_'+str(market)+'_PCT'].values
        tf = Plant[Plant['PLANT']==plant]['DCP_TF_ON_NGL_MARKET_'+str(market)].values[0]
        return (DCP_market_index,price, pct, tf)
#     else:
#         return 0.0,0.0,0.0,0.0

def DCP_NTP(meter):
    
    fractions_NGL = ['C2', 'C3', 'IC4','NC4', 'C5']
    Market1 = DCP_NGL_market(meter,1)
    Market2 = DCP_NGL_market(meter,2)
    dcp_ntp = []
    dcp_tf = []
    for i in range(len(fractions_NGL)):
        
        m1 = (Market1[1][i]+Market1[3])*Market1[2]
        m2 = (Market2[1][i]+Market2[3])*Market2[2]
        m = m1+m2
       
        dcp_ntp.append(m)
        dcp_tf.append(Market1[3])
    return dcp_ntp, dcp_tf



def DCP_res_index(meter):
    
    DCP_RES_index = []
    for ind in range(5):
        DCP_RES_index.append(Plant[Plant['PLANT']==Coeff.loc[meter]['PLANT_NAME']]['DCP_RES_INDEX_'+str(ind+1)].values)
    DCP_RES_deduct = []
    for ind in range(5):
        DCP_RES_deduct.append(Plant[Plant['PLANT']==Coeff.loc[meter]['PLANT_NAME']]['DCP_RES_DEDUCT_INDEX_'+str(ind+1)].values)
    DCP_RES_pct = []
    for ind in range(5):
        DCP_RES_pct.append(Plant[Plant['PLANT']==Coeff.loc[meter]['PLANT_NAME']]['DCP_RES_INDEX_'+str(ind+1)+'_PCT'].values)
        
    return (DCP_RES_index, DCP_RES_deduct, DCP_RES_pct)


# In[25]:


DCP_NGL_market(3,1) ### meter Plant_Name is not in PLANT table
DCP_res_index(3)
DCP_NTP(3)### meter Plant_Name is not in PLANT table


# In[26]:


def Uplift_DCP(meter):
    #route,b1,b2,b3,plant,gs,a = Asset_from_Routing(meter)
    plant = Asset_from_Routing(meter)[4]
    if plant in Plant['PLANT'].values and plant != 0:
        index = Plant[Plant['PLANT']== plant]['UPLIFT_PIPELINE'].values[0]
        
        #print (index)
        if index!= 0.0:
            price = Price[Price['INDEX'] == index]['PRICE'].values[0]
        else:
            price = 0.0
        uplift = DCP_Recovery_Total(meter)[1]*price
        return uplift
    else:
        return []


# In[27]:


Uplift_DCP(4)


# In[28]:


def DCP_NGL_Revenue(meter):
    
    fractions = ['C2', 'C3', 'IC4', 'NC4', 'C5', 'C6']

    recoveries = []
    net_recovered_theoreticalG =[]
    net_recoveredG = []
    net_shrink =[]
   
    for frac in fractions:
        recovery,theoreticalG, recoveredG, shrink = Recovery_DCP(meter, frac)
        
        recoveries.append(recovery)
        net_recovered_theoreticalG.append(theoreticalG)
        net_recoveredG.append(recoveredG)
        net_shrink.append(shrink)
    
    
   
    total_net_recovered_theoretical_G, total_net_recovered_G, total_shrink = DCP_Recovery_Total(meter)

  
    fractions_NGL = ['C2', 'C3', 'IC4', 'NC4', 'C56']
    NGL_net_recoveredG = list(net_recoveredG)
    C56 = NGL_net_recoveredG[4]+NGL_net_recoveredG[5]
    NGL_net_recoveredG[4] = C56


    MARKETS = [1,2]
    DCP_NGL_revenue_Market = []
    price_Market = []
    for market in MARKETS:
        NGL_INDEX, PRICE, pct, tf = DCP_NGL_market(meter,market)
        price_Market.append(PRICE)
       
     
    
        #if len([PRICE])>1:
        for i in range(len(fractions_NGL)):
    ####### Do for each fraction recoveredG 
            
            NGL_fraction_revenue = (PRICE[i]+tf)*pct*NGL_net_recoveredG[i]
            
            DCP_NGL_revenue_Market.append(NGL_fraction_revenue[0])
            #print(fractions_NGL[i],'Price: ',PRICE[i], "     tf: ", tf, ' NGL revenue', NGL_fraction_revenue[0])
    Revenue_DCP_NGL = sum(DCP_NGL_revenue_Market)
#     print()
#     print ('total DCP_NGL_revenue ', Revenue_DCP_NGL)
#     print('*************************************')

    
    return Revenue_DCP_NGL, recoveries, net_recovered_theoreticalG,sum(net_recovered_theoreticalG), net_recoveredG, sum(net_recoveredG)


# In[29]:


DCP_NGL_Revenue(3)


# In[30]:


def DCP_Condensate_Revenue(meter):
    #route,b1,b2,b3,plant,gs,a = Asset_from_Routing(meter)
    plant = Asset_from_Routing(meter)[4]
    fractions_Condensate = ['C5', 'C6']
    condensate_recovered_theoretical_G = 0
    condensate_recovered_G = 0

    for frac in fractions_Condensate:
        recovery,theoreticalG, recoveredG, shrink = Recovery_DCP(meter, frac)
        condensate_recovered_theoretical_G += theoreticalG
        condensate_recovered_G += recoveredG
    if len(Plant[Plant['PLANT']== plant]['CONDENSATE_PCT_RECOVERY'].values) >0:
    
    #print ('Recovered C5 + C6 gallons',condensate_recovered_G)
        condensate_recovery = Plant[Plant['PLANT']==plant]['CONDENSATE_PCT_RECOVERY'].values[0]
    #print ('Condensate Recovery %',condensate_recovery)
        DCP_condensate_G = condensate_recovered_G*condensate_recovery  
        DCP_condensate_barrels = DCP_condensate_G/42.
        price = Price[Price['INDEX'] == 'NYMEX - Crude']['PRICE'].values[0]
        diff = Plant[Plant['PLANT']==Coeff.loc[meter]['PLANT_NAME']]['DCP_CONDENSATE_DIFF'].values[0]
   
        Revenue_DCP_Condensate = (price+diff)*DCP_condensate_barrels#[0]
    #print ('DCP Condensate Revenue', Revenue_DCP_Condensate)
        return Revenue_DCP_Condensate, DCP_condensate_barrels[0], price+diff
    else:
        return 0.0 ## or empty array


# In[31]:


DCP_Condensate_Revenue(1)


# In[32]:


def DCP_RES_Revenue(meter):
    
    #route,b1,b2,b3,plant,gs,a = Asset_from_Routing(meter)
    plant = Asset_from_Routing(meter)[4]
    Plant_fuel_DCP_mmbtu = Net_delivered_mmbtu(meter)*Plant[Plant['PLANT']==plant]['PLANT_FUEL_PCT_MMBTU'].values
    total_shrink = DCP_Recovery_Total(meter)[2]

    RES_AFS = Net_delivered_mmbtu(meter)-total_shrink-Plant_fuel_DCP_mmbtu

    INDEX,DEDUCT,PCT = DCP_res_index(meter)
  
    Revenue_DCP_RES = 0.0
    for i in range(5):
        if INDEX[i] != 0:
        
            price = Price[Price['INDEX'] == INDEX[i][0]]['PRICE'].values[0]
            
            for_each_index = (price+DEDUCT[i][0])*PCT[i][0]*RES_AFS[0]
            Revenue_DCP_RES += for_each_index

#     print ('RES - REVENUE_DCP ', Revenue_DCP_RES)

#     print ('***************************************')
#     print ()
    return Revenue_DCP_RES,total_shrink[0], Plant[Plant['PLANT']==plant]['PLANT_FUEL_PCT_MMBTU'].values[0], RES_AFS[0]


# In[33]:


DCP_RES_Revenue(67)


# In[34]:


def DCP_RES_Price(meter):
    
    INDEX,DEDUCT,PCT = DCP_res_index(meter)
    price_res = 0.0
    for i in range(5):
        if INDEX[i] != 0:
        
            price = (Price[Price['INDEX'] == INDEX[i][0]]['PRICE'].values[0]+DEDUCT[i][0])*PCT[i][0]
            price_res += price
    return price_res


# In[35]:


DCP_RES_Price(3)
#Coeff.loc[1]['TIPS_METER']


# In[36]:


DCP_RES_Revenue(3)
# INDEX = ['NNG Demarc - IFGMR']

# Price[Price['INDEX'] == INDEX[0]]['PRICE'].values[0]


# In[37]:


def DCP_He_Revenue(meter):
    #route,b1,b2,b3,plant,gs,a = Asset_from_Routing(meter)
    
    plant = Asset_from_Routing(meter)[4]
    He_recovery = Recovery_DCP(meter,'HE')[0]
    DCP_He_produced = Recovery_DCP(meter,'HE')[2]
    index = Plant[Plant['PLANT']==plant]['HE_INDEX'].values[0]
    price = Price[Price['INDEX']==index]['PRICE'].values

   #print ('He price', price)
    Revenue_DCP_He = DCP_He_produced*price
    #print ('DCP He Revenue ',Revenue_DCP_He)
    return Revenue_DCP_He[0],DCP_He_produced,price


# In[38]:


DCP_He_Revenue(1)


# In[39]:


def DCP_Fees(meter):
    return Coeff.loc[meter]['FEES']


# In[40]:


def DCP_Revenue(meter): # +DCP_He_Revenue(meter)[0] +Uplift_DCP(meter)
    dcp_revenue = DCP_NGL_Revenue(meter)[0]+DCP_RES_Revenue(meter)[0] + DCP_He_Revenue(meter)[0] +Uplift_DCP(meter)+ DCP_Condensate_Revenue(meter)[0] + DCP_Fees(meter)
    return dcp_revenue


# In[41]:


print (DCP_Revenue(82))#,DCP_NGL_Revenue(2)[0], DCP_RES_Revenue(2)[0], DCP_Condensate_Revenue(2)[0], Uplift_DCP(2),  DCP_Fees(2))


# ## Producer

# In[42]:


def LU_producer(meter):
    if Coeff.loc[meter]['LU'] == 'ACTUAL':
        return LU(meter)
    
    else:
        LU_coefficient = Coeff.loc[meter]['PRODUCER_LU_PCT']
        return LU_coefficient

def Fuel_producer(meter):
    if Coeff.loc[meter]['FIELD_FUEL'] == 'ACTUAL':
       
        return Fuel(meter)
    else: 
        Fuel_coefficient = Coeff.loc[meter]['PRODUCER_FIELD_FUEL_PCT']
        return Fuel_coefficient
    
def Producer_Net_delivered_mcf(meter):
    LU_mcf = total_MCF(meter)*LU_producer(meter)
    Fuel_mcf = total_MCF(meter)*Fuel_producer(meter)
    Net_deliv_mcf = total_MCF(meter)-LU_mcf-Fuel_mcf
    return (Net_deliv_mcf)
  
def Producer_Net_delivered_mmbtu(meter):
    LU_mmbtu = total_MMBTU(meter)*LU_producer(meter)
    Fuel_mmbtu = total_MMBTU(meter)*Fuel_producer(meter)
    Net_deliv_mmbtu = total_MMBTU(meter)- LU_mmbtu-Fuel_mmbtu
    return (Net_deliv_mmbtu)

 
 
 


# In[43]:


print (Fuel_producer(66))
Producer_Net_delivered_mcf(66)
# total_MCF(6736)
# Coeff.loc[6736]['FIELD_FUEL']
# Coeff.loc[6736]['PRODUCER_FIELD_FUEL_PCT']


# In[44]:



 
def Recovery_Producer(meter, name):
 if name == 'HE':   
     if name in rec_prod_dict:
         recovery = rec_prod_dict[name][0]
         theoreticalG = rec_prod_dict[name][1]
         recoveredG = rec_prod_dict[name][2]
         shrink = rec_prod_dict[name][3]
         #print("     Recovery_Producer done {from dict}")             
         return recovery, theoreticalG, recoveredG, shrink
     else:
         recovery = Coeff.loc[meter]['PRODUCER_HE_PCT_RECOVERY']
         theoreticalG = Producer_Net_delivered_mcf(meter)*Coeff.loc[meter]['HE_MOL_PCT']/100.0
         recoveredG = theoreticalG*recovery
         shrink = 0  
         rec_prod_dict[name] = [recovery]
         rec_prod_dict[name].append(theoreticalG)
         rec_prod_dict[name].append(recoveredG)
         rec_prod_dict[name].append(shrink)
         #print("     Recovery_Producer done")             
         return recovery, theoreticalG, recoveredG, shrink
                       
 elif Coeff.loc[meter]['RECOVERIES'] == 'ACTUAL':
     #print("     Recovery_Producer done")
     return Recovery_DCP(meter,name)
 else:
     if name in rec_prod_dict:
         recovery = rec_prod_dict[name][0]
         theoreticalG = rec_prod_dict[name][1]
         recoveredG = rec_prod_dict[name][2]
         shrink = rec_prod_dict[name][3]
        # print("     Recovery_Producer done (from dict)")             
         return recovery, theoreticalG, recoveredG, shrink
     else:
         recovery = Coeff.loc[meter]['PRODUCER_'+name+'_PCT_RECOVERY']
         theoreticalG = Coeff.loc[meter][str(name)+'_GPM']*Producer_Net_delivered_mcf(meter)
         recoveredG = theoreticalG*recovery
         shrink = recoveredG*Static[Static['PRODUCT'] == name]['SHRINK_FACTOR'].values
         rec_prod_dict[name] = [recovery]
         rec_prod_dict[name].append(theoreticalG)
         rec_prod_dict[name].append(recoveredG)
         rec_prod_dict[name].append(shrink)
         #print("     Recovery_Producer done")
         return recovery, theoreticalG, recoveredG, shrink   
 
def Producer_Recovery_Total(meter):
 fractions = ['C2', 'C3', 'IC4', 'NC4', 'C5', 'C6']

 Producer_recovered_theoreticalG =[]
 Producer_recoveredG = []
 Producer_shrink =[]
 

 for frac in fractions:
     recovery,theoreticalG, recoveredG, shrink = Recovery_Producer(meter, frac)
     
     
     Producer_recovered_theoreticalG.append(theoreticalG)
     Producer_recoveredG.append(recoveredG)
     Producer_shrink.append(shrink)
 
     #print (frac, 'recovery %', recovery*100, '  Theoretical G', theoreticalG, '   Recovered G', recoveredG)
 Producer_total_net_theoretical_G = sum(Producer_recovered_theoreticalG)
 Producer_total_net_recovered_G = sum(Producer_recoveredG)
 Producer_total_shrink = sum(Producer_shrink)
 
  
 return Producer_recoveredG, Producer_recovered_theoreticalG, Producer_total_net_theoretical_G, Producer_total_net_recovered_G, Producer_total_shrink


# In[45]:

#
#Producer_Recovery_Total(66)


# In[46]:


def Producer_Settled_G(meter):
    Producer_recovered_G = Producer_Recovery_Total(meter)[0]
#     Producer_total_net_recovered_G = sum(Producer_Recovery_Total(meter)[1])
    #print (Producer_recovered_G)
    Producer_NGL_POP = Coeff.loc[meter]['NGL_POP']
    #print (Producer_NGL_POP)
    Producer_settled_gallons = [Producer_recovered_G[i]*Producer_NGL_POP for i in range(len(Producer_recovered_G))]
    Producer_Settled_gallons_total = sum(Producer_settled_gallons)
    return Producer_NGL_POP,Producer_settled_gallons, Producer_Settled_gallons_total


# In[47]:

#
#Producer_Settled_G(66)


# In[48]:


def Producer_NGL_market(meter):
    producer_fractions_NGL = ['C2', 'C3', 'IC4', 'NC4', 'C5']
    Producer_market_index = []
    price = []
    f_p_price = []
    tf = []
    for frac in producer_fractions_NGL:
        index = Coeff.loc[meter]['PROD_'+frac+'_MARKET']
       
        #print (index)
        Producer_market_index.append(index)
        #print (Producer_market_index)
        if index!= 0.0:
            p = Price[Price['INDEX'] == index]['PRICE'].values[0]
            #print (p)
            price.append(p)
            tf=(Coeff.loc[meter]['PRODUCER_'+str(frac)+'_TF'])
            fraction_producer_price = p + tf
            f_p_price.append(fraction_producer_price)
            ##print (frac, 'Price $', p,'    T&F', tf, '   NTP', fraction_producer_price )
        else:
            p = 0.0
            price.append(p)
            tf=(Coeff.loc[meter]['PRODUCER_'+str(frac)+'_TF'])
           
            fraction_producer_price = p + tf
            f_p_price.append(fraction_producer_price)
   
    
    return (f_p_price)


# In[49]:


#Coeff.loc[0]['PROD_'+'C2'+'_MARKET']
#Producer_NGL_market(82)
#Coeff.loc[1166]['PRODUCER_'+str(frac)+'_TF'])


# In[50]:


def Producer_NGL_Revenue(meter):
    
    producer_NGL_net_recoveredG = list(Producer_Recovery_Total(meter)[0])
    producer_fractions_NGL = ['C2', 'C3', 'IC4', 'NC4', 'C5']
    
    C56 = producer_NGL_net_recoveredG[4]+producer_NGL_net_recoveredG[5]
    producer_NGL_net_recoveredG[4] = C56
    
    
    fraction_producer_price = Producer_NGL_market(meter)
    #print (fraction_producer_price)
    Producer_NGL_revenue_by_fraction = []
    Producer_total_settled_NGL_G = []
    NGL_POP = Coeff.loc[meter]['NGL_POP']
           
    for i in range(len(producer_fractions_NGL)):
        Producer_settled_NGL_G_by_fraction = producer_NGL_net_recoveredG[i]*NGL_POP
        Producer_total_settled_NGL_G.append(Producer_settled_NGL_G_by_fraction)
        Producer_NGL_revenue_by_fraction.append(fraction_producer_price[i]*Producer_settled_NGL_G_by_fraction)
    
    Producer_NGL_revenue = sum(Producer_NGL_revenue_by_fraction)

    return Producer_NGL_revenue, Producer_total_settled_NGL_G, sum(Producer_total_settled_NGL_G), fraction_producer_price, Producer_NGL_revenue_by_fraction, NGL_POP


# In[51]:


#Producer_NGL_Revenue(66)


# In[52]:


def Producer_Condensate_Revenue(meter):
    Producer_recoveredG = Producer_Recovery_Total(meter)[0]
    if Coeff.loc[meter]['RECOVERIES'] == 'FIXED':
        condensateG = Producer_recoveredG[4]+Producer_recoveredG[5]
        Producer_condensateG = condensateG*Coeff.loc[meter]['PRODUCER_CONDENSATE_PCT_RECOVERY']#.values[0]
        #print ('condensate G',condensateG)
        price = Coeff.loc[meter]['PRODUCER_COND_PRICE']#reference_price[reference_price['Index'] == 'NYMEX - Crude']['Price'].values[0]
        Producer_produced_condensate = Producer_condensateG/42.0
        Settled_producer_condensate = Producer_produced_condensate*Coeff.loc[meter]['COND_POP']
        #print(price)*print ('Producer condensate',Revenue_Producer_condensate)
        Revenue_Producer_condensate = price*Settled_producer_condensate
        
    else:
        route,b1,b2,b3,plant,gs,a = Asset_from_Routing(meter)
        condensateG = Producer_recoveredG[4]+Producer_recoveredG[5]
        #print (condensateG)
        Producer_condensateG = condensateG*Plant[Plant['PLANT'] == plant]['CONDENSATE_PCT_RECOVERY'].values[0]
        price = Coeff.loc[meter]['PRODUCER_COND_PRICE']#reference_price[reference_price['Index'] == 'NYMEX - Crude']['Price'].values[0]
        Producer_produced_condensate = Producer_condensateG/42.0
        Settled_producer_condensate = Producer_produced_condensate*Coeff.loc[meter]['COND_POP']
        Revenue_Producer_condensate = Settled_producer_condensate*price
    
    return Revenue_Producer_condensate, Producer_produced_condensate, Settled_producer_condensate, price, Coeff.loc[meter]['COND_POP']


# In[53]:

#
#Producer_Condensate_Revenue(67)


# In[54]:


def Producer_res_index(meter):
    Prod_RES_index = []
    price = []
    Prod_RES_Index_pct = []
    for ind in range(3):
        index = Coeff.loc[meter]['PROD_RES_INDEX_'+str(ind+1)]
        #print(index)
        if index != 0.0 and len(index)>1:
            
            Prod_RES_index.append(index)
#             print()
#             print ('index',index)
            
           
            price.append(Price[Price['INDEX']==index]['PRICE'].values[0])
            #.values[0])
            
    
            pct = Coeff.loc[meter]['PROD_RES_INDEX_'+str(ind+1)+'_PCT']
       

            Prod_RES_Index_pct.append(pct)
          
            
    Prod_RES_deduct = Coeff.loc[meter]['PROD_RES_INDEX_DEDUCT']
    
    return (Prod_RES_index, price, Prod_RES_deduct, Prod_RES_Index_pct)


# In[55]:


#Producer_res_index(82)
#Coeff.loc[0]['TIPS_METER']


# In[56]:


# def Prod_Plant_Fuel(meter):
#     plant = Asset_from_Routing(meter)[4]
#     if Coeff.loc[meter]['PLANT_FUEL'] == 'ACTUAL':
        
#         return Producer_Net_delivered_mmbtu(meter)*Plant[Plant['PLANT']==plant]['PLANT_FUEL_PCT_MMBTU'].values[0]
        
#     else:
#         return Producer_Net_delivered_mmbtu(meter)*Coeff.loc[meter]['PRODUCER_PLANT_FUEL_PCT']

def Prod_Plant_Fuel(meter):
    plant = Asset_from_Routing(meter)[4]
    if Coeff.loc[meter]['PLANT_FUEL'] == 'ACTUAL':
       
        return Plant[Plant['PLANT']==plant]['PLANT_FUEL_PCT_MMBTU'].values[0]
        
    else:
      
        return Coeff.loc[meter]['PRODUCER_PLANT_FUEL_PCT']
  


# In[57]:


#Prod_Plant_Fuel(67)


# In[58]:


def Producer_RES_Revenue(meter):
    Shrink = Producer_Recovery_Total(meter)[4]
    plant_fuel = Prod_Plant_Fuel(meter)
    Plant_fuel_producer_mmbtu = Producer_Net_delivered_mmbtu(meter)* plant_fuel

    #print ('RES POP % ', Coeff.loc[meter]['RES_POP']*100)
    
    Producer_RES_AFS = (Producer_Net_delivered_mmbtu(meter)- Shrink -Plant_fuel_producer_mmbtu)
#     print ('Net_deklivered',Producer_Net_delivered_mmbtu(meter))
#     print ('srink', Shrink)
#     print ('Plant fuel', Plant_fuel_producer_mmbtu)
    Settled_RES_AFS = Producer_RES_AFS*Coeff.loc[meter]['RES_POP']
   
    
    INDEX, PRICE, DEDUCT, PCT = Producer_res_index(meter)
    
    
    total_Price = []

    for count in range(len(INDEX)):
      #  print (count)
       #print ('Index',count+1, ' $',PRICE[count]*100)
       #print ('Index ',count+1, ' %',PCT[count]*100)
       #print ('DEDUCT ',DEDUCT)
        total_Price.append(PRICE[count]*PCT[count])
    #print ('Index1, Index2 $ ', total_Price)
    
    
    #print ('Overall pct', Coeff.loc[meter]['Prod_Res_Overall_pct']*100)
    #print ('Overall deduct $', Coeff.loc[meter]['Prod_Res_Overall_deduct'])
    if coeff.loc[meter]['PROD_RES_OVERALL_PCT'] != 0.0:
        Producer_RES_PRICE  = (np.mean(total_Price)+DEDUCT)*Coeff.loc[meter]['PROD_RES_OVERALL_PCT'] + Coeff.loc[meter]['PROD_RES_OVERALL_DEDUCT']
    else:
        Producer_RES_PRICE  = np.mean(total_Price)+DEDUCT + Coeff.loc[meter]['PROD_RES_OVERALL_DEDUCT']
    
    #print ('Producer RES Price $',Producer_RES_PRICE)
    Revenue_Producer_RES = Producer_RES_PRICE*Settled_RES_AFS
#     print ('Revenue_Producer_RES',Revenue_Producer_RES)

    return Revenue_Producer_RES, Shrink, plant_fuel, Producer_RES_AFS,Settled_RES_AFS,Producer_RES_PRICE


# In[59]:


#Producer_RES_Revenue(67)


# In[60]:


def Producer_He_Revenue(meter):
    Producer_He_recovery, Producer_TheoreticalG, Producer_He_produced, shrink = Recovery_Producer(meter,'HE')
    #print ('He_recoveredG',Producer_He_produced)
    He_POP = Coeff.loc[meter]['HE_POP']
    Producer_He_price = Coeff.loc[meter]['HE_PRODUCER_USD']
    Producer_He_settled = Producer_He_produced*He_POP
    Producer_He_Revenue = Producer_He_settled*Producer_He_price


#     print ('Producer He Revenue ',Producer_He_Revenue)
#     print ()
    return Producer_He_Revenue,Producer_He_produced, Producer_He_settled, Producer_He_price, He_POP


# In[61]:


#Producer_He_Revenue(66)


# In[62]:


def Producer_Revenue(meter):
    producer_revenue = Producer_NGL_Revenue(meter)[0]+Producer_RES_Revenue(meter)[0] + Producer_He_Revenue(meter)[0] + Producer_Condensate_Revenue(meter)[0]
    return producer_revenue


# In[63]:


#Producer_Revenue(66)


# ## Margin

# In[64]:


def Margin(meter):
    margin = DCP_Revenue(meter)-Producer_Revenue(meter)
    return [margin[0]]
    


# In[65]:


Margin(82)


# In[66]:


# Asset_from_Routing(175)
int(Coeff.loc[175]['ROUTE_NUMBER'])
# Converting_to_float(reference_routing)
# reference_routing[reference_routing['ROUTE_NUMBER'] == 53331]#['BOOSTER_NAME'].values[0]
reference_routing[reference_routing['ROUTE_NUMBER'] == 5331]['BOOSTER_NAME'].values

# len(reference_routing)
# reference_routing.ROUTE_NUMBER.values


# In[67]:


def Variables(meter):
    variables =  [total_MCF(meter), total_MMBTU(meter),Fuel(meter), LU(meter),Net_delivered_mmbtu(meter),Net_delivered_mcf(meter),
                 DCP_NGL_Revenue(meter), DCP_NTP(meter), #1, 6recoveries, 6theoretical, 1 sum, 6 recovered, 1sum; 5prices, 5tf     
                 DCP_RES_Revenue(meter),DCP_RES_Price(meter),
                 DCP_Fees(meter), 
                 DCP_Condensate_Revenue(meter),DCP_He_Revenue(meter),Uplift_DCP(meter), DCP_Revenue(meter),
                      Fuel_producer(meter),LU_producer(meter), Producer_Net_delivered_mmbtu(meter),Producer_Net_delivered_mcf(meter),
                  Recovery_Producer(meter, 'C2')[0], Recovery_Producer(meter, 'C3')[0], Recovery_Producer(meter, 'IC4')[0], Recovery_Producer(meter, 'NC4')[0], Recovery_Producer(meter, 'C5')[0], Recovery_Producer(meter, 'C6')[0], Recovery_Producer(meter, 'HE')[0],
    Producer_NGL_Revenue(meter),Producer_Recovery_Total(meter)[0:4],Producer_RES_Revenue(meter),Coeff.loc[meter]['RES_POP'],Producer_Condensate_Revenue(meter), Producer_He_Revenue(meter), Producer_Revenue(meter), Margin(meter)] 
    
    massive = []
    new_element = []

    for element in variables:

        new_element.append(element)
    
    
    massive = []
    for element in new_element:
        if isinstance(element, tuple) or isinstance(element,list):

            for i in range(len(element)):
                if isinstance(element[i], list) or isinstance(element[i],tuple):
                    for k in range(len(element[i])):
                        massive.append(element[i][k])
                else:
                    massive.append(element[i])
        else:
            massive.append(element)
     
    list_variables = np.array(massive)
  
    return list_variables


# In[68]:


len(Variables(1))


# In[69]:


header_DCP = ['TIPS_METER','MCF', 'MMBTU', 'DCP_Field_Fuel','DCP_LU', 'ND_mmbtu', 'ND_mcf', 
          'DCP_NGL_Revenue', 
            'Recovery_pct_C2', 'Recovery_pct_C3','Recovery_pct_iC4','Recovery_pct_nC4','Recovery_pct_C5','Recovery_pct_C6',
       
          'DCP_Theoretical_C2','DCP_Theoretical_C3','DCP_Theoretical_iC4','DCP_Theoretical_nC4','DCP_Theoretical_C5','DCP_Theoretical_C6',
          'DCP_total_Theoretical_G',
          'DCP_Recovered_Gallons_C2', 'DCP_Recovered_Gallons_C3', 'DCP_Recovered_Gallons_iC4', 'DCP_Recovered_Gallons_nC4', 'DCP_Recovered_Gallons_C5', 'DCP_Recovered_Gallons_C6',
          'DCP_total_recovered_G', 
          'NGL_NTP_C2', 'NGL_NTP_C3', 'NGL_NTP_iC4', 'NGL_NTP_nC4', 'NGL_NTP_C5',
            'NGL_TF_C2','NGL_TF_C3','NGL_TF_iC4','NGL_TF_nC4','NGL_TF_C5',
              
              
          'DCP_RES_Revenue', 'DCP_Shrink', 'DCP_plant_fuel','DCP_RES_AFS', 'DCP_RES_Price',
          'Fees', 
          'DCP_Condensate_Revenue', 'DCP_Produced_Condensate', 'DCP_Condensate_Price',
          'DCP_He_Revenue', 'DCP_He_Production', 'DCP_He_Price',
          'DCP_Uplift',
          'DCP_Revenue']

header_producer = ['Producer_Field_Fuel','Producer_LU','Producer_ND_mmbtu', 'Producer_ND_mcf', 
             'Producer_Recovery_C2','Producer_Recovery_C3','Producer_Recovery_iC4','Producer_Recovery_nC4','Producer_Recovery_C5','Producer_Recovery_C6','Producer_Recovery_He',
              'Producer_NGL_Revenue',
            'Producer_Settled_C2','Producer_Settled_C3','Producer_Settled_iC4','Producer_Settled_nC4','Producer_Settled_C5',
          'Producer_Settled_G', 
                   'Producer_NGL_NTP_C2', 'Producer_NGL_NTP_C3', 'Producer_NGL_NTP_iC4', 'Producer_NGL_NTP_nC4', 'Producer_NGL_NTP_C5',
           'Producer_NGL_Revenue_C2','Producer_NGL_Revenue_C3','Producer_NGL_Revenue_iC4','Producer_NGL_Revenue_nC4','Producer_NGL_Revenue_C56',        
            'NGL_POP',
             'Producer_Recovered_C2','Producer_Recovered_C3','Producer_Recovered_iC4','Producer_Recovered_nC4','Producer_Recovered_C5','Producer_Recovered_C6',      
            'Producer_Theoretical_C2','Producer_Theoretical_C3','Producer_Theoretical_iC4','Producer_Theoretical_nC4','Producer_Theoretical_C5','Producer_Theoretical_C6',
          'Producer_Theoretical_total','Producer_Recovered_total', 
                   
          
    
        
                   
                   'Producer_RES_Revenue','Producer_Shrink', 'Producer_plant_fuel','Producer_Res_AFS','Producer_Settled_allocated', 'Producer_RES_price','Producer_RES_POP',
                   
                    'Producer_Condensate_Revenue', 'Producer_Produced_Condensate', 'Producer_Settled_Condensate','Producer_Condensate_Price','Condensate_POP',
                   'Producer_He_Revenue', 'Producer_He_Production','Producer_Settled_He', 'Producer_He_Price', 'He_POP', 'Producer_Revenue', 'Margin']

header = header_DCP+header_producer
print (len(header))
header_error = ['error' for i in range(len(header))]


# In[70]:


# In[88]:


def Assign_date(date1):
    Coeff = coeff[coeff['DATE'] == date1].reset_index(drop = True)
    Price = reference_price[reference_price['DATE'] == date1].reset_index(drop = True)
    Plant = reference_plant[reference_plant['DATE'] == date1].reset_index(drop = True)
    Booster = reference_booster[reference_booster['DATE'] == date1].reset_index(drop = True)
    Route = reference_routing[reference_routing['DATE'] == date1].reset_index(drop = True)
    Opex = reference_opex[reference_opex['DATE'] == date1].reset_index(drop = True)
    Static = reference_static
    return Coeff, Price, Plant, Booster, Route, Opex, Static


# In[89]:


months = ['Aug2018', 'Sept2018', 'Oct2018', 'Nov2108', 'Dec2018', 'Jan2019']
dates = [ '2018-08-01 00:00:00', '2018-09-01 00:00:00', '2018-10-01 00:00:00', '2018-11-01 00:00:00', '2018-12-01 00:00:00', '2019-01-01 00:00:00']


# In[92]:


area = 'DJ Basin'
i=0
d = []
for date in dates:
    print (date)

    Coeff, Price, Plant, Booster, Route, Opex, Static = Assign_date(date)
    Coeff = Converting_to_float(Coeff)
    Booster = Converting_to_float(Booster)
    Price = Converting_to_float(Price)
    Route = Converting_to_float(Route)
    Opex = Converting_to_float(Opex)
    Static = Converting_to_float(Static)
    Plant = Converting_to_float(Plant)

    Coeff = Coeff[Coeff.AREA == area].reset_index(drop = True)
    meters_fall_out = []
    a = {}
    for meter in tqdm(range(len(Coeff.TIPS_METER))):
        try:
        
            rec_dcp_dict = {}
            rec_prod_dict={}
            tag = Coeff.loc[meter]['TIPS_METER']     
            b = Variables(meter)                             
            a[tag] = b

        except:
            meters_fall_out.append(Coeff.loc[meter]['TIPS_METER'])

    df_margin = pd.DataFrame(a).transpose()
    df_margin = df_margin.reset_index()

    df_margin.columns = header

    
    file_name = 'output/DJBasin/DJBasin-Margin-'+str(months[i])+'.csv'
    
    df_margin.to_csv(file_name)
  
    i= i+1
    
    

    
    
    

