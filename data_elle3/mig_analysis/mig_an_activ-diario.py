#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import re, datetime
pd.set_option("display.max_columns", None)
from bs4 import BeautifulSoup
from turbodbc import connect
from config import get_params, get_connection


# # Available Services

# In[14]:


connection = get_connection()
df_comunita = pd.read_sql("Select * from [ELLE3].[dbo].[COMUNITA] ", connection)
df_servizi = pd.read_sql("Select * from [ELLE3].[dbo].[SERVIZIO] ", connection)
df_servizio_comunita = pd.read_sql("Select * from [ELLE3].[dbo].[SERVIZIO_COMUNITA] sc\
                                   join [ELLE3].[dbo].[SERVIZIO] s on s.SRVZ_ID = sc.SRVC_SRVZ_ID \
                                   where s.SRVZ_codice  = 'SRVLEZ' and sc.SRVC_isAbilitato = 1", connection)
df_servizio_comunita
df_available_srv = df_servizio_comunita \
                    .merge(df_servizi, left_on="SRVC_SRVZ_ID", right_on="SRVZ_id") \
                    .merge(df_comunita, left_on="SRVC_CMNT_ID", right_on="CMNT_id")


# In[20]:


df_available_srv[df_available_srv["SRVC_CMNT_ID"] == 2843]#.groupby(["SRVZ_nome", "SRVC_CMNT_ID"]).count()


# In[3]:


df_evento = pd.read_sql("Select * from [ELLE3].[dbo].[EVENTO] ", connection)
sample = df_evento[df_evento["EVNT_CMNT_id"] == 1775]
sample


# In[4]:


df_programa_evento = pd.read_sql("Select * from [ELLE3].[dbo].[PROGRAMMA_EVENTO] ", connection)
sample_progr = df_programa_evento[df_programa_evento["PREV_ORRI_id"].isin(sample["EVNT_id"])]
sample_progr


# In[5]:


df_orario = pd.read_sql("Select * from [ELLE3].[dbo].[ORARIO] ", connection)
df_orario[df_orario["ORRI_EVNT_id"].isin(sample["EVNT_id"])]


# In[11]:


df_available_srv[df_available_srv["SRVC_CMNT_ID"] ==2834]


# In[6]:


df_diario = df_evento\
            .merge(df_orario, left_on="EVNT_id", right_on="ORRI_EVNT_id")\
            .merge(df_programa_evento, left_on="ORRI_id", right_on="PREV_ORRI_id")  
df_diario = df_diario[df_diario["EVNT_CMNT_id"].isin(df_available_srv["SRVC_CMNT_ID"])]


# In[21]:


df_available_srv[df_available_srv["SRVC_CMNT_ID"] == 2843]


# In[9]:


df_diario = df_diario[["EVNT_CMNT_id", "EVNT_id", "PREV_ProgrammaSvolto", "ORRI_inizio", "ORRI_fine"]]
df_diario.sort_values(by="EVNT_CMNT_id")


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[75]:


def convert_time(x):
    #date = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
    timestamp = datetime.datetime.timestamp(x)
    return int(timestamp)
# Extract only the first tag as section name from the description
def transformer(row):
    soup = BeautifulSoup(row, 'html.parser')
    section = soup.get_text()[:100]+ "..."
    for el in soup.find_all('strong'): 
        if el.text != None and el.text.strip() != "" and ~pd.isna(el.text):
            section = el.text   
            break
    return section.encode().decode('utf-8').strip()
df_diario["start"] = df_diario["ORRI_inizio"].apply(lambda x: convert_time(x))
df_diario["duration"] = (pd.to_datetime(df_diario["ORRI_fine"]) - pd.to_datetime(df_diario["ORRI_inizio"])).astype("timedelta64[s]")


# In[76]:


df_diario = df_diario.sort_values(by=["ORRI_inizio", "EVNT_id"])
df_diario["PREV_ProgrammaSvolto"] = df_diario["PREV_ProgrammaSvolto"].str.replace('"', "'")
df_diario["PREV_ProgrammaSvolto"] = df_diario["PREV_ProgrammaSvolto"].fillna(" ")
df_diario["Section_Name"] = df_diario["PREV_ProgrammaSvolto"].apply(lambda x: transformer(x)[0:100]).fillna("...")
df_diario


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[77]:


df_diario[df_diario["EVNT_CMNT_id"] == 2818] #1734


# In[78]:


df_diario.to_csv("../resources/courses/diario.csv", index=False)


# In[ ]:





# In[60]:


df_event_item_file = pd.read_sql("Select \
      [Id] \
      ,[IdFile] \
      ,[IdEvent] \
      ,[IdEventItem] \
      ,[IdCommunity] \
      ,[IdLink] \
      ,[_isVisible] \
      ,[_Deleted] \
      ,[_OwnedBy] \
      ,[_CreatedOn] \
      ,[_CreatedBy] \
      ,[_ModifiedOn] \
      ,[_ModifiedBy] \
      ,[_CreatedProxyIPaddress] \
      ,[_CreatedIPaddress] \
      ,[_ModifiedIPaddress] \
      ,[_ModifiedProxyIPaddress] from [ELLE3].[dbo].[CD_EventItemFile] ", connection)


# In[60]:





# In[65]:


df_event_item_file[df_event_item_file["IdCommunity"] == 3033]


# In[64]:


df_event_item_file.to_csv("../resources/courses/diario_material.csv", index=False)


# In[ ]:





# In[ ]:





# In[3]:


instance = "local"
df_user_course_enrolments = pd.read_csv("../resources/users/enrolments_percorso_{}.csv".format(instance),
                                            chunksize=10)
df_user_course_enrolments2 = pd.read_csv("../resources/users/enrolments_percorso_{}.csv".format(instance))
df_user_course_enrolments_all = pd.read_csv("../resources/users/enrolments_{}.csv".format(instance), chunksize=10)


# In[4]:


df_user_course_enrolments2["roleId"].value_counts()


# In[5]:


df_user_course_enrolments_all = pd.read_csv("../resources/users/enrolments_{}.csv".format(instance))
df_user_course_enrolments_all[(df_user_course_enrolments_all["idCourseMoodle"] == 6216) \
                              & (df_user_course_enrolments_all["TPRL_nome"] == "Formazione permanente")  \
                              & (df_user_course_enrolments_all["roleId"].isin(df_user_course_enrolments2[df_user_course_enrolments2["TPRL_nome"] == "Formazione permanente"]["roleId"]))]


# In[9]:


all = df_user_course_enrolments_all[(df_user_course_enrolments_all["TPRL_nome"] == "Formazione permanente") & (df_user_course_enrolments_all["idCourseMoodle"] == 6216)]
all.roleId.value_counts()


# In[10]:


pf = df_user_course_enrolments2[(df_user_course_enrolments2["idCourseMoodle"] == 6216) & (df_user_course_enrolments2["TPRL_nome"] == "Formazione permanente")]
pf.roleId.value_counts()


# In[ ]:


all[all[""]]


# In[ ]:





# In[ ]:





# In[135]:


temp = all.merge(pf, left_on=["roleId"], right_on=["roleId"], how="left", indicator=True)
temp2 = temp[temp["_merge"] == "left_only"]


# In[136]:


temp2[temp2["roleId"] == 9]


# In[ ]:




