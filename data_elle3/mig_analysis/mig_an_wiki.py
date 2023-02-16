#!/usr/bin/env python
# coding: utf-8

# In[77]:


import pandas as pd
import re, datetime
from config import get_connection
connection = get_connection()
pd.set_option("display.max_columns", None)
from bs4 import BeautifulSoup


# In[4]:


courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv".format())


# # WIKI Formazione

# In[32]:


query = "Select cast(WIKI_id as nvarchar(200)) as WIKI_id, WIKI_nome, WIKI_CMNT_id, DisplayAuthors from [ELLE3].[dbo].[WIKI]"
df_wiki = pd.read_sql(query, connection)
df_wiki["WIKI_nome"] = df_wiki["WIKI_nome"].str.strip()
df_wiki #= df_wiki.merge(courses.drop_duplicates(subset=["IdCommunity"]), left_on="WIKI_CMNT_id", right_on="IdCommunity", how="left")


# In[33]:


df_wiki[df_wiki["WIKI_CMNT_id"] == 120]


# In[34]:


df_wiki[df_wiki["WIKI_id"] == "BC393402-C84D-4BAE-B3DF-CFCC1AC4B79D"]


# In[35]:


df_wiki[df_wiki["WIKI_nome"].str.contains("Tre")]


# In[36]:


df_wiki.to_csv("../resources/wiki/wiki.csv", index=False)
df_wiki


# # Sezione

# In[62]:


query = "Select cast([WKSZ_id] as nvarchar(200)) as WKSZ_id \
      ,cast([WKSZ_WIKI_id] as nvarchar(200)) as WKSZ_WIKI_id  \
      ,[WKSZ_nome] \
      ,[WKSZ_dataInserimento] \
      ,[WKSZ_PRSN_id] \
      ,[WKSZ_isDeleted] \
      ,[WKSZ_isDefault] \
      ,[WKSZ_Descrizione] \
      ,[WKSZ_isPubblica] \
      ,[PlainDescription] from [ELLE3].[dbo].[WIKI_SEZIONE]"
df_wiki_sezione = pd.read_sql(query, connection)
df_wiki_sezione["WKSZ_isDeleted"].value_counts()


# In[63]:


df_wiki_sezione.info()


# In[64]:


df_wiki_sezione = df_wiki_sezione[df_wiki_sezione["WKSZ_isDeleted"] == 0]
df_wiki_sezione


# In[65]:


temp = df_wiki_sezione[df_wiki_sezione["WKSZ_WIKI_id"] == "BC393402-C84D-4BAE-B3DF-CFCC1AC4B79D"].sort_values(by="WKSZ_dataInserimento")
temp.info()#[temp["WKSZ_PRSN_id"] == 9320]


# In[66]:


temp.iloc[0, :]["WKSZ_PRSN_id"]


# In[67]:


df_users = pd.read_csv("../resources/users/users_{}.csv".format("local"))
sections = temp.merge(df_users, left_on="WKSZ_PRSN_id", right_on="PRSN_id")
sections


# In[68]:


df_users[df_users["PRSN_id"] == "9320"]


# In[69]:


sections_titles = "</br>".join(temp["WKSZ_nome"].apply(lambda x: "<h1>[[" + x + "]]</h2>").tolist())
sections_titles


# In[70]:


df_wiki_sezione["WKSZ_isDefault"].value_counts()


# In[71]:


def convert_time(x):
    #date = datetime.datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S.%f")
    timestamp = datetime.datetime.timestamp(x)
    return int(timestamp)
df_wiki_sezione = df_wiki_sezione[df_wiki_sezione["WKSZ_isDeleted"]==False]
df_wiki_sezione = df_wiki_sezione[["WKSZ_id", "WKSZ_WIKI_id", "WKSZ_nome", "WKSZ_dataInserimento", "WKSZ_PRSN_id","WKSZ_isDefault", "WKSZ_Descrizione"]]
df_wiki_sezione["WKSZ_dataInserimento"] = df_wiki_sezione["WKSZ_dataInserimento"].apply(lambda x: convert_time(x))
df_wiki_sezione["WKSZ_nome"] = df_wiki_sezione["WKSZ_nome"].str.strip()
df_wiki_sezione["WKSZ_nome"] = df_wiki_sezione["WKSZ_nome"].fillna(" ")
df_wiki_sezione["WKSZ_Descrizione"] = df_wiki_sezione["WKSZ_Descrizione"].str.strip()
df_wiki_sezione["WKSZ_Descrizione"] = df_wiki_sezione["WKSZ_Descrizione"].fillna(" ")


# In[72]:


df_wiki_sezione


# In[73]:


df_wiki_sezione.to_csv("../resources/wiki/wiki_sections.csv", index=False)


# In[74]:


df_wiki_sezione[df_wiki_sezione["WKSZ_WIKI_id"] == "bc393402-c84d-4bae-b3df-cfcc1ac4b79d"].sort_values(by="WKSZ_nome")


# In[ ]:





# # Topic

# In[78]:


query = "Select cast([WKTP_id] as nvarchar(200)) as WKTP_id \
      ,[WKTP_contenuto] \
      ,[WKTP_nome] \
      ,[WKTP_dataInserimento] \
      ,[WKTP_dataModifica] \
      ,[WKTP_PRSN_id] \
      ,[WKTP_isDeleted] \
      ,cast([WKTP_WKSZ_id] as nvarchar(200)) as WKTP_WKSZ_id \
      ,[WKTP_isPubblica] \
      ,[PlainContent] from [ELLE3].[dbo].[WIKI_TOPIC]"
df_wiki_topic = pd.read_sql(query, connection)
df_wiki_topic = df_wiki_topic[df_wiki_topic["WKTP_isDeleted"] == 0][["WKTP_id", "WKTP_contenuto", "WKTP_nome", "WKTP_dataInserimento", "WKTP_PRSN_id", "WKTP_WKSZ_id"]]
df_wiki_topic["WKTP_dataInserimento"] = df_wiki_topic["WKTP_dataInserimento"].apply(lambda x: convert_time(x))
df_wiki_topic["WKTP_contenuto"] = df_wiki_topic["WKTP_contenuto"].str.strip()
df_wiki_topic["WKTP_contenuto"] = df_wiki_topic["WKTP_contenuto"].fillna(" ")
df_wiki_topic["WKTP_nome"] = df_wiki_topic["WKTP_nome"].str.strip()
df_wiki_topic["WKTP_nome"] = df_wiki_topic["WKTP_nome"].fillna(" ")
df_wiki_topic


# In[79]:


df_wiki_topic.sort_values(by="WKTP_dataInserimento").to_csv("../resources/wiki/wiki_topics.csv", index=False)


# In[ ]:





# In[ ]:





# # Topic History

# In[83]:


query = "Select cast([WKTH_id] as nvarchar(200)) as WKTH_id \
      ,cast([WKTH_WKTP_id] as nvarchar (200)) as  WKTH_WKTP_id \
      ,[WKTH_contenuto] \
      ,[WKTH_nome] \
      ,[WKTH_dataModifica] \
      ,[WKTH_PRSN_id] \
      ,[WKTH_isDeleted] \
      ,[PlainContent] \
      ,[PlainContent] from [ELLE3].[dbo].[WIKI_TOPICHISTORY]"
df_wiki_topic_history = pd.read_sql(query, connection)
df_wiki_topic_history[df_wiki_topic_history["WKTH_WKTP_id"] == "bca3128d-ccc1-4c9e-ba2d-f4aac9771887"].sort_values(by="WKTH_dataModifica")


# In[84]:


df_wiki_topic_history = df_wiki_topic_history[df_wiki_topic_history["WKTH_isDeleted"] == 0]
df_wiki_topic_history["WKTH_dataModifica"] = df_wiki_topic_history["WKTH_dataModifica"].apply(lambda x: convert_time(x))
df_wiki_topic_history["WKTH_contenuto"] = df_wiki_topic_history["WKTH_contenuto"].str.strip()
df_wiki_topic_history["WKTH_contenuto"] = df_wiki_topic_history["WKTH_contenuto"].fillna(" ")
df_wiki_topic_history["WKTH_nome"] = df_wiki_topic_history["WKTH_nome"].str.strip()
df_wiki_topic_history["WKTH_nome"] = df_wiki_topic_history["WKTH_nome"].fillna(" ")
df_wiki_topic_history


# In[85]:


df_wiki_topic_history.groupby(["WKTH_WKTP_id"]).count()


# In[86]:


df_wiki_topic_history[df_wiki_topic_history["WKTH_WKTP_id"].isin(df_wiki_topic["WKTP_id"]) ].sort_values(by=["WKTH_dataModifica"])
# == "fb5223fe-a873-44d3-bd01-90bf3e741d5f"


# In[87]:


df_wiki_topic_history.to_csv("../resources/wiki/wiki_topic_hist.csv", index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




