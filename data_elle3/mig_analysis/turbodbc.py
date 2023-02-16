#!/usr/bin/env python
# coding: utf-8

# In[4]:


import time
import pandas as pd
from turbodbc import connect
from config import get_params


# In[6]:


connection_string = "Driver={};Server={};Port={};Database={};Uid={};Pwd={};".format("ODBC Driver 17 for SQL Server", get_params("mssql")["server"], get_params("mssql")["port"], get_params("mssql")["database"], get_params("mssql")["username"], get_params("mssql")["password"])
connection = connect(connection_string=connection_string)
cursor = connection.cursor()


# In[ ]:





# # Course Icodeon

# In[26]:


#cursor.execute("SELECT * FROM [ELLE3_LMSDB].[dbo].[ICN_COURSE] where COURSE_ID = '9daf225b-6d2c-4c89-a4b2-f1e8800855a6' Order by PK Desc")
cursor.execute("SELECT * FROM COURSE_ID FROM [ELLE3_LMSDB].[dbo].[ICN_COURSE]  Order by PK Desc")
table = cursor.fetchallarrow(adaptive_integers=True)
df_course = table.to_pandas()


# In[23]:


df_course#[df_course["COURSE_ID"] == '9daf225b-6d2c-4c89-a4b2-f1e8800855a6']


# # Scorm Organization

# In[5]:


cursor.execute("SELECT * FROM [ELLE3_LMSDB].[dbo].[ICN_ORGANIZATION]  Order by PK Desc")
table = cursor.fetchallarrow(adaptive_integers=True)
df_organization = table.to_pandas()


# In[6]:


df_organization#[df_organization["PK"] == 5141]


# # Scorm Activity

# In[7]:


cursor.execute("SELECT * FROM [ELLE3_LMSDB].[dbo].[ICN_ACTIVITY] Order by PK Desc")
table = cursor.fetchallarrow(adaptive_integers=True)
df_activity = table.to_pandas()


# In[8]:


df_activity[(df_activity["ACTIVITY_ID"] == "_02_-_Contenuti_-_Il_carattere_SCO") ]
df_activity[(df_activity["ORGANIZATION_PK"] == 5150) ]


# In[9]:


df_activity[df_activity["TITLE"].str.contains("malware", na=False)]


# In[ ]:





# In[ ]:





# # Learners

# In[10]:


cursor.execute("SELECT * FROM [ELLE3_LMSDB].[dbo].[ICN_LEARNER] Order by PK Desc")
table = cursor.fetchallarrow(adaptive_integers=True)
df_learners = table.to_pandas()


# In[11]:


df_learners[df_learners["LEARNER_ID"] == "7"]


# In[ ]:





# In[ ]:





# # ICN_COCD

# In[12]:


cursor.execute("SELECT * FROM [ELLE3_LMSDB].[dbo].[ICN_COCD] where LEARNER_PK = '216959'")
batches  = cursor.fetchallarrow(adaptive_integers=True) #.fetcharrowbatches(adaptive_integers=True)
#df_cocd = pd.DataFrame()
#for batch in batches:
#    df_cocd.append(batch.to_pandas())
df_cocd = batches.to_pandas()
df_cocd


# In[184]:


from bs4 import BeautifulSoup
text = df_cocd.iloc[0]["XML"]
soup = BeautifulSoup(text, 'html.parser')
soup#.suspenddata.text


# In[195]:


text = df_cocd[df_cocd["PK"] == 2143847]
text


# In[193]:


from isodate import parse_duration
str(parse_duration('PT00H10M05S'))


# In[25]:


import datetime
def convert_time(x):
    timestamp = datetime.datetime.timestamp(x)
    return int(timestamp)


# In[256]:


cursor.execute("SELECT col_cocd.ICN_COCD_PK, COURSE_ID as fileguid, col_cocd.INSERT_DATE, cocd.PK, cocd.ACTIVITY_PK, cocd.XML, cocd.CMI_SESSION_TIME, cocd.CMI_COMPLETION_STATUS, l.LEARNER_ID, \
                a.ACTIVITY_ID, a.TITLE as activity_title, o.ORG_ID, o.TITLE as org_title\
                FROM [ELLE3_LMSDB].[dbo].[ICN_COCD] cocd " + \
                "JOIN [ELLE3_LMSDB].[dbo].[COL_COCD] col_cocd on col_cocd.ICN_COCD_PK = cocd.PK " + \
                "JOIN [ELLE3_LMSDB].[dbo].[ICN_LEARNER] l ON cocd.LEARNER_PK = l.PK " + \
                "JOIN [ELLE3_LMSDB].[dbo].[ICN_ACTIVITY] a ON a.PK = cocd.ACTIVITY_PK " + \
                "JOIN [ELLE3_LMSDB].[dbo].[ICN_ORGANIZATION] o ON a.ORGANIZATION_PK = o.PK " + \
                "JOIN [ELLE3_LMSDB].[dbo].[ICN_COURSE] c ON o.COURSE_PK = c.PK " + \
                "where LEARNER_ID in ('7', '5029', '54337', '551', '552') and a.SCORM_TYPE = 'sco' ")
table = cursor.fetchallarrow()
result = table.to_pandas()
result["time"] = result["INSERT_DATE"].apply(lambda x: convert_time(x))
"""
c.COURSE_ID in ('34d03648-2243-498a-afd5-c8997ac4f251',\
 'e93f2ecc-1b97-43bd-a402-0688d31febfc',\
 '443dff2d-22af-4fb8-b4f7-c72f4ba3378f',\
 'bcdc6391-0faa-4c27-a6aa-2ea75b5640de',\
 'd60909e7-f216-4958-8b6f-fb41725d4de7',\
 'de7326f8-8ad9-4241-95de-712485266723',\
 '513c95d9-ba9c-4a7e-bfa8-7c8a2b5d1c84',\
 'b1d70d48-b572-4e69-b3f5-7a301195b652',\
 '9daf225b-6d2c-4c89-a4b2-f1e8800855a6') and
 """


# In[257]:


result


# # Get Scorm tracks only for course 1775

# In[71]:


cursor.execute("SELECT col_cocd.ICN_COCD_PK, cocd.VERSION, COURSE_ID as fileguid, col_cocd.INSERT_DATE, cocd.PK, cocd.ACTIVITY_PK, cocd.XML,\
cocd.CMI_SESSION_TIME, cocd.CMI_COMPLETION_STATUS, l.LEARNER_ID, \
                a.ACTIVITY_ID, a.TITLE as activity_title, o.ORG_ID, o.TITLE as org_title\
                FROM [ELLE3_LMSDB].[dbo].[ICN_COCD] cocd " + \
                "JOIN [ELLE3_LMSDB].[dbo].[COL_COCD] col_cocd on col_cocd.ICN_COCD_PK = cocd.PK " + \
                "JOIN [ELLE3_LMSDB].[dbo].[ICN_LEARNER] l ON cocd.LEARNER_PK = l.PK " + \
                "JOIN [ELLE3_LMSDB].[dbo].[ICN_ACTIVITY] a ON a.PK = cocd.ACTIVITY_PK " + \
                "JOIN [ELLE3_LMSDB].[dbo].[ICN_ORGANIZATION] o ON a.ORGANIZATION_PK = o.PK " + \
                "JOIN [ELLE3_LMSDB].[dbo].[ICN_COURSE] c ON o.COURSE_PK = c.PK " + \
                "where  a.SCORM_TYPE = 'sco' ")
table = cursor.fetchallarrow()
result = table.to_pandas()


# In[72]:


result["time"] = result["INSERT_DATE"].apply(lambda x: convert_time(x))
result['time2'] = pd.to_datetime(result['INSERT_DATE'])
result['dates'] = result['time2'].dt.date
result['hour'] = result['time2'].dt.hour
result.sort_values(by="INSERT_DATE")


# In[73]:


result2 = result.sort_values(by="INSERT_DATE").drop_duplicates(subset=["ICN_COCD_PK","ACTIVITY_ID", "dates"], keep="last")
result2[result["LEARNER_ID"] == '54337']


# In[79]:


result[result["LEARNER_ID"] == '54337'].groupby(["ACTIVITY_ID", "ICN_COCD_PK", "dates", "hour"]).count()


# In[75]:


result[(result["LEARNER_ID"] == '54337') & (result["ACTIVITY_ID"]=="_02_-_Sicurezza_Info_-_L'attacco_informatico_SCO")]


# In[ ]:





# In[77]:


result2.to_csv('../resources/usage/scorm_tracks_pat.csv', index=False)


# In[ ]:





# In[259]:


result["ACTIVITY_ID"].value_counts()


# In[266]:





# # COL_COCD

# In[40]:


#cursor.execute("SELECT * FROM [ELLE3_LMSDB].[dbo].[COL_COCD] \
#            where ACTIVITY_PK = 11539 and LEARNER_PK = 216959 \
#           Order by PK Desc")
cursor.execute("SELECT  * FROM [ELLE3_LMSDB].[dbo].[COL_COCD] \
            where ACTIVITY_PK = 11539 and LEARNER_PK = 216959 and CMI_LESSON_STATUS ='completed' \
               UNION \
           SELECT  * FROM [ELLE3_LMSDB].[dbo].[COL_COCD] \
           where ACTIVITY_PK = 11539 and LEARNER_PK = 216959 and CMI_LESSON_STATUS ='incomplete' \
           Order by PK Desc")
table = cursor.fetchallarrow(adaptive_integers=True)
df_col_cocd = table.to_pandas()


# In[41]:


df_col_cocd#[df_col_cocd["SESSION_ID"] == "elle3_47fe6a53-f2e4-422d-ae97-ef86fdefa0c9"]


# ## ICN_COCD all

# In[47]:


#PK, ICN_COCD_PK, LEARNER_PK, ACTIVITY_PK, INSERT_DATE, CMI_LESSON_STATUS 
cursor.execute("SELECT TOP(5) * \
            FROM [ELLE3_LMSDB].[dbo].[ICN_COCD] ")
table = cursor.fetchallarrow(adaptive_integers=True)
df_col_cocd = table.to_pandas()


# In[49]:


df_col_cocd


# In[48]:


df_col_cocd[df_col_cocd["LEARNER_PK"] == 216959]


# In[44]:


#df_col_cocd.to_csv('../resources/usage/scorm_tracks.csv', index=False)


# In[123]:


cursor.execute("SELECT Id, IdPerson,CreatedOn,DateZone,IdAction,WorkingSessionID,IdFile,IdCommunity,IdRepositoryItemType,Transferred,IdLink \
               FROM [ELLE3].[dbo].[FR_FilePlayInfo] \
               where CreatedOn > '2021-12-01 09:00:05.000'")
table_scorm_evaluate  = cursor.fetchallarrow(adaptive_integers=True)
df_scorm_evaluate = table_scorm_evaluate .to_pandas()
df_scorm_evaluate[df_scorm_evaluate["IdCommunity"] == 1775]#.groupby(["IdPerson", "IdFile"]).count()


# In[146]:


df_scorm_evaluate[df_scorm_evaluate["IdPerson"] == 54337]


# In[118]:


cursor.execute("SELECT [Id]\
      ,[SCST_FILE_Id] \
      ,[SCST_FLST_Id] \
      ,[SCST_Compl] \
      ,[SCST_Info] \
      ,[SCST_Path] \
      ,[SCST_FileName] \
      ,[SCST_LastModify] \
      ,[SCST_Decompress] \
      ,[SCST_TotActivity] \
               FROM [ELLE3].[dbo].[SCORM_FileStato] ")
table_scorm_file_stato  = cursor.fetchallarrow(adaptive_integers=True)
df_scorm_file_stato = table_scorm_file_stato.to_pandas()
df_scorm_file_stato


# TEST DATA
# - IdPerson: Admin - 54337
#  standard ISO 8601 time duration value - PT0000H01M16.28S
#  

# ## Technical References
# - https://turbodbc.readthedocs.io/en/latest/pages/advanced_usage.html#advanced-usage-options-read-buffer
