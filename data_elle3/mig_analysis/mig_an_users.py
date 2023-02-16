#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymysql, re
from config import get_connection
connection = get_connection()
import pandas as pd
pd.set_option("display.max_columns", None)
instance="local"


# # Users

# In[12]:


query = "Select PRSN_id, PRSN_login, \
            PRSN_nome, \
            PRSN_cognome, \
            PRSN_codFiscale , \
            PRSN_mail, \
            PRSN_LNGU_id, \
            PRSN_citta , \
            PRSN_TPPR_id, \
            PRSN_telefono1, \
            PRSN_cellulare , IdDefaultProvider, PRSN_AUTN_ID, \
                    PRSN_ultimoCollegamento, PRSN_AUTN_RemoteUniqueID  from [ELLE3].[dbo].[PERSONA]"
df_users = pd.read_sql(query, connection)
#df_users= pd.read_csv("/var/www/html/moodlebackup/upload/user_roles/PERSONA.csv")
df_users = df_users.astype({"PRSN_login": "string", "PRSN_nome": "string", "PRSN_cognome": "string", "PRSN_codFiscale": "string", "PRSN_mail": "string"
                           , "PRSN_citta": "string", "PRSN_telefono1": "string", "PRSN_cellulare": "string"})
df_users#.info()


# In[13]:


df_users[(df_users["PRSN_mail"] == "alessandra.ianes@comune.trento.it")]


# In[14]:


user_id = df_users.loc[df_users["PRSN_id"]==9430, :].to_dict('records')[0]["PRSN_login"]
user_id


# In[15]:


regex_cf = "^[A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{3}[A-Z]$"
pd.Series(["clplbn88l63z100j"]).str.upper().str.match(regex_cf)[0]
#pd.Series(["clplbn88l63z100j"]).str.upper().str.findall(regex_cf)[0][0]


# In[16]:


def check_cf(cf):
    result = cf.head(1)
    print(result)
    if cf.str.upper().str.match(regex_cf)[0]:
        result = cf.str.upper().str.findall(regex_cf)[0][0]
    return result
t = check_cf(pd.Series(["clplbn88l63z100j"])) 
t


# In[17]:


regex = r'\b[A-Za-z0-9._%&\'+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
def check_email(email):
    return re.fullmatch(regex, email)


# In[18]:


df_users[df_users["PRSN_mail"].duplicated()]


# In[19]:


df_users[df_users["PRSN_login"].duplicated()]


# In[20]:


df_users[df_users["PRSN_login"] == "LVSGLR87R44L378Z"].sort_values(by="PRSN_ultimoCollegamento", ascending=False)


# In[21]:


df_users[df_users["PRSN_mail"] == "francesca.paissan@comune.trento.it"]


# In[22]:


df_users = df_users.rename(columns={'PRSN_login':'username', 
                                    'PRSN_nome':'firstname', 
                                    'PRSN_cognome':'lastname', 
                                    'PRSN_LNGU_id': "lang", 
                                    "PRSN_codFiscale":'codice_fiscale', 
                                    'PRSN_citta': 'city', 
                                    'PRSN_mail':'email', 
                                    'PRSN_telefono1':'phone1', 
                                    'PRSN_cellulare':'phone2'}).sort_values(by="PRSN_ultimoCollegamento", ascending=False)

df_users["username"].fillna(df_users["codice_fiscale"], inplace=True) # fill empty username with codice fiscale
df_users["username"] = df_users["username"].str.lower()
df_users["username"] = df_users["username"].str.strip()
df_users["email"] = df_users["email"].str.strip()
df_users["phone1"] = df_users["phone1"].fillna(" ")
df_users["phone2"] = df_users["phone2"].fillna(" ")
df_users["city"] = df_users["city"].fillna(" ")

df_users["checkemail"] = df_users["email"].apply(lambda x: check_email(x))
df_users = df_users[df_users["checkemail"].notna()]
df_users = df_users.drop(["checkemail"], axis=1)
df_users["moodleUserId"] = ""


# In[26]:


def get_first_from_group(col):
    """
    Custom function to get the first element from the group
    :param col:
    :return:
    """
    return col.tail(1)


# In[27]:


df_users[df_users["email"] == "francesca.paissan@comune.trento.it"].groupby("email").aggregate({"codice_fiscale": get_first_from_group})


# In[ ]:





# In[23]:


df_users_result = df_users.sort_values(by="PRSN_ultimoCollegamento", ascending=False).drop_duplicates(subset=["username"], keep="first")
df_users_result


# In[59]:


df_users_result[df_users_result["PRSN_id"] == 56792]


# In[56]:


df_users_result[df_users_result["username"] == "LVSGLR87R44L378Z".lower()]


# In[57]:


df_users_result[df_users_result["email"].str.contains("frizzera")]


# ## Users having duplicated usernames

# In[80]:


duplicated = df_users[~df_users["PRSN_id"].isin(df_users_result["PRSN_id"])]
duplicated


# In[97]:


df_users_result#[(df_users_result["firstname"] == "Lucia") & (df_users_result["lastname"] == "Lugoboni")] 


# In[ ]:





# In[81]:


df_users_result.to_csv("../resources/users/users.csv", index=False)
duplicated.to_csv("../resources/users/users_duplicated.csv", index=False)


# In[85]:


df_users_result


# In[87]:


res = pd.read_csv("../resources/users/users_local.csv")
res#[res["usernameMoodle"].isna()]


# In[177]:


res[res["username"] == "lvsglr87r44l378z"]


# In[ ]:





# ## Analize users created locally

# In[178]:


df_user_created = pd.read_csv("../resources/users/users_local.csv")
df_user_created


# In[179]:


duplicated.loc[duplicated["username"].isin(df_user_created["username"]), :]


# In[180]:


df_user_created.loc[df_user_created["username"].isin(duplicated["username"]), :]


# In[ ]:





# In[181]:


df_users[df_users["email"].duplicated()]


# In[ ]:





# # PERSONA - ORGANIZZAZIONE

# In[182]:


df_LK_PRSN_ORGN = pd.read_csv("../../user_roles/LK_PRSN_ORGN.csv") #55737
df_LK_PRSN_ORGN.groupby("LKPO_ORGN_id").count()


# In[183]:


df_LK_PRSN_ORGN[df_LK_PRSN_ORGN["LKPO_PRSN_id"] == 7]


# In[ ]:





# # User Enrolments

# In[22]:


query = "Select * from [ELLE3].[dbo].[LK_RUOLO_PERSONA_COMUNITA]"
df_iscrizioni = pd.read_sql(query, connection)
#df_iscrizioni = df_iscrizioni[df_iscrizioni["RLPC_PRSN_id"].isin(df_users['PRSN_id'])]
df_iscrizioni = df_iscrizioni[['RLPC_CMNT_id', 'RLPC_PRSN_id', 'RLPC_TPRL_id', 'RLPC_responsabile', 'RLPC_attivato', 'RLPC_abilitato']]
df_iscrizioni[(df_iscrizioni["RLPC_CMNT_id"] == 1775) & (df_iscrizioni["RLPC_PRSN_id"] == 7)]


# In[42]:


df_iscrizioni[(df_iscrizioni["RLPC_CMNT_id"] == 120) ]#["RLPC_TPRL_id"].value_counts()


# In[28]:


df_iscrizioni[(df_iscrizioni["RLPC_CMNT_id"].isin([2834])) & (df_iscrizioni["RLPC_TPRL_id"]==35)][["RLPC_PRSN_id"]].to_dict('list')


# In[29]:


df_iscrizioni[(df_iscrizioni["RLPC_CMNT_id"] == 1795) & (df_iscrizioni["RLPC_TPRL_id"] == 44)]


# In[30]:


df_iscrizioni.to_csv('../resources/users/LK_RUOLO_PERSONA_COMUNITA.csv', index=False)


# In[ ]:





# In[63]:


temp = pd.read_csv("../resources/users/percorso_role.csv")
t = temp[(temp["Id_Community"] == 1741)]
#t = temp[(temp["Id_Community"] == 1750) & (temp["IdPath"] == 237)]
t#[t["IdRoleCommunity"] == 44]


# In[64]:


both = t.merge(df_iscrizioni, left_on=["Id_Community", "IdRoleCommunity"], right_on=["RLPC_CMNT_id", "RLPC_TPRL_id"])
#both.drop_duplicates(subset=["Id_Community", "IdPath", "RLPC_TPRL_id"])
both.groupby(["Id_Community", "IdPath", "RLPC_TPRL_id"]).count()


# In[37]:


df_is = df_iscrizioni[(df_iscrizioni["RLPC_CMNT_id"]==2843)  & (df_iscrizioni["RLPC_TPRL_id"]==35)]
df_is


# In[40]:


df_user_moodle = pd.read_csv("../resources/users/users_{}.csv".format(instance))
df_user_iscriz = df_is.merge(df_user_moodle, left_on="RLPC_PRSN_id", right_on="PRSN_id")
len(df_is[ ~df_is["RLPC_PRSN_id"].isin(df_user_moodle["PRSN_id"])])


# In[202]:


df_iscrizioni_local = pd.read_csv('../resources/users/enrolments_local.csv')


# In[203]:


df_iscrizioni_local[df_iscrizioni_local["TPRL_nome"].str.contains('edizione')]


# # Skip the role 'passante'

# In[196]:


df_iscrizioni[(df_iscrizioni["RLPC_TPRL_id"]  == -3)]


# In[197]:


df_iscrizioni = df_iscrizioni[(df_iscrizioni["RLPC_TPRL_id"]  != -3) ]
df_iscrizioni


# In[ ]:





# In[206]:


groups = df_iscrizioni[df_iscrizioni["RLPC_TPRL_id"] >= 34]
groups[groups["RLPC_CMNT_id"] == 536]


# In[190]:


groups[groups.duplicated(["RLPC_CMNT_id", "RLPC_PRSN_id", "RLPC_TPRL_id"])]


# In[ ]:





# In[ ]:





# # Count users based on their Roles

# In[50]:


df_iscrizioni["RLPC_TPRL_id"].value_counts()


# In[51]:


# iscrizioni alle comunità di tipo Corso
df_courses_pat_moodle = pd.read_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance))
df_user_moodle = pd.read_csv("../resources/users/users_{}.csv".format(instance))
df_com_iscriz = df_courses_pat_moodle\
                .merge(df_iscrizioni, left_on="IdCommunity", right_on="RLPC_CMNT_id")\
                .merge(df_user_moodle, left_on="RLPC_PRSN_id", right_on="PRSN_id", how="left")
df_com_iscriz.groupby(['RLPC_CMNT_id']).count()
df_com_iscriz[df_com_iscriz["RLPC_CMNT_id"] == 118]


# In[8]:


df_user_moodle = pd.read_csv("../resources/users/users_{}.csv".format(instance))
df_com_iscriz_user = df_com_iscriz.merge(df_user_moodle, left_on="RLPC_PRSN_id", right_on="PRSN_id")
result_df = df_com_iscriz_user[["idCourseMoodle", "moodleUserId"]]
result_df


# In[9]:


#result_df.to_csv("../resources/users/enrolments_{}.csv".format(instance), index=False)


# In[198]:


generated = pd.read_csv("../resources/users/enrolments_{}.csv".format(instance))


# In[199]:


generated["roleId"].value_counts()


# # COMUNITA different from the parent

# In[26]:


df_LK_CMNT_CMNT = pd.read_csv("../../user_roles/LK_CMNT_CMNT.csv")
df_LK_CMNT_CMNT[df_LK_CMNT_CMNT["LKCC_FiglioID"] == 1765]


# In[ ]:





# # ALBERO

# In[48]:


df_albero = pd.read_csv("../../user_roles/ALBERO_COMUNITA.csv")
df_albero[df_albero["ALCM_CMNT_ID"]==1775]


# In[ ]:




