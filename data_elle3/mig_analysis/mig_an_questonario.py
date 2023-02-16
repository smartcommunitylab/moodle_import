#!/usr/bin/env python
# coding: utf-8

# In[368]:


import pandas as pd
from config import get_connection
pd.set_option("display.max_columns", None)
connection = get_connection()


# # Service 'Questionario'

# ## Questionario Group - Community

# In[369]:


df_questionario_group = pd.read_sql("Select * from QS_QUESTIONARIO_GRUPPO", connection)


# In[380]:


df_questionario_group[df_questionario_group["QSGR_Id"] == 289]


# ## Questionario

# In[371]:


df_questionario = pd.read_sql("Select [QSTN_Id]\
      ,[QSTN_DataCreazione]\
      ,[QSTN_PRSN_Creator_Id]\
      ,[QSTN_QSGR_Id]\
      ,[QSTN_DataInizio]\
      ,[QSTN_DataFine]\
      ,[QSTN_Tipo]\
      ,[QSTN_PesoTotale]\
      ,[QSTN_ScalaValutazione]\
      ,[MinScore]\
      ,[MaxAttempts]\
      ,[LibraryAccessibility]\
      ,[DisplayScoreToUser]\
      ,[DisplayAttemptScoreToUser]\
      ,[DisplayAvailableAttempts]\
      ,[DisplayResultsStatus]\
      ,[DisplayCurrentAttempts]\
      ,[DisplayNotPassedScoreToUser]\
      ,[DisplayWrongAttemptAnswers]\
      ,[QSTN_IsChiuso]\
      ,[QSTN_Durata]\
      ,[QSTN_forUtentiComunita]\
      ,[QSTN_forUtentiPortale]\
      ,[QSTN_forUtentiEsterni]\
      ,[QSTN_forUtentiInvitati]\
      ,[QSTN_RisultatiAnonimi]\
      ,[QSTN_visualizzaRisposta]\
      ,[QSTN_visualizzaCorrezione]\
      ,[QSTN_editaRisposta]\
      ,[QSTN_TPGF_Id]\
      ,[QSTN_DataCancellazioneRisposte]\
      ,[QSTN_AutoreCancellazioneRisposte]\
      ,[QSTN_isRandomOrder]\
      ,[QSTN_DataModifica]\
      ,[QSTN_PRSN_Editor_Id]\
      ,[QSTN_nDomandeDiffBassa]\
      ,[QSTN_nDomandeDiffMedia]\
      ,[QSTN_nDomandeDiffAlta]\
      ,[QSTN_isRandomOrder_Options]\
      ,[QSTN_nQuestionsPerPage]\
      ,[QSTN_isPassword]\
      ,[QSTN_visualizzaSuggerimenti]\
      ,[QSTN_ownerType]\
      ,[QSTN_ownerId] from QS_QUESTIONARIO", connection)


# In[372]:


df_questionario["QSTN_Tipo"].value_counts()


# In[373]:


df_questionario["MaxAttempts"].value_counts()


# In[374]:


df_questionario[df_questionario["MaxAttempts"] ==5]


# In[375]:


df_questionario[df_questionario["QSTN_Tipo"]==1]


# In[376]:


df_questionario[df_questionario["QSTN_QSGR_Id"]==332] # 310


# As we can see from the aggregated data the most used questionnaire type is the Rate one (type 0)
#  - 0 -- Rate - Questionnaire(Survey)
#  - 1 -- Question Banks (Librerie)
#  - 5 --
#  - 7 -- Test di autovalutazione-  Random - Libreria 

# In[377]:


df_activities_percorso = pd.read_csv("../resources/courses/courses_activities.csv")
df_activities_percorso = df_activities_percorso[df_activities_percorso["IdModule"] == 30][["IdObjectLong"]]
df_quiz_to_import = df_questionario[df_questionario["QSTN_Id"].isin(df_activities_percorso["IdObjectLong"])]
df_quiz_to_import[df_quiz_to_import["QSTN_QSGR_Id"]== 75]                   


# In[309]:


df_questionario[df_questionario["QSTN_QSGR_Id"] == 205]


# In[378]:


df_questionario[df_questionario["QSTN_Tipo"] == 5]


# In[311]:


#df_quiz_to_import = df_quiz_to_import.append(df_questionario.sort_values(by=["QSTN_Id"]).groupby(["QSTN_QSGR_Id"]).tail(1)).drop_duplicates(subset=["QSTN_Id"])
df_quiz_to_import[df_quiz_to_import["QSTN_QSGR_Id"] == 75]


# In[312]:


import datetime
def convert_time(x):
    #date = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
    timestamp = datetime.datetime.timestamp(x)
    return int(timestamp)


# In[313]:


df_questionario_multilingua = pd.read_sql("Select * from QS_QUESTIONARIO_MULTILINGUA", connection)
df_questionario_multilingua


# ### Questionario - Comunità 

# In[314]:


df_comunita = pd.read_sql("Select CMNT_id, CMNT_idPadre, CMNT_nome, CMNT_TPCM_id, CMNT_ORGN_id from [ELLE3].[dbo].[COMUNITA]", connection)
#df_comunita = df_comunita[(df_comunita["CMNT_ORGN_id"] == 1) & (df_comunita["CMNT_TPCM_id"].isin([0, 14]))]
df_comunita


# In[315]:


df_questionario_cmnt = df_questionario_group \
                        .merge(df_questionario, left_on="QSGR_Id", right_on="QSTN_QSGR_Id") \
                        .merge(df_questionario_multilingua, left_on="QSTN_Id", right_on="QSML_QSTN_Id") \
                        .merge(df_comunita, left_on="QSGR_CMNT_Id", right_on="CMNT_id")
df_questionario_cmnt[df_questionario_cmnt["QSTN_Tipo"]==4][["QSTN_Id", "QSML_Nome", "QSTN_Tipo", "CMNT_id", "CMNT_nome", "CMNT_TPCM_id", "CMNT_ORGN_id"]]


# In[316]:


df_questionario_cmnt["QSTN_Tipo"].value_counts()


# In[ ]:





# # Enrich Questionnaires data with related data

# In[317]:


# .merge(df_quiz_to_import, left_on="QSGR_Id", right_on="QSTN_QSGR_Id") \
df_questionario_multilingua_all = df_questionario_group\
                                    .merge(df_questionario, left_on="QSGR_Id", right_on="QSTN_QSGR_Id")\
                                    .merge(df_questionario_multilingua, left_on="QSTN_Id", right_on="QSML_QSTN_Id")
df_questionario_multilingua_all = df_questionario_multilingua_all[["QSGR_CMNT_Id", "QSTN_Id", "QSTN_QSGR_Id", "QSML_Id", "QSML_Nome", \
                                                                   "QSML_Descrizione", "QSML_IdLingua", "QSTN_nQuestionsPerPage", "QSTN_Tipo", \
                                                             "MaxAttempts", "QSTN_ScalaValutazione", "QSTN_DataInizio", "QSTN_DataFine"]]
#df_questionario_multilingua_all = df_questionario_multilingua_all[df_questionario_multilingua_all["QSGR_CMNT_Id"] == 1775]
df_questionario_multilingua_all["QSTN_DataInizio"] = df_questionario_multilingua_all["QSTN_DataInizio"].fillna(0)
df_questionario_multilingua_all["QSTN_DataFine"] = df_questionario_multilingua_all["QSTN_DataFine"].fillna(0)
df_questionario_multilingua_all["opendate"] = df_questionario_multilingua_all["QSTN_DataInizio"].apply(lambda x: convert_time(x) if x!= 0 else 0)
df_questionario_multilingua_all["closedate"] = df_questionario_multilingua_all["QSTN_DataFine"].apply(lambda x: convert_time(x) if x!= 0 else 0)
df_questionario_multilingua_all


# In[318]:


df_questionario_multilingua_all["QSTN_Tipo"].value_counts()


# In[322]:


df_questionario_multilingua_all[(df_questionario_multilingua_all["QSGR_CMNT_Id"] == 1750) & (df_questionario_multilingua_all["QSTN_Tipo"] == 7)]#.to_dict()  #1795


# In[126]:


df_questionario_multilingua_all.groupby("QSGR_CMNT_Id").count().sort_values(by="QSTN_Id")


# In[ ]:





# # Random questionnaire - Libreria

# In[127]:


def convertion(x):
    val = x
    if x<0:
        val = 1
    return val


# In[128]:


df_quest_random = pd.read_sql("Select * from QS_LK_QUESTIONARIO_LIBRERIA", connection)
df_quest_random["LKQL_nDomandeDiffBassa"] = df_quest_random["LKQL_nDomandeDiffBassa"].fillna(0).apply(lambda x: convertion(x)).astype(int)
df_quest_random["LKQL_nDomandeDiffMedia"] = df_quest_random["LKQL_nDomandeDiffMedia"].fillna(0).apply(lambda x: convertion(x)).astype(int)
df_quest_random["LKQL_nDomandeDiffAlta"] = df_quest_random["LKQL_nDomandeDiffAlta"].fillna(0).apply(lambda x: convertion(x)).astype(int)
df_quest_random


# In[129]:


df_quest_random[df_quest_random["LKQL_QSTN_Id"] == 2161]


# In[130]:


df_quest_random[df_quest_random["LKQL_QSTN_Id"] == 3838  ]# 2159 # 3244


# In[131]:


df_quest_random["maxquestions"] = df_quest_random[["LKQL_nDomandeDiffBassa", "LKQL_nDomandeDiffMedia", "LKQL_nDomandeDiffAlta"]].sum(axis=1)
# .drop_duplicates(subset=["LKQL_QSTN_Id"], keep="last")
df_questionario_multilingua_all = df_questionario_multilingua_all.merge(df_quest_random[["LKQL_QSTN_Id", "LKQL_LIBRERIA_Id", "maxquestions"]], left_on="QSTN_Id", right_on="LKQL_QSTN_Id", how="left")
df_questionario_multilingua_all["israndom"] = df_questionario_multilingua_all["LKQL_QSTN_Id"].apply(lambda x: 0 if pd.isna(x) else 1)
df_questionario_multilingua_all["maxquestions"] = df_questionario_multilingua_all["maxquestions"].fillna(0).astype('int32')


# In[132]:


df_questionario_multilingua_all["QSML_Descrizione"] = df_questionario_multilingua_all["QSML_Descrizione"].fillna(" ")
df_questionario_multilingua_all["LKQL_LIBRERIA_Id"] = df_questionario_multilingua_all["LKQL_LIBRERIA_Id"].fillna(0)
df_questionario_multilingua_all


# In[133]:


df_questionario_multilingua_all[df_questionario_multilingua_all["QSGR_CMNT_Id"]==1795] #121


# In[134]:


df_questionario_multilingua_all["QSTN_Tipo"].value_counts()


# In[135]:


#df_comunita = pd.read_csv("../../comunita.csv", usecols=["CMNT_id", "CMNT_idPadre", "CMNT_nome", "CMNT_TPCM_id", "CMNT_ORGN_id"])
df_organizations = pd.read_sql("Select * from ORGANIZZAZIONE", connection)
quest_tipo1 = df_questionario_multilingua_all[df_questionario_multilingua_all["QSTN_Tipo"] == 1][["QSGR_CMNT_Id", "QSTN_Id", "QSML_Nome"]]
quest_res = df_comunita.merge(quest_tipo1, left_on="CMNT_id", right_on="QSGR_CMNT_Id").merge(df_organizations, left_on="CMNT_ORGN_id", right_on="ORGN_id")
quest_res = quest_res[["CMNT_id", "CMNT_nome", "QSTN_Id", "QSML_Nome", "ORGN_ragioneSociale"]]
quest_res = quest_res.rename(columns={"QSML_Nome":"Nome Questionario"})
quest_res


# In[136]:


df_questionario_multilingua_all[df_questionario_multilingua_all["QSGR_CMNT_Id"] == 1795]


# In[137]:


df_questionario_multilingua_all[df_questionario_multilingua_all["QSML_Descrizione"].str.len() > 9000]


# In[138]:


df_questionario_multilingua_all.loc[df_questionario_multilingua_all["QSTN_Id"] == 1923, ["QSML_Descrizione"]] = " "
df_questionario_multilingua_all.loc[df_questionario_multilingua_all["QSTN_Id"] == 1922, ["QSML_Descrizione"]] = " "
df_questionario_multilingua_all.loc[df_questionario_multilingua_all["QSTN_Id"] == 521, ["QSML_Descrizione"]] = " "
df_questionario_multilingua_all["IdMoodle"] = 0
df_questionario_multilingua_all[df_questionario_multilingua_all["QSML_Nome"].str.contains("albana") ]


# In[139]:


df_questionario_multilingua_all["QSML_Descrizione"] = df_questionario_multilingua_all["QSML_Descrizione"].astype("string")
df_questionario_multilingua_all["QSML_Descrizione"] = df_questionario_multilingua_all["QSML_Descrizione"].fillna(" ")
df_questionario_multilingua_all.info()


# ## Export librerie (question banks- type 1)

# In[140]:


df_questionario_multilingua_all[df_questionario_multilingua_all["QSTN_Tipo"] == 1].to_csv("../resources/questionnaire/df_librerie.csv", index=False)
len(df_questionario_multilingua_all[df_questionario_multilingua_all["QSTN_Tipo"] == 1])


# ## Export quiz / questionnaires (types 0, 7, 5)

# In[141]:


df_questionario_multilingua_all[df_questionario_multilingua_all["QSTN_Tipo"] != 1].to_csv("../resources/questionnaire/questionnaires.csv", index=False)
len(df_questionario_multilingua_all[df_questionario_multilingua_all["QSTN_Tipo"] != 1])
groups = df_questionario_multilingua_all[df_questionario_multilingua_all["QSTN_Id"] == 2159].groupby(["QSTN_Id"])
groups.get_group(2159)#.iloc[0]


# In[142]:


len(df_questionario_multilingua_all)


# In[ ]:





# In[ ]:





# In[143]:


import numpy as np
questionnaires = pd.read_csv("../resources/questionnaire/df_librerie.csv", converters={"QSML_Descrizione": str})
questionnaires[(questionnaires["QSGR_CMNT_Id"] == 1750)] # 1750 1795


# In[144]:


questionnaires[(questionnaires["QSGR_CMNT_Id"] == 1750)] #1795


# In[145]:


temp = pd.read_csv("../resources/questionnaire/questionnaires.csv")
temp["QSML_Descrizione"] = temp["QSML_Descrizione"].fillna(" ")
temp[(temp["QSGR_CMNT_Id"] == 1750) & (temp["QSTN_Tipo"] == 7)] # 1775  1795


# In[ ]:





# # Domande  - Import in Question Banks

# In[146]:


df_domande = pd.read_sql("Select * from QS_DOMANDA", connection)
df_domande


# In[147]:


df_domande["DMND_Tipo"].value_counts()


# ## Domande Tipo:
# - 3 -- Choice 
# - 1 -- Rate
# - 5 -- Text
# - 2 -- Number
# - 4 -- DropDown

# In[148]:


df_domande[df_domande["DMND_Id"] == 14035]


# # QS_LK_QUESTIONARIO_DOMANDA

# In[149]:


df_questionario_domande = pd.read_sql("Select * from QS_LK_QUESTIONARIO_DOMANDA", connection)


# In[150]:


df_questionario_domande


# ## Should we exclude the questions with LKQD_NumeroDomanda = 0?

# In[151]:


df_questionario_domande[(df_questionario_domande["LKQD_NumeroDomanda"] == 0 ) ] #3663


# In[152]:


df_domanda_multilingua = pd.read_sql("Select * from QS_DOMANDA_MULTILINGUA", connection)
df_domanda_multilingua


# In[153]:


df_domanda_multilingua[df_domanda_multilingua["DMML_DMND_Id"] == 14035]


# In[154]:


df_domanda_multilingua["DMML_IdLingua"].value_counts()


# ## Languages
# - 1 - Italian
# - 2 - English
# - 3 - German
# - 4 - French

# In[ ]:





# # Questions belonging only to quiz/questionnaires to be imported (last questionnaire)

# In[155]:


df_domande_all = df_questionario_multilingua_all \
                    .merge(df_questionario_domande, left_on="QSTN_Id", right_on="LKQD_QSTN_Id")\
                    .merge(df_domande, left_on="LKQD_DMND_Id", right_on="DMND_Id")\
                    .merge(df_domanda_multilingua, left_on="DMND_Id", right_on="DMML_DMND_Id")


# In[156]:


df_domande_all.info()


# In[157]:


df_domande_all = df_domande_all[["QSGR_CMNT_Id", "QSTN_Id", "QSTN_Tipo", "DMND_Id", "DMML_Id", "DMML_Testo", "LKQD_isObbligatorio", "DMND_Tipo", "DMML_IdLingua", "LKQD_NumeroDomanda","LKQD_Difficolta"]]
df_domande_all["DMML_Testo"] = df_domande_all["DMML_Testo"].str.replace('"', "'")


# In[158]:


df_domande_all["LKQD_isObbligatorio"] = df_domande_all["LKQD_isObbligatorio"].apply(lambda x: 'n' if x==0 else 'y')
df_domande_all = df_domande_all.drop_duplicates(["QSTN_Id", "DMND_Id","DMML_IdLingua"]).sort_values(by=["QSTN_Id", "LKQD_NumeroDomanda"])


# In[159]:


df_domande_all[df_domande_all["QSTN_Id"] == 2167]


# In[160]:


df_domande_all["QSTN_Tipo"].value_counts()


# ## Questions types of Questionnaire type 7 (TestAutovalutazione)

# In[161]:


df_domande_all[df_domande_all["QSTN_Tipo"] == 7]["DMND_Tipo"].value_counts()


# ## Questions types of Questionnaire type 0 (Quetionario di gradimento)

# In[162]:


df_domande_all[df_domande_all["QSTN_Tipo"] == 0]["DMND_Tipo"].value_counts()


# ## Try to find questionnaires having different question types

# In[163]:


domande_multi_choice = df_domande_all[(df_domande_all["QSTN_Tipo"] == 0) & (df_domande_all["DMND_Tipo"] == 3)]
domande_multi_choice


# In[164]:


df_domande_all["LKQD_isObbligatorio"].value_counts()


# In[165]:


df_domande_all.groupby(["QSGR_CMNT_Id", "QSTN_Id"]).count()


# In[166]:


import html
p = df_domande_all[df_domande_all["QSTN_Id"] == 3822 ] #374
p["DMML_Testo"] = p["DMML_Testo"].apply(lambda x: html.unescape(x))
p#.to_dict()


# In[167]:


p[p["DMND_Tipo"] == 5]


# In[168]:


#df_domande_all["DMML_Testo"] = df_domande_all["DMML_Testo"].apply(lambda x: html.unescape(x))
df_domande_all


# In[169]:


df_domande_all[df_domande_all["DMML_Id"] == 7771]


# In[170]:


df_domande_all[df_domande_all["QSTN_Id"] == 1641]


# In[171]:


df_domande_all.to_csv("../resources/questionnaire/domande.csv", index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# ## Domande Rating

# In[172]:


df_domanda_rating = pd.read_sql("Select * from QS_DOMANDA_RATING", connection)
df_domanda_rating_all = df_domanda_rating.merge(df_domande_all, left_on="DMRT_DMML_Id", right_on="DMML_Id")
df_domanda_rating_all["DMRT_MostraND"] = df_domanda_rating_all["DMRT_MostraND"].astype(int)
df_domanda_rating_all#[df_domanda_rating_all["QSGR_CMNT_Id"] == 1775].to_dict()


# In[173]:


df_domanda_rating_all["DMRT_MostraND"].value_counts()


# ## Rating Headers

# In[174]:


df_domanda_rating_intestazioni = pd.read_sql("Select * from QS_DOMANDA_RATING_INTESTAZIONI", connection)
df_domanda_rating_intestazioni["DMRI_Testo"] = df_domanda_rating_intestazioni["DMRI_Testo"].fillna(" ")
df_domanda_rating_intestazioni


# In[175]:


'","'.join(df_domanda_rating_intestazioni[df_domanda_rating_intestazioni["DMRI_DMRT_Id"] == 5396]["DMRI_Testo"].to_dict().values())


# In[176]:


df_rating_intestazioni = df_domanda_rating_all\
                         .merge(df_domanda_rating_intestazioni, left_on="DMRT_Id", right_on="DMRI_DMRT_Id")
df_rating_intestazioni = df_rating_intestazioni[["QSTN_Id", "DMRT_DMML_Id", "DMRT_Id", "DMRI_Indice", "DMRI_Testo", "DMML_IdLingua", "DMRT_MostraND"]].sort_values(by=["DMRT_DMML_Id", "DMRI_Indice"])
df_rating_intestazioni


# In[177]:


df_rating_intestazioni["DMML_IdLingua"].value_counts()


# In[178]:


df_rating_intestazioni[df_rating_intestazioni["DMRT_DMML_Id"]==7525]


# In[179]:


df_rating_intestazioni.to_csv("../resources/questionnaire/domande_rating_headers.csv", index=False)


# ## Rating Options

# In[180]:


df_domanda_rating_opzioni = pd.read_sql("Select * from QS_DOMANDA_RATING_OPZIONI", connection)
df_domanda_rating_opzioni["DMRO_TestoMin"] = df_domanda_rating_opzioni["DMRO_TestoMin"].fillna(" ")
df_domanda_rating_opzioni


# In[181]:


df_domanda_rating_opzioni[df_domanda_rating_opzioni["DMRO_Id"] == 25240]


# In[182]:


df_rating_opzioni = df_domanda_rating\
                         .merge(df_domanda_multilingua, left_on="DMRT_DMML_Id", right_on="DMML_Id")\
                         .merge(df_domanda_rating_opzioni, left_on="DMRT_Id", right_on="DMRO_DMRT_Id")
df_rating_opzioni = df_rating_opzioni[["DMRT_DMML_Id", "DMRT_Id", "DMRO_Id", "DMRO_NumeroOpzione", "DMRO_TestoMin"]].sort_values(by=["DMRT_DMML_Id", "DMRO_NumeroOpzione"])
df_rating_opzioni


# In[183]:


df_rating_opzioni[df_rating_opzioni["DMRO_Id"] == 25240]


# In[184]:


df_rating_opzioni[df_rating_opzioni["DMRT_DMML_Id"] == 16666]["DMRO_TestoMin"].to_dict().values()


# In[185]:


df_rating_opzioni.to_csv("../resources/questionnaire/domande_rating_options.csv", index=False)


# In[ ]:





# In[ ]:





# # QS_QUESTIONARIO_RANDOM_DESTINATARIO

# In[186]:


df_quest_random_destinatario = pd.read_sql("Select * from QS_QUESTIONARIO_RANDOM_DESTINATARIO", connection)
df_quest_random_destinatario


# In[187]:


df_quest_random_destinatario[df_quest_random_destinatario["QSRD_QSTN_Padre_Id"] == 3244]


# In[188]:


df_quest_random_destinatario[df_quest_random_destinatario["QSRD_PRSN_Destinatario_Id"] == 54337]


# In[ ]:





# In[ ]:





# # Quiz Pages

# In[189]:


df_pages = pd.read_sql("Select * from QS_QUESTIONARIO_PAGINA", connection)
df_pages


# In[190]:


df_pages[df_pages["QSPG_QSML_Id"] == 3529]


# In[191]:


df_pages = df_pages.merge(df_questionario_multilingua[["QSML_QSTN_Id", "QSML_Id"]], left_on="QSPG_QSML_Id", right_on="QSML_Id")
df_pages


# In[192]:


df_pages.to_csv("../resources/questionnaire/questionario_pages.csv", index=False)


# In[193]:


"@@".join(df_pages[df_pages["QSML_QSTN_Id"] == 2]["QSPG_NomePagina"].to_dict().values())


# # Multichoice Questions

# In[194]:


df_multichoice = pd.read_sql("Select * from QS_DOMANDA_MULTIPLA", connection)
df_multichoice


# In[195]:


df_multichoice["DMMT_NumeroMaxRisposte"].value_counts()


# In[196]:


df_multichoice[df_multichoice["DMMT_NumeroMaxRisposte"] == 2]


# In[197]:


def max_response(x):
    if x != 1:
        return 0
    else:
        return 1
df_multichoice_all = df_multichoice\
                         .merge(df_domande_all, left_on="DMMT_DMML_Id", right_on="DMML_Id")
df_multichoice_all =df_multichoice_all[["DMMT_DMML_Id", "DMMT_NumeroMaxRisposte", "DMND_Id", "DMND_Tipo", "DMML_Testo", "LKQD_isObbligatorio", "DMMT_Id", "QSTN_Id", "LKQD_NumeroDomanda"]]
df_multichoice_all["single"] = df_multichoice_all["DMMT_NumeroMaxRisposte"].apply(lambda x: max_response(x))
df_multichoice_all#[df_multichoice_all["DMMT_NumeroMaxRisposte"] == 2]


# In[198]:


df_multichoice_all["DMMT_NumeroMaxRisposte"].value_counts()


# In[199]:


temp = df_multichoice_all[df_multichoice_all["QSTN_Id"] == 1641]
temp


# In[200]:


df_multichoice_all[df_multichoice_all["DMMT_DMML_Id"] == 17684]


# In[201]:


df_multichoice_all[df_multichoice_all["DMMT_Id"] == 6455]


# In[202]:


df_multichoice_all.to_csv("../resources/questionnaire/domande_multichoice.csv", index=False)


# In[203]:


df_multichoice_all[df_multichoice_all["QSTN_Id"] == 2079]
df_multichoice_all[df_multichoice_all["DMMT_DMML_Id"] == 11438].to_dict("records")[0]["single"]


# ## Multichoice Options

# In[204]:


def options_percentage(answer, isaltro):
    ret = answer
    if '$$1.0' in answer:
        ret = ret.replace('$$1.0', ',,') + str(isaltro)
    if '$$0.0' in answer:
        ret = ret.replace('$$0.0', ',,') + str(isaltro)
    return ret


# In[205]:


df_multichoice_opzioni = pd.read_sql("Select [DMMO_id]\
      ,[DMMO_Testo]\
      ,[DMMO_NumeroOpzione]\
      ,[DMMO_LayoutTesto]\
      ,[DMMO_LayoutImmagine]\
      ,[DMMO_DMMT_Id]\
      ,[DMMO_Peso]\
      ,[DMMO_isCorretta]\
      ,[DMMO_isAltro]\
      ,[DMMO_Suggestion] from QS_DOMANDA_MULTIPLA_OPZIONI", connection)


# In[206]:


df_multichoice_opzioni = df_multichoice_opzioni.merge(df_multichoice_all, left_on="DMMO_DMMT_Id", right_on="DMMT_Id")
df_multichoice_opzioni = df_multichoice_opzioni[["DMMO_id", "DMMO_DMMT_Id", "DMMT_DMML_Id", "DMMO_Testo", "DMMO_NumeroOpzione", "DMMO_Peso", "DMMO_isCorretta", "DMMO_isAltro"]]
df_multichoice_opzioni = df_multichoice_opzioni.drop_duplicates(subset=["DMMO_id", "DMMT_DMML_Id"])
df_multichoice_opzioni["DMMO_Peso"] = df_multichoice_opzioni["DMMO_Peso"].apply(lambda x: x/100).astype(str)
df_multichoice_opzioni["answer"] = df_multichoice_opzioni[["DMMO_Testo", "DMMO_Peso"]].apply(lambda x: "$$".join(x), axis=1)
df_multichoice_opzioni["answer_questionnaire"] = df_multichoice_opzioni.apply(lambda x: options_percentage(x["answer"], x["DMMO_isAltro"]), axis=1)
df_multichoice_opzioni[df_multichoice_opzioni["DMMO_isCorretta"] == 1]#[df_multichoice_opzioni["DMMT_DMML_Id"] == 11443]


# In[207]:


df_multichoice_opzioni[df_multichoice_opzioni["DMMO_Testo"].str.contains("Gli addetti alla gestione delle emergenze")]


# In[208]:


df_multichoice_opzioni[df_multichoice_opzioni["DMMO_DMMT_Id"] == 6440]["answer"].to_dict().values()


# In[209]:


opts = []
for opt in df_multichoice_opzioni[df_multichoice_opzioni["DMMO_DMMT_Id"] == 6440]["answer"]:
    opts.append(opt)
print("##".join(opts))


# In[210]:


df_multichoice_opzioni[df_multichoice_opzioni["DMMO_id"] == 28042]


# In[211]:


df_multichoice_opzioni[df_multichoice_opzioni["answer"].str.contains("%%")]


# In[ ]:





# In[212]:


df_multichoice_opzioni.to_csv("../resources/questionnaire/domande_multichoice_opzioni.csv", index=False)


# In[213]:


prova = df_multichoice_opzioni[df_multichoice_opzioni["DMMO_id"]==10]
prova.to_dict('records')


# In[214]:


df_multichoice_opzioni[df_multichoice_opzioni["DMMT_DMML_Id"] == 7771]


# In[ ]:





# In[ ]:





# # DropDown Questions

# In[215]:


df_domanda_dropdown = pd.read_sql("Select * from QS_DOMANDA_DROPDOWN", connection)
df_domanda_dropdown_all =  df_domanda_dropdown.merge(df_domande_all, left_on="DMDR_DMML_Id", right_on="DMML_Id")       
df_domanda_dropdown_all


# In[216]:


df_dropdown = pd.read_sql("Select * from QS_DROPDOWN", connection)
df_dropdown_all = df_dropdown.merge(df_domanda_dropdown_all, left_on="DROP_DMML_Id", right_on="DMDR_DMML_Id")  
df_dropdown_all = df_dropdown_all[["QSTN_Id", "DROP_Id", "DROP_Nome", "DMML_Id", "DMND_Id", "DMML_Testo"]]
df_dropdown_all#[df_dropdown_all["DROP_Id"]== 168]


# ## DropDown Items

# In[217]:


df_dropdown_item = pd.read_sql("select * from QS_DROPDOWN_ITEM", connection)


# In[218]:


def options_code(row):
    return "%" + str(row["DRIT_Peso"]) + "%" + row["DRIT_Testo"]
def options_percentage(col):
    ret = "~".join(col)
    if '%100%' not in ret:
        ret = ret.replace('%0%', '')
    return ret


# In[219]:


df_dropdown_item[df_dropdown_item["DRIT_DROP_Id"] == 168]


# In[220]:


df_dropdown_item["options"] = df_dropdown_item.apply(lambda x: options_code(x), axis=1)
df_dropdown_item.groupby(["DRIT_DROP_Id"])["options"].apply(lambda x: options_percentage(x))


# In[221]:


df_dropdown_all = df_dropdown_all.set_index("DROP_Id")
df_dropdown_all["options"] = df_dropdown_item.groupby(["DRIT_DROP_Id"])["options"].apply(lambda x: options_percentage(x))
df_dropdown_all


# In[222]:


df_dropdown_item[df_dropdown_item["DRIT_DROP_Id"] == 168].groupby(["DRIT_DROP_Id"])["options"].apply(lambda x: options_percentage(x)).to_dict()


# In[223]:


df_dropdown_all = df_dropdown_all.reset_index()
df_dropdown_all.to_csv("../resources/questionnaire/domande_dropdown.csv", index=False)
df_dropdown_all


# In[ ]:





# In[ ]:





# # Short Answers

# In[224]:


df_shortanswer = pd.read_sql("Select * from QS_DOMANDA_TESTOLIBERO", connection)
df_shortanswer = df_shortanswer.merge(df_domande_all, left_on="DMTL_DMML_Id", right_on="DMML_Id")
df_shortanswer[df_shortanswer["DMTL_Id"] == 2952]


# # Numerical Questions

# In[381]:


df_numeric = pd.read_sql("Select * from QS_DOMANDA_NUMERICA", connection)
df_numeric = df_numeric.merge(df_domande_all, left_on="DMNM_DMML_Id", right_on="DMML_Id")
df_numeric#[df_numeric["DMML_Id"] == 7766].to_dict()


# In[382]:


df_numeric.to_csv("../resources/questionnaire/domande_number.csv", index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# # Risposte

# In[323]:


df_risposte_quest = pd.read_sql("Select * from QS_RISPOSTA_QUESTIONARIO", connection)


# In[324]:


df_risposte_quest = df_risposte_quest[(df_risposte_quest["RSQS_IndirizzoIPEnd"].notna()) ]


# In[328]:


df_risposte_quest[(df_risposte_quest["RSQS_QSTN_Id"] == 1946) & ( df_risposte_quest["RSQS_PRSN_Id"] == 41282)]#["RSQS_PRSN_Id"].value_counts() # 3503


# ## Understanding the multiple responses given by the same user for the same questionnaire

# In[329]:


counting = df_risposte_quest.groupby(["RSQS_QSTN_Id", "RSQS_PRSN_Id"]).count().reset_index()
counting[(counting["RSQS_Id"]>2) & (counting["RSQS_PRSN_Id"] != 2)].sort_values(by="RSQS_Id")


# In[330]:


df_risposte_quest[(df_risposte_quest["RSQS_QSTN_Id"] == 3646) & (df_risposte_quest["RSQS_PRSN_Id"] == 9151) ]


# # Keep only the last attempt responses

# In[331]:


df_risposte_quest = df_risposte_quest.sort_values(by="RSQS_DataInizio").drop_duplicates(subset=["RSQS_PRSN_Id", "RSQS_QSTN_Id"], keep="last")
df_risposte_quest


# ## Response of quiz

# In[334]:


df_risposte_quest[ (df_risposte_quest["RSQS_QSTN_Id"] == 1826)]   #3503 #126136 #(df_risposte_quest["RSQS_PRSN_Id"] == 54337) &


# ## Response of questionnaire

# In[336]:


temp = df_risposte_quest[(df_risposte_quest["RSQS_QSTN_Id"] == 1946)]
temp


# In[337]:


temp["RSQS_PRSN_Id"].value_counts()


# In[338]:


df_risposte_quest[(df_risposte_quest["RSQS_Id"] == 293392)]


# In[240]:


df_users_moodle = pd.read_csv("../resources/users/users_local.csv")
df_users_moodle


# In[241]:


df_users_moodle[df_users_moodle["PRSN_id"] == 54337]


# In[ ]:





# In[ ]:





# In[ ]:





# # Questionnaire - Questions

# In[339]:


df_quest_domanda = pd.read_sql("Select * from QS_LK_QUESTIONARIO_DOMANDA", connection)


# In[341]:


df_quest_domanda[df_quest_domanda["LKQD_QSTN_Id"] == 1946] #2079


# ## Questions of random quiz

# In[342]:


df_quest_domanda_random = df_quest_domanda.merge(df_risposte_quest, left_on="LKQD_QSRD_Id", right_on="RSQS_QSRD_Id")
df_quest_domanda_random


# In[343]:


df_quest_domanda_random = df_quest_domanda_random[["RSQS_QSTN_Id", "LKQD_DMND_Id", "LKQD_NumeroDomanda", "RSQS_PRSN_Id", "RSQS_DataInizio", "RSQS_DataFine","RSQS_Id", "RSQS_QSRD_Id"]]


# In[344]:


df_quest_domanda_random.info()


# In[345]:


df_quest_domanda_random = df_quest_domanda_random.merge(df_domande_all[["DMND_Id", "DMML_Id", "DMML_Testo"]], left_on="LKQD_DMND_Id", right_on="DMND_Id")
df_quest_domanda_random = df_quest_domanda_random.drop_duplicates(subset=["RSQS_QSTN_Id", "RSQS_PRSN_Id", "LKQD_NumeroDomanda"])


# In[346]:


def convert_time(x):
    if pd.isna(x):
        return 0
    #date = datetime.datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S.%f")
    timestamp = datetime.datetime.timestamp(x)
    return int(timestamp)


# In[347]:


df_quest_domanda_random["time_start"] = df_quest_domanda_random["RSQS_DataInizio"].apply(lambda x: convert_time(x))
df_quest_domanda_random["time_start"] = df_quest_domanda_random["time_start"].fillna(0)
df_quest_domanda_random["time_end"] = df_quest_domanda_random["RSQS_DataFine"].apply(lambda x: convert_time(x))
df_quest_domanda_random["time_end"] = df_quest_domanda_random["time_end"].fillna(0)


# In[ ]:





# In[355]:


temp = df_quest_domanda_random[(df_quest_domanda_random["RSQS_PRSN_Id"] == 53611) & (df_quest_domanda_random["RSQS_QSTN_Id"] == 1946)]
temp.sort_values(by="LKQD_NumeroDomanda")


# In[251]:


df_quest_domanda_random.to_csv("../resources/questionnaire/risposte_domande_random_quiz.csv", index=False)


# In[252]:


df_quest_domanda_random


# In[253]:


df_quest_domanda_random[df_quest_domanda_random["RSQS_QSTN_Id"] == 2142 ] #2142


# In[ ]:





# # Questions of non random questionnaires

# In[254]:


df_domande_non_random = df_quest_domanda[df_quest_domanda["LKQD_QSRD_Id"].isna()]
df_domande_non_random[df_domande_non_random["LKQD_QSTN_Id"] == 3641]  # 2080


# # Non random quiz never received responses from users

# In[255]:


# Questionnaire type - 1 -- Static questions
static_quest = df_questionario_multilingua_all[df_questionario_multilingua_all["QSTN_Tipo"] == 1]


# In[256]:


df_risposte_quest[df_risposte_quest["RSQS_QSTN_Id"].isin(static_quest["QSTN_Id"])]


# In[ ]:





# # Responses of surveys - Questionnaire type 0

# In[257]:


surveys_quest = df_questionario_multilingua_all[df_questionario_multilingua_all["QSTN_Tipo"] == 0]
surveys_quest


# In[258]:


df_risposte_quest = pd.read_sql("Select * from QS_RISPOSTA_QUESTIONARIO", connection)
df_risposte_quest = df_risposte_quest[(df_risposte_quest["RSQS_IndirizzoIPEnd"].notna()) ]


# In[259]:


df_risposte_quest[df_risposte_quest["RSQS_QSTN_Id"] == 1641] # 3503


# In[260]:


df_risposte_domande_non_random = df_domande_non_random.merge(df_risposte_quest, left_on="LKQD_QSTN_Id", right_on="RSQS_QSTN_Id")
df_risposte_domande_non_random


# In[261]:


df_risposte_domande_non_random[df_risposte_domande_non_random["RSQS_QSTN_Id"] == 1835 ] # 2079  #2080


# In[262]:


df_domande_all[df_domande_all["DMND_Id"] == 15447]


# In[263]:


df_risposte_domande_non_random = df_risposte_domande_non_random.merge(df_domande_all[["DMND_Id", "DMML_Id", "DMML_Testo"]], left_on="LKQD_DMND_Id", right_on="DMND_Id")


# In[264]:


df_risposte_domande_non_random = df_risposte_domande_non_random[["RSQS_QSTN_Id", "DMND_Id", "DMML_Id", "DMML_Testo", "RSQS_PRSN_Id", "RSQS_DataInizio", "RSQS_DataFine", "LKQD_NumeroDomanda", "RSQS_Id"]]


# In[265]:


df_risposte_domande_non_random[df_risposte_domande_non_random["RSQS_QSTN_Id"] == 3285 ] 


# In[266]:


df_risposte_domande_non_random


# In[267]:


df_risposte_domande_non_random[df_risposte_domande_non_random[["RSQS_QSTN_Id",  "RSQS_PRSN_Id", "LKQD_NumeroDomanda"]].duplicated()].sort_values(by="RSQS_DataFine")


# In[268]:


#df_risposte_domande_non_random = df_risposte_domande_non_random.drop_duplicates(subset=["RSQS_QSTN_Id", "RSQS_PRSN_Id", "LKQD_NumeroDomanda"])
df_risposte_domande_non_random["time_start"] = df_risposte_domande_non_random["RSQS_DataFine"].apply(lambda x: convert_time(x))
df_risposte_domande_non_random[(df_risposte_domande_non_random["RSQS_QSTN_Id"] == 2132) ] #& (df_risposte_domande_non_random["RSQS_PRSN_Id"] == 16848)


# In[359]:


df_risposte_domande_non_random[(df_risposte_domande_non_random["RSQS_QSTN_Id"] == 2049)]


# In[270]:


df_risposte_domande_non_random.to_csv("../resources/questionnaire/risposte_domande_non_random.csv", index=False)


# In[271]:


prova = df_risposte_domande_non_random[df_risposte_domande_non_random["RSQS_QSTN_Id"] == 1641] #3503
prova


# In[ ]:





# In[ ]:





# # Risposte Multichoice

# In[358]:


df_risposte_multichoice = pd.read_sql("Select * from QS_RISPOSTA_OPZIONE_MULTIPLA", connection)
df_risposte_multichoice


# In[361]:


t = df_risposte_multichoice[df_risposte_multichoice["RSOM_RSQS_Id"] == 45992]
t


# In[274]:


df_risposte_multichoice[df_risposte_multichoice["RSOM_TestoIsAltro"].notna()]


# In[275]:


df_risposte_multichoice_all = df_risposte_multichoice.merge(df_multichoice_opzioni, left_on="RSOM_DMMO_Id", right_on="DMMO_id")
df_risposte_multichoice_all


# In[276]:


df_risposte_multichoice_all = df_risposte_multichoice_all[["RSOM_DMMO_Id", "RSOM_RSQS_Id", "RSOM_TestoIsAltro", "DMMT_DMML_Id", "DMMO_Testo", "DMMO_NumeroOpzione"]]


# In[277]:


t = df_risposte_multichoice_all[df_risposte_multichoice_all["RSOM_RSQS_Id"].isin(prova["RSQS_Id"])]
t[t["DMMO_Testo"].str.contains("Italiana")]


# In[278]:


t[t["RSOM_TestoIsAltro"].notna()]


# In[ ]:





# In[279]:


df_risposte_multichoice_all.to_csv("../resources/questionnaire/risposte_multichoice_opzioni.csv", index=False)


# In[ ]:





# In[ ]:





# # Risposte Rating 

# In[280]:


df_risposte_rating = pd.read_sql("Select * from QS_RISPOSTA_RATING", connection)
df_risposte_rating


# In[281]:


df_risposte_rating[df_risposte_rating["RSRT_TestoIsAltro"].notna()]


# In[282]:


df_risposte_rating[df_risposte_rating["RSRT_RSQS_Id"] ==  251443]


# In[283]:


df_risposte_rating_all = df_risposte_rating.merge(df_rating_opzioni, left_on="RSRT_DMRO_Id", right_on="DMRO_Id")
df_risposte_rating_all


# In[284]:


df_risposte_rating_all["RSRT_Valore"] = df_risposte_rating_all["RSRT_Valore"].astype(str)
df_risposte_rating_all["option"] = df_risposte_rating_all[["DMRO_TestoMin", "RSRT_Valore"]].apply(lambda x: "@@@".join(x), axis=1) 


# In[285]:


df_risposte_rating_all["RSRT_Valore"].value_counts()


# In[286]:


temp = df_risposte_rating_all[df_risposte_rating_all["RSRT_RSQS_Id"] == 293392]
temp


# In[287]:


temp["RSRT_Valore"].value_counts()


# In[288]:


prova = pd.DataFrame(temp.groupby("DMRT_DMML_Id")["option"].apply(';;;'.join))
prova.reset_index().to_dict()


# In[ ]:





# In[289]:


#df_risposte_rating[~df_risposte_rating["RSRT_DMRO_Id"].isin(df_rating_opzioni["DMRO_Id"])]


# In[290]:


df_risposte_rating_all.to_csv("../resources/questionnaire/risposte_rating_opzioni.csv", index=False)


# In[ ]:





# In[ ]:





# # Risposte Short Answer

# In[291]:


df_risposte_shortanswer = pd.read_sql("Select * from QS_RISPOSTA_TESTOLIBERO", connection)
df_risposte_shortanswer


# In[292]:


df_risposte_shortanswer_all = df_risposte_shortanswer.merge(df_shortanswer, left_on="RSTL_DMTL_Id", right_on="DMTL_Id")
df_risposte_shortanswer_all = df_risposte_shortanswer_all[["RSTL_DMTL_Id", "RSTL_RSQS_Id", "RSTL_Testo", "DMML_Id", "QSTN_Id"]]
df_risposte_shortanswer_all


# In[293]:


df_risposte_shortanswer_all[df_risposte_shortanswer_all["DMML_Id"] == 15511]


# In[294]:


df_risposte_shortanswer_all.to_csv("../resources/questionnaire/risposte_short_answer.csv", index=False)


# In[ ]:





# # Risposte Number

# In[295]:


df_risposte_number = pd.read_sql("Select * from QS_RISPOSTA_NUMERICA", connection)
df_risposte_number


# In[392]:


df_numeric_all = df_risposte_number.merge(df_numeric, left_on="RSNM_DMNM_Id", right_on="DMNM_Id")
df_numeric_all = df_numeric_all[["RSNM_DMNM_Id", "RSNM_RSQS_Id", "RSNM_Numero", "DMML_Id", "QSTN_Id"]]
df_numeric_all 


# In[393]:


df_numeric_all.to_csv("../resources/questionnaire/risposte_numeric.csv", index=False)


# In[ ]:




