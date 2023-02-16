#!/usr/bin/env python
# coding: utf-8

# # Migration Data Analysis

# This document aims to explore the datasets extracted from Elle3 databases in order to map the concepts behind them into the proper ones corresponding to the Moodle e-learning platform.
# 

# ## 1.Comunità-online dataset

# In[150]:


import pandas as pd
import re
from bs4 import BeautifulSoup
from config import get_connection
pd.set_option("display.max_columns", None)
connection = get_connection()


# In[151]:


from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(days=1) + timedelta(hours=1)
print(yesterday)
datetime.timestamp(yesterday)


# In[154]:


query = "Select * from [ELLE3].[dbo].[COMUNITA] where CMNT_idPadre = 120"
df = pd.read_sql(query, connection)
df#[df["CMNT_id"] == 2866]
120
42	120
306	120
728	2865
732	2942
733	2944
734	2952
735	2989


# In[91]:


df_comunita = pd.read_sql("Select * from [ELLE3].[dbo].[COMUNITA] ", connection)
df_tipocomunita = pd.read_sql("Select * from [ELLE3].[dbo].[TIPO_COMUNITA] ", connection)
df_organizations = pd.read_sql("Select * from [ELLE3].[dbo].[ORGANIZZAZIONE] ", connection, coerce_float=False)
df_percorso_formativo = pd.read_sql_query("Select [Id] ,[Name],[Duration],[DisplayOrder],[Description],[MinCompletion],[isDefault]\
      ,[IdCommunity],[StartDate],[EndDate],[Status],[MinMark],[EPType],[Weight],[UpdateScorm],[EditingPolicy],[CompletionPolicy]\
      ,[DisplayPolicy],[ScormSettingsPolicy],[IsMooc],[_Deleted],[_CreatedOn],[_CreatedBy],[_ModifiedOn],[_ModifiedBy] from [ELLE3].[dbo].[EP_PATH] ", \
                                          connection)
df_diario = pd.read_csv("../resources/courses/diario.csv")
df_comunita.sort_values(by=["CMNT_dataCreazione"], ascending=True)


# ## 1.1 Enriching the dataset with relative data

# In[92]:


df_comunita_tmp = df_comunita.merge(df_tipocomunita, left_on="CMNT_TPCM_id", right_on="TPCM_id")
df_comunita = df_comunita_tmp[["CMNT_id", "CMNT_idPadre", "CMNT_nome", "CMNT_TPCM_id", "CMNT_ORGN_id", "TPCM_descrizione"]]


# In[93]:


df_comunita_tmp = df_comunita.merge(df_organizations, left_on="CMNT_ORGN_id", right_on="ORGN_id")
df_comunita = df_comunita_tmp[["CMNT_id", "CMNT_idPadre", "CMNT_nome", "CMNT_TPCM_id", "CMNT_ORGN_id", "TPCM_descrizione", "ORGN_ragioneSociale"]]


# In[94]:


#df_comunita = df_comunita[(df_comunita["TPCM_descrizione"].isin(["Corso", "Organizzazione"]))]
#& (df_comunita["TPCM_descrizione"] == "Corso") "Corso", "Organizzazione"
# (df_comunita["ORGN_ragioneSociale"] == "PAT-tsm | Formazione per la Pubblica Amministrazione") &


# In[95]:


df_comunita_tmp = df_comunita.merge(df_percorso_formativo, left_on="CMNT_id", right_on="IdCommunity", how="left")
df_comunita = df_comunita_tmp[["CMNT_id", "CMNT_idPadre", "CMNT_nome", "CMNT_TPCM_id", "CMNT_ORGN_id", "TPCM_descrizione", "ORGN_ragioneSociale", "Name", "Id", "Description"]]


# In[96]:


df_comunita


# In[97]:


df_comunita[df_comunita["CMNT_idPadre"] == 1765]#.groupby("CMNT_id").count()


# In[98]:


df_percorso_formativo[df_percorso_formativo["Id"] == 25]


# In[99]:


df_percorso_formativo["Status"].value_counts()


# In[103]:


df_comunita[df_comunita["CMNT_idPadre"] == 120] #.drop_duplicates(subset=["CMNT_id"])[["CMNT_id"]].to_dict('list')


# ## 1.2 Aggregate comunità based on their type and organization
# In order to decouple the enitites of Elle3 we need to have a clear view of the type of information they hold.

# In[104]:


aggregate = df_comunita.groupby(["ORGN_ragioneSociale", "TPCM_descrizione"])["CMNT_nome"].count()
agg = pd.DataFrame(aggregate)
agg.index.names = ["Organizzazione", "Tipo Comunità"]
agg.columns = ["Count"]
agg


# In[105]:


import numpy as np
import pandas as pd
from pandas import Series, DataFrame
import matplotlib.pyplot as plt

data1 = [23,85, 72, 43, 52]
data2 = [42, 35, 21, 16, 9]
width =0.3
plt.bar(np.arange(len(agg["Count"])), agg["Count"], width=width)
#plt.bar(np.arange(len(data2))+ width, data2, width=width)
plt.show()


# In[ ]:





# # 2. Categories
# ## 2.1 Categories derived from Comunità containing other courses
# Let's explore the list of communità of type 'Course' that contain other courses.  </br>
# In terms of Moodle terminology these are considered categories. </br>
# The query below gives an example of the list of courses inside 'Pat-tsm' organization to which refer other courses by means of the column 'CMNT_idPadre'

# In[106]:


categories = df_comunita.groupby("CMNT_idPadre")["CMNT_nome"].count()
df_categories = df_comunita[df_comunita["CMNT_id"].isin(categories.index)]


# In[107]:


categ_father = pd.DataFrame(categories).rename(columns=({"CMNT_nome": "Count"}))
categ_father


# In[142]:


df_categories[df_categories["CMNT_nome"].str.contains("Formazione P.I.Tre")]


# In[144]:


df_categories[df_categories["CMNT_id"]=="2866"]


# In total there are 32 comunità that are being refered by other courses as their parent category. </br>
# One of them is the root of comunità (of type 'Organization')

# ## 2.2 Categories derived from Comunità-Courses containing 'Percorsi Formativi'

# In[110]:


df_categories_formativo = df_comunita[df_comunita["Name"].notna()].drop_duplicates(subset=["CMNT_id"])


# In[111]:


df_categories_result  = df_categories.append(df_categories_formativo).drop_duplicates(subset=["CMNT_id"])
df_categories_result[df_categories_result["CMNT_idPadre"] == 0]


# ### Courses that serves as father for other courses but also contain percorsi formativi 

# In[112]:


fathers_containing_paths = pd.DataFrame(df_categories.groupby(["CMNT_id"]).count()["CMNT_idPadre"]>1)
fathers_containing_paths = fathers_containing_paths[fathers_containing_paths["CMNT_idPadre"]==True]
fathers_containing_paths


# We need to transform the dataset by removing reduntant columns and renaming them.</br>
# We also need to get the name of the parent category instead of its id, since it is relevant only in Elle3 context but not in Moodle.

# In[113]:


df_comunita.loc[df_comunita["CMNT_id"] == 120, ["CMNT_id"]]


# In[114]:


df_categories_result = df_comunita.drop_duplicates(subset=["CMNT_id"]).merge(df_categories_result, left_on="CMNT_id", right_on="CMNT_idPadre", how="right")
df_categories_result = df_categories_result[["CMNT_id_y", "CMNT_idPadre_y", "CMNT_nome_y", "CMNT_nome_x"]]
df_categories_result.rename(columns = {"CMNT_id_y": "idnumber", "CMNT_idPadre_y": "parent", "CMNT_nome_x": "parent_name", "CMNT_nome_y":"name"}, inplace=True)
df_categories_result = df_categories_result.fillna("0")


# In[115]:


df_categories_result


# We then export the resulting dataset into csv file, so that it can be elaborated inside moodle import procedures.

# In[174]:


df_categories_result[df_categories_result["parent"] == 120]


# In[117]:


df_categories_result = df_categories_result.sort_values(by="idnumber")
df_categories_result.to_csv("../resources/courses/categories_pat_tsm.csv", index=False)


# In[118]:


df_categories_result[df_categories_result["idnumber"] == 1775]


# In[ ]:





# # 3. Courses
# ## 3.1 Generate the list of courses

# In[119]:


courses_pat = df_comunita
courses_pat


# Extract only those courses that are not categories

# In[120]:


courses_pat[courses_pat["CMNT_id"].isin(fathers_containing_paths.index)]


# ## Collecting all courses

# In[121]:


df_courses = courses_pat #[~courses_pat["CMNT_id"].isin(df_categories_result["idnumber"]) | (courses_pat["CMNT_id"].isin(fathers_containing_paths.index))]


# In[122]:


df_courses[df_courses["CMNT_id"] == 1795]


# In[123]:


test = 'Analisi di processo e progettazione di servizi.'
serie = pd.Series({"name": test, "IdCommunity":"123", "Id_Path":5, "EVNT_CMNT_id": 12})
def transform_name(row):
    arr = row["name"].split(' ')
    res = [re.sub(r'[\W_]+', '_', el[0:3]) for el in arr[:4]]
    path = "_" + str(row["Id_Path"]) if row["Id_Path"] != 0 else ""
    path = "" if row["EVNT_CMNT_id"] != 0 else path
    return "_".join(res) + "_" + row["IdCommunity"]  + path
transform_name(serie)


# In[124]:


df_courses[df_courses["CMNT_id"] == 120]


# In[ ]:





# ## Include also diario courses

# In[125]:


df_comunita_diario = df_comunita.drop_duplicates(subset="CMNT_id").merge(df_diario.drop_duplicates(subset="EVNT_CMNT_id"), left_on="CMNT_id", right_on="EVNT_CMNT_id")
df_comunita_diario


# In[126]:


df_courses = pd.concat([df_courses, df_comunita_diario])
df_courses["EVNT_CMNT_id"] = df_courses["EVNT_CMNT_id"].fillna(0)
df_courses


# In[ ]:





# In[127]:


df_courses_temp = df_courses.merge(df_comunita.drop_duplicates(subset=["CMNT_id"]), left_on="CMNT_idPadre", right_on="CMNT_id")


# In[128]:


df_courses_temp[df_courses_temp["CMNT_id_x"] == 2818]


# In[129]:


df_courses_pat = df_courses_temp[["CMNT_id_x", "CMNT_nome_x", "CMNT_nome_y", "Name_x", "Id_x", "EVNT_CMNT_id"]]
df_courses_pat = df_courses_pat.rename(columns={"CMNT_nome_x":"name", "CMNT_nome_y": "category", "CMNT_id_x": "IdCommunity", "Name_x": "Name_Path", "Id_x": "Id_Path"})
df_courses_pat["IdCommunity"] = df_courses_pat["IdCommunity"].astype(str)
df_courses_pat["Id_Path"] = df_courses_pat["Id_Path"].fillna(0)
df_courses_pat["Id_Path"] = df_courses_pat["Id_Path"].astype(int)
df_courses_pat["Name_Path"] = df_courses_pat["Name_Path"].fillna("")
# generate shortname
df_courses_pat["shortname"] = df_courses_pat.apply(lambda x: transform_name(x), axis=1)
df_courses_pat["idCourseMoodle"] = ""


# In[130]:


df_courses_pat[df_courses_pat["IdCommunity"] == "1795"]


# ### Rename courses after their path's name

# In[131]:


multi_path_courses = pd.DataFrame(df_courses_pat[df_courses_pat["EVNT_CMNT_id"] == 0].groupby(["IdCommunity"]).count()["name"]>=1)
multi_path_courses


# In[132]:


multi_path_courses = pd.DataFrame(df_courses_pat[df_courses_pat["EVNT_CMNT_id"] == 0].groupby(["IdCommunity"]).count()["name"]>=1)
multi_path_courses = multi_path_courses[multi_path_courses["name"]==True]
condition = (df_courses_pat["EVNT_CMNT_id"] == 0) & (df_courses_pat["Id_Path"] != 0) & (df_courses_pat["IdCommunity"].isin(multi_path_courses.index))
df_courses_pat.loc[condition, ["category"]] = df_courses_pat["name"]
df_courses_pat.loc[condition, ["name"]] = df_courses_pat["Name_Path"]


# ## Rename category of diario 

# In[133]:


condition = (df_courses_pat["EVNT_CMNT_id"] != 0)
df_courses_pat.loc[condition, ["category"]] = df_courses_pat["name"]


df_courses_pat.loc[condition, ["name"]] = "Diario di lezione - " + df_courses_pat["name"]
df_courses_pat.loc[condition, ["Id_Path"]] = 0


# In[134]:


df_courses_pat[df_courses_pat["IdCommunity"] == "2818"]


# In[135]:


df_courses_pat["IdCommunity"] = df_courses_pat["IdCommunity"].astype('int32')
df_courses_pat = df_courses_pat.drop_duplicates(subset="shortname")


# In[136]:


df_courses_pat


# In[137]:


df_courses_pat[df_courses_pat["shortname"] == "Cul_3078"]


# In[ ]:





# In[138]:


df_courses_pat[df_courses_pat["IdCommunity"] == "1775"][["IdCommunity"]].drop_duplicates(subset=["IdCommunity"]).sort_values(by="IdCommunity", ascending=False).to_csv("temp.csv", index=False)


# In[173]:


df_courses_pat[df_courses_pat["category"]== "Formazione P.I.Tre."][["IdCommunity"]]


# In[ ]:





# In[156]:


import re
match2 = "".join(re.findall("[a-zA-Z]+", "sadasd22324"))
match2


# In[ ]:





# # Sections inside each course - Units

# In[157]:


# Extract only the first tag as section name from the description
def transformer(row):
    soup = BeautifulSoup(row, 'html.parser')
    section = soup.get_text()[:100]+ "..."
    for el in soup.find_all('strong'): 
        if el.text != None and el.text.strip() != "" and ~pd.isna(el.text):
            section = el.text[:100]
            break
    return section.encode().decode('utf-8').strip()


# In[158]:


df_percorso_units = pd.read_sql("Select Id, IdCommunity, IdPath, Name, Description, DisplayOrder, _Deleted from [ELLE3].[dbo].[EP_Unit] \
                                where IdCommunity = 200 and IdPath=202 and _Deleted <>1", connection)
df_percorso_units


# In[159]:


# Units
df_percorso_units = pd.read_sql("Select Id, IdCommunity, IdPath, Name, Description, DisplayOrder from [ELLE3].[dbo].[EP_Unit] \
                                where _Deleted <>1", connection)
df_percorso_units.loc[(df_percorso_units["Name"].str.strip() == "") & (df_percorso_units["Description"].str.strip() != ""), ["Name"]] = df_percorso_units["Description"].apply(lambda x: transformer(x))
df_percorso_units = df_percorso_units[(df_percorso_units["Name"].str.strip() != "") & (df_percorso_units["Description"].str.strip() != "")].sort_values(by="DisplayOrder")
df_percorso_units["Description"] = df_percorso_units["Description"].fillna(" ").astype(str)
df_percorso_units["Description"] = df_percorso_units["Description"].str.replace('"', "'")
df_percorso_units["Name"] = df_percorso_units["Name"].fillna(" ").apply(lambda x: x.encode().decode('utf-8').strip()).str[0:100].replace('"', "'")
df_percorso_units["MoodleOrder"] = ""

# Activities
df_percorso_activities = pd.read_sql("Select IdCommunity, Id, IdUnit, IdPath, Name, Description, Weight, DisplayOrder, _Deleted \
                                     from [ELLE3].[dbo].[EP_Activity] where _Deleted <> 1", connection)
df_percorso_activities["customfield_duration_hours"] = df_percorso_activities["Weight"].apply(lambda x: x//60*3600 if x>60 else 0)
df_percorso_activities["customfield_duration_mins"] = df_percorso_activities["Weight"] % 60 * 60
# Subactivities
df_percorso_subactivities = pd.read_sql("Select Id,IdActivity,Name,ContentPermission,Description,DisplayOrder,Status,ContentType,Link,IdCommunity, \
                                        IdObjectLong,IdModuleAction,IdModule,CodeModule,IdModuleLink,_Deleted,Weight,IdPath,Duration \
                                        from [ELLE3].[dbo].[EP_SubActivity] where _Deleted <> 1", connection)


# ## Examples for course 1750 - "Formazione salute For_Sal_e_Sic_1750_262"

# In[160]:


df_percorso_units[(df_percorso_units["IdCommunity"]==200) & (df_percorso_units["IdPath"]==202)]#["Description"].to_dict()
#df_percorso_units[(df_percorso_units["IdCommunity"]==1775) & (df_percorso_units["IdPath"]==265)]#["Description"].to_dict()
#df_percorso_units[(df_percorso_units["IdCommunity"]==3077) ]


# In[161]:


df_percorso_units.to_csv("../resources/courses/courses_sections.csv", index=False)


# In[ ]:





# ### Activities

# ## Consider as Course Sections also the activities of type Subsection 

# In[162]:


df_percorso_activities[(df_percorso_activities["IdCommunity"]==1750) & (df_percorso_activities["IdPath"]==262) & (df_percorso_activities["IdUnit"] == 948)]


# In[163]:


#Example 1791
to_be_considered_section_activities = df_percorso_activities#[(df_percorso_activities["IdCommunity"]==1775) & (df_percorso_activities["IdPath"]==265)]
# Example 1750
#to_be_considered_section_activities = df_percorso_activities[(df_percorso_activities["IdCommunity"]==1750) & (df_percorso_activities["IdPath"]==262)]
to_be_considered_section_activities = to_be_considered_section_activities[ \
                                                                 (to_be_considered_section_activities["IdUnit"].isin(df_percorso_units["Id"])) \
                                                                  & (~to_be_considered_section_activities["Id"].isin(df_percorso_subactivities["IdActivity"])) \
                                                                 # & (to_be_considered_section_activities["Name"].str.strip() != "")\
                                                                  & (to_be_considered_section_activities["Description"].str.strip() != "")
                                                                         ]
# & (df_percorso_subactivities["IdActivity"].isin(to_be_considered_section_activities["Id"]))
to_be_considered_section_activities["Name"] = to_be_considered_section_activities["Description"].apply(lambda x: transformer(x)).replace('"', "'")
to_be_considered_section_activities = to_be_considered_section_activities.sort_values(by=["IdUnit", "DisplayOrder"])
to_be_considered_section_activities = to_be_considered_section_activities.rename(columns={"IdUnit": "Id_section"})
to_be_considered_section_activities#.to_dict('records')


# In[164]:


to_be_considered_section_activities.to_csv("../resources/courses/courses_sections_sub.csv", index=False)


# In[ ]:





# In[165]:


test = pd.read_csv("../resources/courses/courses_sections_sub.csv")
test[(test["IdCommunity"]==1750) & (test["IdPath"]==262)]#.to_dict()


# ## Subactivities

# In[166]:


df_activities_all = df_percorso_activities \
                .merge(df_percorso_subactivities, left_on="Id", right_on="IdActivity")
df_activities_all = df_activities_all[["IdCommunity_x", "Id_y", "IdUnit", "IdPath_x", "IdActivity", "Name_x", "Description_x", \
                                       "customfield_duration_hours", "customfield_duration_mins", "DisplayOrder_x", \
                                       "ContentType", "IdObjectLong", "CodeModule", "IdModule"]]
df_activities_all = df_activities_all.rename(columns={"IdCommunity_x":"IdCommunity", "Id_y": "Id", "IdPath_x":"IdPath", "Name_x":"Name", \
                                             "Description_x": "Description", "DisplayOrder_x":"DisplayOrder"})
df_activities_all["IdMoodle"] = ""
df_activities_all["moduleMoodle"] = ""
df_activities_all["Description"] = df_activities_all["Description"].fillna(df_activities_all["Name"])


# In[167]:


df_activities_all[(df_activities_all["IdCommunity"] == 1750) & (df_activities_all["IdPath"] == 234)]


# In[ ]:





# In[168]:


df_activities_all.to_csv("../resources/courses/courses_activities.csv", index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # Percorsi Formativi without any section

# In[169]:


paths = df_courses_pat[df_courses_pat["Id_Path"] != 0]
empty_paths = paths[~paths["Id_Path"].isin(df_percorso_units["IdPath"])]
empty_paths


# In[170]:


df_courses_pat2 = df_courses_pat[(~df_courses_pat["Id_Path"].isin(empty_paths["Id_Path"]))]
df_courses_pat2


# In[171]:


df_courses_pat2.sort_values(by="IdCommunity").to_csv("../resources/courses/courses_pat_tsm.csv", index=False)


# In[172]:


df_courses_pat2[df_courses_pat2["IdCommunity"] == 1750]


# In[ ]:





# In[ ]:





# In[ ]:





# # Activities inside each section

# In[320]:


df_percorso_activities = pd.read_sql("Select IdCommunity, Id, IdUnit, IdPath, Name, Description, Weight, DisplayOrder, _Deleted from [ELLE3].[dbo].[EP_Activity] ", connection)
#df_percorso_activities = df_percorso_activities[df_percorso_activities["IdCommunity"] == 1775]
# Consider only activities referring to subactivities, excluding those that are considered as subsections
df_percorso_activities = df_percorso_activities[df_percorso_activities["Name"].notna()] 
df_percorso_activities["customfield_duration_hours"] = df_percorso_activities["Weight"].apply(lambda x: x//60*3600 if x>60 else 0)
df_percorso_activities["customfield_duration_mins"] = df_percorso_activities["Weight"] % 60 * 60
#df_percorso_activities = df_percorso_activities[["IdCommunity", "Id", "IdUnit", "IdPath", "Name", "Description", "customfield_duration_hours", "customfield_duration_mins", "DisplayOrder", "_Deleted"]].sort_values(by=["IdCommunity", "IdPath", "IdUnit", "DisplayOrder"])
df_percorso_activities


# In[321]:


#df_percorso_activities[(df_percorso_activities["IdCommunity"]==345) & (df_percorso_activities["IdPath"]==142)]
df_percorso_activities[(df_percorso_activities["IdCommunity"]==1750) & (df_percorso_activities["IdPath"]==262)]


# In[ ]:





# # SubActivities

# In[322]:


df_percorso_subactivities[(df_percorso_subactivities["IdCommunity"] == 3077)]


# In[323]:


df_percorso_subactivities["ContentType"].value_counts()


# In[ ]:





# In[324]:


df_activities_all = df_percorso_activities \
                .merge(df_percorso_subactivities, left_on="Id", right_on="IdActivity")
df_activities_all = df_activities_all[["IdCommunity_x", "Id_y", "IdUnit", "IdPath_x", "IdActivity", "Name_x", "Description_x", \
                                       "customfield_duration_hours", "customfield_duration_mins", "DisplayOrder_x", \
                                       "ContentType", "IdObjectLong", "CodeModule", "IdModule"]]
df_activities_all = df_activities_all.rename(columns={"IdCommunity_x":"IdCommunity", "Id_y": "Id", "IdPath_x":"IdPath", "Name_x":"Name", \
                                             "Description_x": "Description", "DisplayOrder_x":"DisplayOrder"})
df_activities_all["IdMoodle"] = ""
df_activities_all["moduleMoodle"] = ""
df_activities_all["Description"] = df_activities_all["Description"].fillna(df_activities_all["Name"])
df_activities_all[(df_activities_all["IdCommunity"] == 1791) & (df_activities_all["IdPath"] == 274)]
#df_activities_all[(df_activities_all["IdCommunity"] == 1734)]


# In[325]:


######## TEMPORARILY
#df_activities_all.to_csv("../resources/courses/courses_activities.csv", index=False)


# In[ ]:





# In[ ]:





# In[ ]:





# # Permissions on PF elements

# In[2]:


df_EP_Assignment = pd.read_sql("Select [Id]\
      ,[IdActivity]\
      ,[IdUnit]\
      ,[IdPath]\
      ,[IdSubActivity]\
      ,[IdPerson]\
      ,[IdRoleCommunity]\
      ,[IdCommunity]\
      ,[MinCompletion]\
      ,[Completion]\
      ,[Role]\
      ,[StatusAssignment]\
      ,[Status]\
      ,[Active]\
      ,[StartDate]\
      ,[EndDate] from [ELLE3].[dbo].[EP_Assignment] ", connection)


# In[3]:


df_EP_Assignment[(df_EP_Assignment["IdCommunity"] == 1795) & (df_EP_Assignment["IdPath"] == 278)] #120


# In[64]:


df_EP_Assignment[(df_EP_Assignment["IdCommunity"] == 2834) & (df_EP_Assignment["IdPath"] == 10302)]["IdRoleCommunity"].value_counts()


# In[65]:


df_EP_Assignment[(df_EP_Assignment["IdCommunity"] == 2843) & (df_EP_Assignment["IdPath"] == 10377)]["IdRoleCommunity"].value_counts()


# In[7]:


df_EP_Assignment[df_EP_Assignment["IdRoleCommunity"] == 44] 


# In[439]:


df_EP_Assignment["Role"].value_counts()


# In[440]:


df_EP_Assignment = df_EP_Assignment[["IdCommunity", "IdPath", "IdRoleCommunity"]]
df_EP_Assignment = df_EP_Assignment[(df_EP_Assignment["IdPath"].notna()) & (df_EP_Assignment["IdCommunity"].notna()) & (df_EP_Assignment["IdRoleCommunity"].notna())]
df_EP_Assignment = df_EP_Assignment.rename(columns={"IdCommunity": "Id_Community"})


# In[441]:


df_EP_Assignment.to_csv("../resources/users/percorso_role.csv", index=False)


# In[ ]:





# In[ ]:





# # Activities Dependencies

# In[142]:


df_activities_dependency = pd.read_sql("Select PathId,IdSource,IdDestination from EP_RuleCompletion", connection)
df_activities_dependency


# In[143]:


df_activities_dependency.to_csv("../resources/courses/courses_activities_dep.csv", index=False)


# In[ ]:





# # Servizio Materiale

# In[240]:


df_file = pd.read_sql("Select FLDS_id, FLDS_CMNT_id, FLDS_nome, Lower(convert(varchar(200), FLDS_GUID)) as FLDS_GUID, ContentType, FLDS_isFile, FLDS_padreID, \
         FLDS_isSCORM, IsDownloadable, IdRepositoryItemType, Extension, FLDS_dimensione from FILE_DISPONIBILE", connection)
size_tot = df_file["FLDS_dimensione"].sum()
size_tot


# In[250]:


df_file["IdMoodle"] = 0
df_file[df_file["FLDS_CMNT_id"]==2818]


# In[242]:


#df_file["FLDS_CMNT_id"] = df_file["FLDS_CMNT_id"].notna().astype('int32')


# In[243]:


df_file.loc[df_file["FLDS_id"]==5244, :]


# In[244]:


df_file["IdRepositoryItemType"].value_counts()


# ### File type:
# - 1 - PDF          --- IsDownloadable = 1
# - 2 - SCORM        --- FLDS_isSCORM = 1
# - 3 - Folder  
# - 5 - Multimedia (?)

# In[245]:


df_file[df_file["IdRepositoryItemType"] == 5]


# In[246]:


activities_all = df_activities_all[df_activities_all["CodeModule"]=="SRVMATER"]
df_materiale_of_percorso = df_file.merge(activities_all, left_on="FLDS_id", right_on="IdObjectLong")
#df_materiale_of_percorso = df_materiale_of_percorso[["FLDS_id", "FLDS_CMNT_id", "FLDS_nome", "FLDS_GUID", "ContentType_x", "FLDS_isFile", "FLDS_padreID", \
#         "FLDS_isSCORM", "IsDownloadable", "IdRepositoryItemType", "Extension"]]
df_materiale_of_percorso = df_materiale_of_percorso.rename(columns={"ContentType_x":"ContentType"})
df_materiale_of_percorso = df_materiale_of_percorso.drop_duplicates(subset=["FLDS_CMNT_id"])


# In[247]:


df_materiale_of_percorso


# In[248]:


df_file.to_csv("../resources/courses/materiale.csv", index=False)


# In[249]:


df_file[df_file["FLDS_id"] == 108814]


# In[157]:


df_courses_pat[df_courses_pat["Id_Path"]==0]


# In[ ]:





# In[162]:


files_diario = df_file[(df_file["FLDS_CMNT_id"].isin(df_courses_pat["IdCommunity"])) & (~df_file["FLDS_CMNT_id"].isin(df_materiale_of_percorso["FLDS_CMNT_id"]))]
files_bbdiario  = files_diario.drop_duplicates(subset=["FLDS_CMNT_id"])
files_diario["FLDS_CMNT_id"] = files_diario["FLDS_CMNT_id"].astype('int32')
files_diario#[files_diario["FLDS_CMNT_id"] == 2818]


# In[163]:


files_diario2 = files_diario[["FLDS_CMNT_id"]].rename(columns={"FLDS_CMNT_id":"NumeroComunità"}).sort_values(by="NumeroComunità")
files_diario2.to_csv("materiale_diario.csv", index=False)


# In[164]:


df_file[df_file["FLDS_CMNT_id"]==1775]


# In[ ]:





# # Community File Assignment

# In[166]:


df_file_assignment = pd.read_sql("Select * from CR_CommunityFileAssignment", connection)
df_file_assignment[df_file_assignment["IdCommunity"] == 1775]


# In[ ]:





# In[ ]:





# # File Transfer

# In[222]:


df_file_transfer = pd.read_sql("select [Id]\
      ,[IdFile]\
      ,cast([FileUniqueID] as varchar(200)) as FileUniqueID\
      ,[Info]\
      ,[Path]\
      ,[Filename]\
      ,[ModifiedOn]\
      ,[Decompress]\
      ,[TransferPolicy]\
      ,[Discriminator]\
      ,[IdTransferStatus]\
      ,[TotalActivity]\
      ,[DefaultDocumentPath]\
      ,[IdDefaultDocument]\
      ,[Transferred] from FR_FileTransfer", connection)


# In[231]:


df_file_transfer[df_file_transfer["IdFile"] == 64838]


# In[ ]:





# In[ ]:





# In[235]:


df_file_transfer[df_file_transfer["Id"] == 65797]


# In[ ]:





# In[175]:


df_file_2 = df_file_transfer[df_file_transfer["IdFile"].isin(files_diario["FLDS_id"])]
df_file_2


# In[ ]:





# In[176]:


df_courses_pat["IdCommunity"] = df_courses_pat["IdCommunity"].astype('int32')


# In[177]:


materiale = df_activities_all[(df_activities_all["CodeModule"]=="SRVMATER") & (df_activities_all["IdCommunity"].isin(df_courses_pat.to_dict('list')["IdCommunity"]))]
materiale#.drop_duplicates(subset=["IdCommunity"])


# In[178]:


prova = df_file_transfer.merge(df_activities_all[(df_activities_all["CodeModule"]=="SRVMATER")], left_on="IdFile", right_on="IdObjectLong")
#merge(df_courses_pat, left_on="IdCommunity", right_on="IdCommunity")
prova = prova[["IdCommunity", "Name", "FileUniqueID", "Path", "Filename"]].drop_duplicates(subset=["IdCommunity", "Filename"])
prova


# In[179]:


prova["Path"].str[0:20].value_counts()


# In[180]:


prova.to_csv("files2.csv", index=False)


# In[ ]:





# In[181]:


df_files = pd.read_csv("../../scorm/FilePaths1.csv") #view-13
df_files[df_files["FileUniqueID"]=='bcdc6391-0faa-4c27-a6aa-2ea75b5640de']


# In[329]:


df_files[df_files["IdFile"] == 64712]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # File Download Info

# In[182]:


df_file_downloads = df_file_downloads[["IdCommunity", "IdFile", "IdPerson", "IdRepositoryItemType", "ServiceCode","CreatedOn"]]
df_file_downloads["IdRepositoryItemType"].value_counts()


# In[333]:


df_file_downloads[df_file_downloads["IdFile"] == 64773] # 07 - Sicurezza info - Vademecum


# In[ ]:





# # Completing course/activities of 'Servizio Percorso Formativo'
# EP_Stat

# In[68]:


df_stat = pd.read_sql("SELECT \
[IdPerson],\
[IdPath],\
[IdUnit] ,\
[IdActivity] ,\
[IdSubActivity],\
[Completion],\
[MandatoryPassedItemCount] ,\
[StartDate],\
[Status] ,\
[Discriminator],\
[MandatoryCompletedItemCount],\
[Mark]\
               FROM [ELLE3].[dbo].[EP_Stat] \
               where Completion =100 \
               ORDER BY ID DESC", connection)
df_stat #4331  #4327 # IdPath = 265 and IdSubActivity = 22896  22892 and IdPerson = 41573  where Completion =100 Completion >= 100 \


# In[69]:


import datetime
def convert_time(x):
    timestamp = datetime.datetime.timestamp(x)
    return int(timestamp)
df_stat["IdSubActivity"] = df_stat["IdSubActivity"].fillna(0).astype('int32')
df_stat["IdActivity"] = df_stat["IdActivity"].fillna(0).astype('int32')
#df_stat["time"] = df_stat["StartDate"].apply(lambda x: convert_time(x))


# In[70]:


df_stat = df_stat[df_stat["IdSubActivity"].notna()].sort_values(by="StartDate").drop_duplicates(subset=["IdSubActivity", "IdPerson", "StartDate"], keep="last")


# In[71]:


df_stat[df_stat["StartDate"] == "2020-10-16 14:32:09"]#.groupby(["StartDate"]).count()


# In[72]:


temp = df_stat[(df_stat["IdSubActivity"] == 22896  ) & (df_stat["IdPerson"] == 41573)].sort_values(by="StartDate")#.drop_duplicates(subset=["StartDate"], keep="last")
temp#["Completion"].value_counts() # 22892 # 3856


# In[73]:


temp = df_stat[(df_stat["IdSubActivity"] == 232)].sort_values(by="StartDate").drop_duplicates(subset=["StartDate"], keep="last")
temp


# In[74]:


temp = df_stat[(df_stat["IdSubActivity"] == 232)].sort_values(by="StartDate").drop_duplicates(subset=["StartDate"], keep="last")
temp


# In[75]:


df_iscrizioni = pd.read_csv('../resources/users/LK_RUOLO_PERSONA_COMUNITA.csv')
percorso_role = pd.read_csv("../resources/users/percorso_role.csv")
current_enrolled_users = percorso_role.merge(df_iscrizioni, left_on=["Id_Community", "IdRoleCommunity"], right_on=["RLPC_CMNT_id", "RLPC_TPRL_id"])


# In[76]:


len(current_enrolled_users[ (current_enrolled_users["Id_Community"] == 1750)]) #["IdRoleCommunity"].value_counts()


# In[77]:


df_stat = df_stat[df_stat["IdPerson"].isin(current_enrolled_users["RLPC_PRSN_id"])]
df_stat


# In[78]:


df_stat[(df_stat["IdSubActivity"] == 22892  ) & (df_stat["IdPerson"] == 41573)].sort_values(by="StartDate")


# In[79]:


df_stat.to_csv("../resources/usage/status.csv", index=False)


# In[ ]:





# In[450]:


df_stat = pd.read_sql("SELECT \
[IdPerson],\
[IdPath],\
[IdUnit] ,\
[IdActivity] ,\
[IdSubActivity],\
[Completion],\
[MandatoryPassedItemCount] ,\
[StartDate],\
[Status] ,\
[Discriminator],\
[MandatoryCompletedItemCount],\
[Mark]\
               FROM [ELLE3].[dbo].[EP_Stat] \
               where Completion < 100 and Completion > 0 \
               ORDER BY ID DESC", connection)


# In[ ]:


import datetime
def convert_time(x):
    timestamp = datetime.datetime.timestamp(x)
    return int(timestamp)
df_stat["IdSubActivity"] = df_stat["IdSubActivity"].fillna(0).astype('int32')
df_stat["IdActivity"] = df_stat["IdActivity"].fillna(0).astype('int32')


# In[ ]:


df_stat.sort_values(by="Completion").drop_duplicates(subset=["IdPerson", "IdSubActivity"], keep="last")


# In[ ]:


df_stat.to_csv("../resources/usage/status_initiated.csv", index=False)


# In[ ]:





# In[381]:


df_stat["Completion"].value_counts().head(20)


# In[382]:


len(df_stat)


# In[199]:


df_users = pd.read_csv("../resources/users/users_local.csv")
df_users


# In[200]:


df_usage = temp.merge(df_users, left_on="IdPerson", right_on="PRSN_id")
df_usage


# In[ ]:





# In[ ]:




