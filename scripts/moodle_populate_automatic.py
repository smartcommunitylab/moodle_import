import pandas as pd
import multiprocessing as mp
from moodle_services import create_categories, core_course_create_courses, core_user_create_users, enrol_manual_enrol_users, core_user_delete_users,\
    core_cohort_create_cohorts, add_group_members, create_groups, generate_inactive_enrolments, create_groups_formazione_permanente
from moodle_services_post import create_blocks, activities_dependencies, activities_completion, import_scorm_tracks, tag_courses, add_wikis, \
    questionnaire_import_responses_quiz, questionnaire_import_responses_questionnaire
from utils import role_mapping
from config import instance_params

instance = instance_params["instance"]
df_courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv")
df_activities = pd.read_csv("../resources/courses/courses_activities.csv")
df_dependencies = pd.read_csv("../resources/courses/courses_activities_dep.csv")
df_materiale = pd.read_csv("../resources/courses/materiale.csv")

def delete_users():
    '''
     Delete existing users
    '''
    df_users_instance = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    core_user_delete_users(df_users_instance["moodleUserId"])

def creating_users(df):
    core_user_create_users(df)

def create_users():
    '''
    Create users
    '''
    pool = mp.Pool(mp.cpu_count()) # use 4 processes

    #df_users = pd.read_csv("../resources/users/users.csv", nrows=2)

    df_users = pd.read_csv("../resources/users/users.csv", chunksize=100)
    for blocks in df_users:
        df_users_created = pd.read_csv("../resources/users/users_{}.csv".format(instance))
        blocks = blocks[~blocks["username"].isin(df_users_created["usernameMoodle"])]
        core_user_create_users(blocks)



def append_duplicated_users():
    # After creating unique users let's merge with the duplicated users in order to link them with the same moodle user account
    df_users_created = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_users_duplicated = pd.read_csv("../resources/users/users_duplicated.csv")
    for user in df_users_duplicated.itertuples():
        moodle_user = df_users_created[df_users_created["username"] == user.username]
        df_users_duplicated.loc[df_users_duplicated["username"] == user.username, ["moodleUserId"]] = moodle_user[
            "moodleUserId"].values
    df_result = df_users_created.append(df_users_duplicated)
    # df_result = df_result.drop_duplicates(subset=[""])
    df_result.to_csv("../resources/users/users_{}.csv".format(instance), index=False)


def create_all_categories():
    '''
    Create Categories
    '''
    categories = pd.read_csv("../resources/courses/categories_pat_tsm.csv")
    create_categories(categories)

def create_courses():
    '''
    Create Courses
    '''
    courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv", chunksize=50)
    sections = pd.read_csv("../resources/courses/courses_sections.csv")
    sections["Description"] = sections["Description"].fillna("")
    sub_sections = pd.read_csv("../resources/courses/courses_sections_sub.csv")
    df_diario = pd.read_csv("../resources/courses/diario.csv")

    df_domande = pd.read_csv("../resources/questionnaire/domande.csv")
    df_domande_rating_options = pd.read_csv("../resources/questionnaire/domande_rating_options.csv")
    df_domande_rating_headers = pd.read_csv("../resources/questionnaire/domande_rating_headers.csv")

    df_domande_multichoice = pd.read_csv("../resources/questionnaire/domande_multichoice.csv")
    df_domande_multichoice_opzioni = pd.read_csv("../resources/questionnaire/domande_multichoice_opzioni.csv")
    df_pages = pd.read_csv("../resources/questionnaire/questionario_pages.csv")
    
    df_dropdown = pd.read_csv("../resources/questionnaire/domande_dropdown.csv")

    for block in courses:
        #536, 1775,
        # 1793,2863,2884,3005,3006,3011,1775,3014,3015,3016,3017
        #temp = block[(block['IdCommunity'].isin([1750]) ) & (block['Id_Path'].isin([234]))] # 1795 3077
        temp = block[block['IdCommunity'].isin([1795]) ]
        #For_Sal_e_Sic_1750_262
        #temp = block[block["shortname"] == "Sic_Can___2__121_container"]
        # "Key___San_sti_1796_10325"
        #For_per_adi_ad_1791_274 #For_Sal_e_Sic_1750_262
        #temp = courses.iloc[290:340, :] # on prod - questionnaire not imported 'Error writing to database'
        #courses.iloc[383:385, :]
        #temp = block[block['category'] == "Formazione lavoratori - aggiornamento - rischio basso"]
        temp = temp[(~temp["shortname"].str.endswith("_container"))]
        if len(temp) > 0:
            core_course_create_courses(temp, sections, sub_sections, df_diario, df_activities, \
                                       df_domande, df_domande_rating_options, df_domande_rating_headers, \
                                       df_domande_multichoice, df_domande_multichoice_opzioni, df_pages, df_dropdown)

def create_course_containers():
    """
    Creates a course containing the questionnaires not currently active inside the learning paths
    :return:
    """
    courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv", chunksize=50)
    sections = pd.read_csv("../resources/courses/courses_sections.csv")
    sections["Description"] = sections["Description"].fillna("")
    sub_sections = pd.read_csv("../resources/courses/courses_sections_sub.csv")
    df_diario = pd.read_csv("../resources/courses/diario.csv")

    df_domande = pd.read_csv("../resources/questionnaire/domande.csv")
    df_domande_rating_options = pd.read_csv("../resources/questionnaire/domande_rating_options.csv")
    df_domande_rating_headers = pd.read_csv("../resources/questionnaire/domande_rating_headers.csv")

    df_domande_multichoice = pd.read_csv("../resources/questionnaire/domande_multichoice.csv")
    df_domande_multichoice_opzioni = pd.read_csv("../resources/questionnaire/domande_multichoice_opzioni.csv")
    df_pages = pd.read_csv("../resources/questionnaire/questionario_pages.csv")

    df_dropdown = pd.read_csv("../resources/questionnaire/domande_dropdown.csv")

    containers_courses = df_courses[(df_courses["shortname"].str.endswith("_container")) & (df_courses["idCourseMoodle"] == 0)]
    #containers_courses = containers_courses[containers_courses["IdCommunity"] == 1750]
    core_course_create_courses(containers_courses, sections[sections["IdCommunity"] == -1], sub_sections[sub_sections["Id_section"] == -1], \
                             df_diario[df_diario["EVNT_CMNT_id"] == -1], df_activities[df_activities["IdCommunity"] == -1], \
                             df_domande, df_domande_rating_options, df_domande_rating_headers, \
                             df_domande_multichoice, df_domande_multichoice_opzioni, df_pages, df_dropdown, \
                             container=True)


def append_container_courses():
    # add the container courses in the courses dataframe
    df_questionnaires = pd.read_csv("../resources/questionnaire/questionnaires.csv", converters={"QSML_Descrizione": str})
    questionnaires = df_questionnaires[df_questionnaires["IdMoodle"] == 0]
    course_list = []
    containers_courses = df_courses[df_courses["shortname"].str.endswith("_container")]
    print(containers_courses)
    if len(containers_courses) > 0:
        return containers_courses
    for comm_id, comm_questionnaires in questionnaires.groupby(["QSGR_CMNT_Id"]):
        course_category = df_courses[df_courses["IdCommunity"] == comm_id].to_dict('records')
        if len(course_category) > 0:
            course_category = course_category[0]
            course_category["name"] = "Contenitore di: {}".format(course_category["name"].replace("Diario di lezione - ", ""))
            course_category["Name_Path"] = ""
            course_category["Id_Path"] = 0
            course_category["EVNT_CMNT_id"] = 0
            course_category["shortname"] = "{}_container".format(course_category["shortname"])
            course_category["idCourseMoodle"] = 0
            course_list.append(course_category)
    containers_courses = pd.DataFrame(course_list)
    print(containers_courses)
    df_courses2 = df_courses.append(containers_courses, ignore_index=True)
    print(df_courses2)
    df_courses2.to_csv("../resources/courses/courses_pat_tsm.csv", index=False)
    return containers_courses


def config_blocks():
    '''
    Create Blocks
    '''
    courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv")
    courses = courses[(~courses["idCourseMoodle"].isna()) & (courses["idCourseMoodle"] != 0)]
    create_blocks(courses)
    
def config_activities_dependencies():
    '''
    Apply dependencies between activities
    '''
    activities_dependencies(df_activities, df_dependencies)

def completion_status():
    '''
    Update completion status of activities for the users
    :return:
    '''
    df_user_moodle = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_status = pd.read_csv("../resources/usage/status.csv")
    #df_status = df_status[(df_status["IdPath"] == 234) & (df_status["IdPerson"] == 41573)]
    activities_completion(df_activities, df_user_moodle, df_status)

def generate_enrolments_list_diario():
    '''
    Generate enrolments list
    '''
    df_tipo_ruolo = pd.read_csv("../resources/users/TIPO_RUOLO.csv")
    df_users = pd.read_csv("../resources/users/users.csv")
    df_iscrizioni = pd.read_csv('../resources/users/LK_RUOLO_PERSONA_COMUNITA.csv')
    df_iscrizioni = df_iscrizioni[df_iscrizioni["RLPC_PRSN_id"].isin(df_users['PRSN_id'])]
    df_iscrizioni = df_iscrizioni[['RLPC_CMNT_id', 'RLPC_PRSN_id', 'RLPC_TPRL_id']]
    df_iscrizioni = df_iscrizioni.merge(df_tipo_ruolo, left_on="RLPC_TPRL_id", right_on="TPRL_id")
    df_iscrizioni["roleId"] = df_iscrizioni["RLPC_TPRL_id"].apply(lambda x: role_mapping(x))

    df_courses_pat_moodle = pd.read_csv("../resources/courses/courses_pat_tsm.csv")
    df_courses_pat_moodle = df_courses_pat_moodle[(df_courses_pat_moodle["idCourseMoodle"].notna()) & (df_courses_pat_moodle["idCourseMoodle"] != 0)]
    #df_courses_pat_moodle = df_courses_pat_moodle[df_courses_pat_moodle["IdCommunity"] == 1750]
    df_com_iscriz = df_courses_pat_moodle.merge(df_iscrizioni[df_iscrizioni["RLPC_TPRL_id"]  != -3], left_on="IdCommunity", right_on="RLPC_CMNT_id")

    df_user_moodle = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_com_iscriz_user = df_com_iscriz.merge(df_user_moodle, left_on="RLPC_PRSN_id", right_on="PRSN_id")
    result_df = df_com_iscriz_user[["idCourseMoodle", "Id_Path", "moodleUserId", "roleId", "TPRL_nome"]]
    result_df = result_df.drop_duplicates(subset=["IdCommunity", "moodleUserId", "roleId", "TPRL_nome"])
    result_df.to_csv("../resources/users/enrolments_{}.csv".format(instance), index=False)

    # create groups belonging to roles 'Partecipante' and 'Formazione permanente' for courses derived from 'Diario'
    df_groups = result_df[(result_df["Id_Path"] == 0) & ((result_df["TPRL_nome"].str.contains("edizione")) | (result_df["TPRL_nome"].str.contains("Formazione permanente")))]
    create_groups(df_groups.drop_duplicates(subset=["idCourseMoodle", "TPRL_nome"]))
    create_groups_formazione_permanente(df_courses_pat_moodle[(df_courses_pat_moodle["Id_Path"] == 0)])


def generate_groups_percorso():
    df_tipo_ruolo = pd.read_csv("../resources/users/TIPO_RUOLO.csv")
    df_role_pf = pd.read_csv("../resources/users/percorso_role.csv")
    df_courses_pat_moodle = pd.read_csv("../resources/courses/courses_pat_tsm.csv")
    df_courses_pat_moodle = df_courses_pat_moodle[(df_courses_pat_moodle["Id_Path"] != 0) & (df_courses_pat_moodle["idCourseMoodle"].notna())]
    #df_courses_pat_moodle = df_courses_pat_moodle[(df_courses_pat_moodle["IdCommunity"] == 1750) & (df_courses_pat_moodle["Id_Path"] == 234)]
    df_groups = df_role_pf.merge(df_tipo_ruolo, left_on="IdRoleCommunity", right_on="TPRL_id")
    # consider only roles Partecipante and Formazione Permanente
    df_groups = df_groups[(df_groups["TPRL_nome"].str.contains("edizione"))]
    df_groups = df_courses_pat_moodle.merge(df_groups, left_on=["IdCommunity", "Id_Path"], right_on=["Id_Community", "IdPath"])
    df_groups = df_groups.drop_duplicates(subset=["IdCommunity", "Id_Path", "TPRL_nome"])
    df_groups = df_groups[["IdCommunity", "Id_Path", "idCourseMoodle", "TPRL_nome"]]
    create_groups(df_groups)
    create_groups_formazione_permanente(df_courses_pat_moodle)




def enrol_users_to_courses(type="diario"):
    '''
    Enrol users to courses
    '''
    if type == "diario":
        df_user_course_enrolments = pd.read_csv("../resources/users/enrolments_{}.csv".format(instance), chunksize=10)
    else:
        df_user_course_enrolments = pd.read_csv("../resources/users/enrolments_percorso_{}.csv".format(instance),
                                                chunksize=10)
    for block in df_user_course_enrolments:
        block = block[(block["idCourseMoodle"].notna())]
        if type == "diario":
            block = block[(block["Id_Path"] == 0)]
        #block = block[block["idCourseMoodle"] == 6216]
        if len(block) > 0:
            enrol_manual_enrol_users(block)
            add_group_members(block)


def enrol_users_to_percorsi():
    df_role_pf = pd.read_csv("../resources/users/percorso_role.csv")
    #df_role_pf = df_role_pf[(df_role_pf["Id_Community"] == 1750) & (df_role_pf["IdPath"] == 234)]
    df_iscrizioni = pd.read_csv('../resources/users/LK_RUOLO_PERSONA_COMUNITA.csv')
    #df_iscrizioni = df_iscrizioni[df_iscrizioni["RLPC_CMNT_id"] == 1750]
    df_tipo_ruolo = pd.read_csv("../resources/users/TIPO_RUOLO.csv")
    df_user_moodle = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    #df_user_moodle = df_user_moodle[(df_user_moodle["PRSN_id"] == 41573)]
    df_courses_pat_moodle = pd.read_csv("../resources/courses/courses_pat_tsm.csv")
    df_courses_pat_moodle = df_courses_pat_moodle[(df_courses_pat_moodle["Id_Path"] != 0) \
                                                  & (df_courses_pat_moodle["idCourseMoodle"].notna()) \
                                                  & (df_courses_pat_moodle["idCourseMoodle"] != 0) ]

    #print(df_iscrizioni.drop_duplicates(subset=["RLPC_CMNT_id", "RLPC_TPRL_id"]))
    #print(df_role_pf.drop_duplicates(subset=["Id_Community", "IdPath", "IdRoleCommunity"]))
    df_enrollments = df_role_pf.merge(df_iscrizioni, left_on=["Id_Community", "IdRoleCommunity"], right_on=["RLPC_CMNT_id", "RLPC_TPRL_id"])

    #print(df_enrollments.groupby(["Id_Community", "IdPath", "RLPC_TPRL_id"]).count()) #.drop_duplicates(subset=["Id_Community", "IdRoleCommunity"]))
    df_enrollments = df_enrollments.merge(df_tipo_ruolo, left_on="RLPC_TPRL_id", right_on="TPRL_id")
    df_enrollments["roleId"] = df_enrollments["RLPC_TPRL_id"].apply(lambda x: role_mapping(x))
    df_enrollments = df_enrollments.merge(df_courses_pat_moodle, left_on=["Id_Community", "IdPath"], right_on=["IdCommunity", "Id_Path"])
    df_enrollments = df_enrollments.merge(df_user_moodle, left_on="RLPC_PRSN_id", right_on="PRSN_id")

    result_df = df_enrollments[["idCourseMoodle", "moodleUserId", "roleId", "TPRL_nome"]]
    result_df = result_df.drop_duplicates(subset=["idCourseMoodle", "moodleUserId", "roleId", "TPRL_nome"])
    result_df.to_csv("../resources/users/enrolments_percorso_{}.csv".format(instance), index=False)

    # active enrolments of users into active roles in Percorso Formativo
    enrol_users_to_courses(type="percorso")
    generate_inactive_enrolments()



def create_groups_in_courses():
    courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv", chunksize=1)
    for block in courses:
        block = block[block["idCourseMoodle"].notna()]
        create_groups(block)

def create_cohorts():
    courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv")
    cohorts = pd.read_csv("../resources/users/cohorts.csv", chunksize=50)
    for block in cohorts:
        core_cohort_create_cohorts(block, courses)

def importing_scorm_tracks():
    '''
    Import scorm tracks
    :return:
    '''
    df_materiale = pd.read_csv("../resources/courses/materiale.csv")
    df_scorm_tracks = pd.read_csv("../resources/usage/scorm_tracks_pat.csv")
    df_users = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    #df_users = df_users[df_users["PRSN_id"] == 54337]
    import_scorm_tracks(df_materiale, df_activities, df_scorm_tracks, df_users)

def tagging_courses():
    '''

    :return:
    '''
    courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv")
    tag_courses(courses)

def adding_wikis():
    '''

    :return:
    '''
    df_courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv")
    df_wiki = pd.read_csv("../resources/wiki/wiki.csv")
    df_wiki_sections = pd.read_csv("../resources/wiki/wiki_sections.csv")
    df_topics = pd.read_csv("../resources/wiki/wiki_topics.csv")
    df_topics_history = pd.read_csv("../resources/wiki/wiki_topic_hist.csv")
    df_users = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_materiale = pd.read_csv("../resources/courses/materiale.csv")
    # DEM_Cor_27_132
    # [df_courses["shortname"] == "For_P_I_120"]
    # [df_courses["IdCommunity"] == 118]
    add_wikis(df_courses, df_wiki, df_wiki_sections, df_topics, df_topics_history, df_users, df_materiale)

def import_quiz_responses():
    '''
    Import the quiz responses given by each user
    :return:
    '''
    df_questionnaires = pd.read_csv("../resources/questionnaire/questionnaires.csv")
    df_questionnaires_random = df_questionnaires[(df_questionnaires["QSTN_Tipo"].isin([1,5,7]))].drop_duplicates(subset=["QSTN_Id"])
    df_users = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_risposte_domande_random_quiz = pd.read_csv("../resources/questionnaire/risposte_domande_random_quiz.csv")

    df_risposte_multichoice = pd.read_csv("../resources/questionnaire/risposte_multichoice_opzioni.csv")
    #df_questionnaires_random = df_questionnaires_random[df_questionnaires_random["QSTN_Id"] == 1946]
    #print(df_questionnaires_random)
    #df_users = df_users[df_users["PRSN_id"] == 54337]
    # Import responses of random quiz
    questionnaire_import_responses_quiz(df_questionnaires_random, df_users, df_risposte_domande_random_quiz, df_risposte_multichoice)

def import_questionnaire_responses():
    '''
    Import the quiz responses given by each user
    :return:
    '''
    df_questionnaires = pd.read_csv("../resources/questionnaire/questionnaires.csv")
    df_questionnaires = df_questionnaires[(df_questionnaires["QSTN_Tipo"] == 0)]
    # [1793,2863,2884,3005,3006,3011,1775,3014,3015,3016,3017]
    #df_questionnaires = df_questionnaires[(df_questionnaires["QSTN_Id"].isin([3180]))]
    df_users = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_risposte_domande_non_random_quiz = pd.read_csv("../resources/questionnaire/risposte_domande_non_random.csv")
    df_risposte_multichoice = pd.read_csv("../resources/questionnaire/risposte_multichoice_opzioni.csv")
    df_domande = pd.read_csv("../resources/questionnaire/domande.csv")
    df_risposte_rating = pd.read_csv("../resources/questionnaire/risposte_rating_opzioni.csv")
    df_risposte_short_answer = pd.read_csv("../resources/questionnaire/risposte_short_answer.csv")
    df_risposte_numeric = pd.read_csv("../resources/questionnaire/risposte_numeric.csv")
    #df_risposte_rating = df_risposte_rating[df_risposte_rating["RSRT_RSQS_Id"] == 290930]
    #df_questionnaires = df_questionnaires[df_questionnaires["IdMoodle"] == 376]
    #df_users = df_users[df_users["PRSN_id"] == 54337]
    questionnaire_import_responses_questionnaire(df_questionnaires, df_users, df_risposte_domande_non_random_quiz, df_risposte_multichoice, df_domande, df_risposte_rating, df_risposte_short_answer, df_risposte_numeric)


# PHASE 1

############################## STEP 0: Delete existing users
#delete_users()
############################## STEP 1: Create users
#create_users()
############################## Append duplicated users
# append_duplicated_users()
############################## STEP 2: Create Categories
#create_all_categories()
############################## STEP 3: Create Courses
create_courses()

# PHASE 2

############################## STEP 4: Apply activities dependencies
#config_activities_dependencies()
############################## STEP 5: Config Blocks
#config_blocks()
############################## STEP 6: Generate groups

############################## STEP 7: Generate enrolments list
generate_enrolments_list_diario()
#generate_groups_percorso()
############################## STEP 8: Enrol users to courses
#enrol_users_to_courses()
#enrol_users_to_percorsi()

############################## STEP 9: Update activities completion for each user
#completion_status()

# PHASE 3

############################## STEP 10: Import scorm tracks
#importing_scorm_tracks()
############################## STEP 11: Import wiki
#adding_wikis()
############################## STEP 12: Import quiz responses
#import_quiz_responses()
############################## STEP 13: Import questionnaire responses
#import_questionnaire_responses()

# PHASE 4

############################## Create Course containers for importing questionnaires
#append_container_courses()   # Execute separately - mini step 1
#df_questionnaires = pd.read_csv("../resources/questionnaire/questionnaires.csv")
#container_questionnaire = df_questionnaires[(df_questionnaires["QSTN_Tipo"] == 0) & (df_questionnaires["IdMoodle"] == 0)]
#create_course_containers()   # Execute mini step 2
#import_questionnaire_responses(container_questionnaire) # Execute ministep 3
