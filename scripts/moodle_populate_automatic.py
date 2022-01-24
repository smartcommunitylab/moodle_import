import pandas as pd
import multiprocessing as mp
from moodle_services import create_categories, core_course_create_courses, core_user_create_users, enrol_manual_enrol_users, core_user_delete_users,\
    core_cohort_create_cohorts, create_groups, add_group_members
from moodle_services_post import create_blocks, activities_dependencies, activities_completion, import_scorm_tracks, tag_courses, add_wikis, \
    questionnaire_import_responses_quiz, questionnaire_import_responses_questionnaire
from utils import role_mapping
from config import instance_params

instance = instance_params["instance"]
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
    #df_users = pd.read_csv("../resources/users/users.csv", chunksize=100) # , names=df_users.columns, skiprows=41353
    #for blocks in df_users:
    #    #pool.apply_async(creating_users,[blocks])
    #    core_user_create_users(blocks)
    # After creating unique users let's merge with the duplicated users in order to link them with the same moodle user account
    df_users_created = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_users_duplicated = pd.read_csv("../resources/users/users_duplicated.csv")
    for user in df_users_duplicated.itertuples():
        moodle_user = df_users_created[df_users_created["username"] == user.username]
        df_users_duplicated.loc[df_users_duplicated["username"] == user.username, ["moodleUserId"]] = moodle_user["moodleUserId"].values
    df_result = df_users_created.append(df_users_duplicated)
    df_result.to_csv("../resources/users/users_{}.csv".format(instance),index=False)

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
    courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv".format(), chunksize=50)
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
        temp = block[block['IdCommunity'].isin([1775]) ]
        #temp = courses[courses["shortname"] == "Gli_str_del_PAT_536_10398"]
        #temp = courses.iloc[290:340, :] # on prod - questionnaire not imported 'Error writing to database'
        #courses.iloc[383:385, :]
        core_course_create_courses(temp, sections, sub_sections, df_diario, df_activities, \
                                   df_domande, df_domande_rating_options, df_domande_rating_headers, \
                                   df_domande_multichoice, df_domande_multichoice_opzioni, df_pages, df_dropdown)

def config_blocks():
    '''
    Create Blocks
    '''
    courses = pd.read_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance))
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
    activities_completion(df_activities, df_user_moodle, df_status)

def generate_enrolments_list():
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

    df_courses_pat_moodle = pd.read_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance))
    df_com_iscriz = df_courses_pat_moodle.merge(df_iscrizioni[df_iscrizioni["RLPC_TPRL_id"]  != -3], left_on="IdCommunity", right_on="RLPC_CMNT_id")

    df_user_moodle = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_com_iscriz_user = df_com_iscriz.merge(df_user_moodle, left_on="RLPC_PRSN_id", right_on="PRSN_id")
    result_df = df_com_iscriz_user[["idCourseMoodle", "moodleUserId", "roleId", "TPRL_nome"]]
    result_df.to_csv("../resources/users/enrolments_{}.csv".format(instance), index=False)

def enrol_users_to_courses():
    '''
    Enrol users to courses
    '''
    df_user_course_enrolments = pd.read_csv("../resources/users/enrolments_{}.csv".format(instance), chunksize=200)
    for block in df_user_course_enrolments:
        block = block[block["idCourseMoodle"].notna()]
        #block = block[block["idCourseMoodle"]==5729]
        enrol_manual_enrol_users(block)
        add_group_members(block)

def create_groups_in_courses():
    courses = pd.read_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance), chunksize=1)
    for block in courses:
        block = block[block["idCourseMoodle"].notna()]
        create_groups(block)

def create_cohorts():
    courses = pd.read_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance))
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
    df_users = df_users[df_users["PRSN_id"] == 54337]
    import_scorm_tracks(df_materiale, df_activities, df_scorm_tracks, df_users)

def tagging_courses():
    '''

    :return:
    '''
    courses = pd.read_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance))
    tag_courses(courses)

def adding_wikis():
    '''

    :return:
    '''
    df_courses = pd.read_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance))
    df_wiki = pd.read_csv("../resources/wiki/wiki.csv")
    df_wiki_sections = pd.read_csv("../resources/wiki/wiki_sections.csv")
    df_topics = pd.read_csv("../resources/wiki/wiki_topics.csv")
    df_topics_history = pd.read_csv("../resources/wiki/wiki_topic_hist.csv")
    df_users = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_materiale = pd.read_csv("../resources/courses/materiale.csv")
    # DEM_Cor_27_132
    # [df_courses["shortname"] == "For_P_I_120"]
    add_wikis(df_courses[df_courses["IdCommunity"] == 118], df_wiki, df_wiki_sections, df_topics, df_topics_history, df_users, df_materiale)

def import_quiz_responses():
    '''
    Import the quiz responses given by each user
    :return:
    '''
    df_questionnaires = pd.read_csv("../resources/questionnaire/questionnaires.csv")
    df_questionnaires_random = df_questionnaires[df_questionnaires["QSTN_Tipo"] == 7]
    df_users = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_risposte_domande_random_quiz = pd.read_csv("../resources/questionnaire/risposte_domande_random_quiz.csv")

    df_risposte_multichoice = pd.read_csv("../resources/questionnaire/risposte_multichoice_opzioni.csv")
    #df_questionnaires_random = df_questionnaires_random[df_questionnaires_random["QSTN_Id"] == 3231]
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
    df_questionnaires = df_questionnaires[(df_questionnaires["QSGR_CMNT_Id"].isin([1793,2863,2884,3005,3006,3011,1775,3014,3015,3016,3017]))]
    df_users = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_risposte_domande_non_random_quiz = pd.read_csv("../resources/questionnaire/risposte_domande_non_random.csv")
    df_risposte_multichoice = pd.read_csv("../resources/questionnaire/risposte_multichoice_opzioni.csv")
    df_domande = pd.read_csv("../resources/questionnaire/domande.csv")
    df_risposte_rating = pd.read_csv("../resources/questionnaire/risposte_rating_opzioni.csv")
    df_risposte_short_answer = pd.read_csv("../resources/questionnaire/risposte_short_answer.csv")
    #df_risposte_rating = df_risposte_rating[df_risposte_rating["RSRT_RSQS_Id"] == 290930]
    #df_questionnaires = df_questionnaires[df_questionnaires["IdMoodle"] == 376]
    #df_users = df_users[df_users["PRSN_id"] == 54337]
    questionnaire_import_responses_questionnaire(df_questionnaires, df_users, df_risposte_domande_non_random_quiz, df_risposte_multichoice, df_domande, df_risposte_rating, df_risposte_short_answer)


############################## STEP 1: Delete existing users
#delete_users()
############################## STEP 2: Create users
#create_users()
############################## STEP 3: Create Categories
#create_all_categories()
############################## STEP 4: Create Courses
#create_courses()

# PHASE 2

############################## STEP 5: Apply activities dependencies
#config_activities_dependencies()
############################## STEP 6: Config Blocks
#config_blocks()
############################## STEP  : Generate groups
#create_groups_in_courses()
############################## STEP 7: Generate enrolments list
#generate_enrolments_list()
############################## STEP 8: Enrol users to courses
#enrol_users_to_courses()
############################## STEP 9: Update activities completion for each user
#completion_status()


# PHASE 3

############################## STEP 10: Import scorm tracks
#importing_scorm_tracks()
############################## STEP 11: Import wiki
#adding_wikis()
############################## STEP : Import quiz responses
#import_quiz_responses()
############################## STEP : Import questionnaire responses
import_questionnaire_responses()
