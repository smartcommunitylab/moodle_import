import pandas as pd
from moodle_services import create_categories, core_course_create_courses, core_user_create_users, enrol_manual_enrol_users, core_user_delete_users
from moodle_services_post import create_blocks, activities_dependencies, activities_completion
from config import instance_params

instance = instance_params["instance"]
df_activities = pd.read_csv("../resources/courses/courses_activities.csv")
df_dependencies = pd.read_csv("../resources/courses/courses_activities_dep.csv")

def delete_users():
    '''
     Delete existing users
    '''
    df_users_instance = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    core_user_delete_users(df_users_instance["moodleUserId"])

def create_users():
    '''
    Create users
    '''
    df_users = pd.read_csv("../resources/users/users.csv")
    core_user_create_users(df_users)

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
    courses = pd.read_csv("../resources/courses/courses_pat_tsm.csv".format())
    sections = pd.read_csv("../resources/courses/courses_sections.csv")
    sections["Description"] = sections["Description"].fillna("")
    sub_sections = pd.read_csv("../resources/courses/courses_sections_sub.csv")
    df_diario = pd.read_csv("../resources/courses/diario.csv")
    df_questionnaires = pd.read_csv("../resources/questionnaire/questionnaires.csv")
    df_domande = pd.read_csv("../resources/questionnaire/domande.csv")
    df_domande_rating_options = pd.read_csv("../resources/questionnaire/domande_rating_options.csv")
    df_domande_rating_headers = pd.read_csv("../resources/questionnaire/domande_rating_headers.csv")

    df_domande_multichoice = pd.read_csv("../resources/questionnaire/domande_multichoice.csv")
    df_domande_multichoice_opzioni = pd.read_csv("../resources/questionnaire/domande_multichoice_opzioni.csv")
    df_pages = pd.read_csv("../resources/questionnaire/questionario_pages.csv")
    
    df_dropdown = pd.read_csv("../resources/questionnaire/domande_dropdown.csv")
    df_materiale = pd.read_csv("../resources/courses/materiale.csv")

    core_course_create_courses(courses, sections, sub_sections, df_diario, df_activities, df_questionnaires, \
                               df_domande, df_domande_rating_options, df_domande_rating_headers, \
                               df_domande_multichoice, df_domande_multichoice_opzioni, df_pages, df_dropdown, df_materiale)

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
    df_users = pd.read_csv("../resources/users/users.csv")
    df_iscrizioni = pd.read_csv('../../user_roles/LK_RUOLO_PERSONA_COMUNITA.csv')
    df_iscrizioni = df_iscrizioni[df_iscrizioni["RLPC_PRSN_id"].isin(df_users['PRSN_id'])]
    df_iscrizioni = df_iscrizioni[['RLPC_CMNT_id', 'RLPC_PRSN_id', 'RLPC_TPRL_id', 'RLPC_responsabile', 'RLPC_attivato', 'RLPC_abilitato']]

    df_courses_pat_moodle = pd.read_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance))
    df_com_iscriz = df_courses_pat_moodle.merge(df_iscrizioni, left_on="IdCommunity", right_on="RLPC_CMNT_id")

    df_user_moodle = pd.read_csv("../resources/users/users_{}.csv".format(instance))
    df_com_iscriz_user = df_com_iscriz.merge(df_user_moodle, left_on="RLPC_PRSN_id", right_on="PRSN_id")
    result_df = df_com_iscriz_user[["idCourseMoodle", "moodleUserId"]]
    result_df.to_csv("../resources/users/enrolments_{}.csv".format(instance), index=False)

def enrol_users_to_courses():
    '''
    Enrol users to courses
    '''
    df_user_course_enrolments = pd.read_csv("../resources/users/enrolments_{}.csv".format(instance), chunksize=200)
    for block in df_user_course_enrolments:
        enrol_manual_enrol_users(block)

  
# PHASE 1      
        
############################## STEP 1: Delete existing users
delete_users()
############################## STEP 2: Create users
create_users()
############################## STEP 3: Create Categories
create_all_categories()
############################## STEP 4: Create Courses
create_courses()

# PHASE 2

############################## STEP 5: Apply activities dependencies
config_activities_dependencies()
############################## STEP 6: Config Blocks
config_blocks()
############################## STEP 7: Generate enrolments list
generate_enrolments_list()
############################## STEP 8: Enrol users to courses
enrol_users_to_courses()
############################## STEP 9: Update activities completion for each user
completion_status()
