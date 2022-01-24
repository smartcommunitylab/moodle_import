import requests, json, os, sys, re, html
from pathlib import Path
import pandas as pd
import subprocess
from collections import namedtuple
from utils import clean_text, log_generator
from config import instance_params
from download import main
from bs4 import BeautifulSoup

MOODLE_UPLOAD_L_ = instance_params["materials"]

token = instance_params["token"]
moodle_url = instance_params["url"] + "?wstoken=" + token + "&moodlewsrestformat=json"
instance = instance_params["instance"]
moosh_sudo = ""
if instance == "prod":
    moosh_sudo = "sudo"
df_activities = pd.read_csv("../resources/courses/courses_activities.csv")
df_materiale = pd.read_csv("../resources/courses/materiale.csv")
df_questionnaires = pd.read_csv("../resources/questionnaire/questionnaires.csv")

############################################################## Categories & Courses Services ###################################################
def get_category(category_name):
    params = {
        'wstoken': token,
        'wsfunction': "core_course_get_categories",
        'moodlewsrestformat': "json",
    }
    # construct criteria in PHP array query string format
    filter = {"name": category_name}
    num_filters = 0
    for key, value in filter.items():
        params["criteria[{}][key]".format(num_filters)] = key
        params["criteria[{}][value]".format(num_filters)] = value
        num_filters += 1
    params["addsubcategories"] = 0
    res = requests.get(moodle_url, params)
    result = json.loads(res.content)
    return result[0]["id"] if len(result) > 0 else -1
    
    
def core_course_create_categories(category_name, parent):    
    functionname = "core_course_create_categories"
    categories = {}
    categories["categories[{}][name]".format(0)] = category_name
    categories["categories[{}][parent]".format(0)] = parent
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    res = requests.post(serverurl, data=categories)
    return res.content
    
def create_categories(dataframe):
    for row in dataframe.itertuples():
        parent = get_category(row[4])
        if parent == -1:
            parent = 0 # if parent does not exist then include it inside root category
        if get_category(row[3]) == -1:
            core_course_create_categories(row[3], parent)

def core_course_create_courses(dataframe, df_sections, df_subsections, df_diario, df_activities, \
                               df_domande, df_domande_rating_options, df_domande_rating_headers, df_domande_multichoice, df_domande_multichoice_opzioni, pages, \
                               df_dropdown):
    try:
        for row in dataframe.itertuples():
            print(row)
            sections = df_sections[(df_sections["IdCommunity"] == row[1]) & (df_sections["IdPath"] == row[5])].sort_values(by="DisplayOrder")
            subsections = df_subsections[df_subsections["Id_section"].isin(sections["Id"])].sort_values(by="DisplayOrder")
            path_activities = df_activities[(df_activities["IdCommunity"]  == row[1])]
            activities = df_activities[(df_activities["IdCommunity"]  == row[1]) & (df_activities["IdPath"] == row[5])]
            diario = df_diario[df_diario["EVNT_CMNT_id"] == row[1]]
            questionnaires = df_questionnaires[df_questionnaires["QSGR_CMNT_Id"] == row[1]]
            domande = df_domande[df_domande["QSGR_CMNT_Id"] == row[1]]
            materiale = df_materiale[df_materiale["FLDS_CMNT_id"] == row[1]]
            course_id = get_course(row.shortname)
            if course_id != -1:
                update_dataframe(dataframe, "shortname", row.shortname, "idCourseMoodle", int(course_id))
                populate_course_modules(activities, course_id, df_domande_multichoice, df_domande_multichoice_opzioni,
                                        df_domande_rating_headers, df_domande_rating_options, df_dropdown, diario,
                                        domande, materiale, pages, questionnaires, sections, subsections, path_activities)
            else:
                if len(sections) > 0:
                    numsections = len(sections) + len(subsections)
                elif len(diario)>0:
                    numsections = len(diario)
                else:
                    numsections = 1
                create_course(activities, dataframe, df_domande_multichoice, df_domande_multichoice_opzioni,
                              df_domande_rating_headers, df_domande_rating_options, df_dropdown, diario, domande,
                              materiale, numsections, pages, questionnaires, row, sections, subsections, path_activities)
    finally:
        if os.path.exists("../resources/courses/courses_pat_tsm_{}.csv".format(instance)):
            dataframe.to_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance), index=False, mode='a', header=False)
        else:
            dataframe.to_csv("../resources/courses/courses_pat_tsm_{}.csv".format(instance), index=False)


def create_course(activities, dataframe, df_domande_multichoice, df_domande_multichoice_opzioni,
                  df_domande_rating_headers, df_domande_rating_options, df_dropdown, diario, domande,
                  materiale, numsections, pages, questionnaires, row, sections, subsections, path_activities):
    functionname = "core_course_create_courses"
    courses = {}
    courses["courses[{}][fullname]".format(0)] = row[2]
    courses["courses[{}][shortname]".format(0)] = row.shortname
    courses["courses[{}][categoryid]".format(0)] = get_category(row[3])
    courses["courses[{}][numsections]".format(0)] = numsections
    courses["courses[{}][format]".format(0)] = "remuiformat"
    courses["courses[{}][courseformatoptions][0][name]".format(0)] = "remuicourseformat"
    courses["courses[{}][courseformatoptions][0][value]".format(0)] = "1"
    serverurl = moodle_url + '&wsfunction=' + functionname
    res = requests.post(serverurl, data=courses)
    moodle_courses = json.loads(res.content)
    print(moodle_courses)
    update_dataframe(dataframe, "shortname", row.shortname, "idCourseMoodle", int(moodle_courses[0]["id"]))
    for course in moodle_courses:
        populate_course_modules(activities, course["id"], df_domande_multichoice, df_domande_multichoice_opzioni,
                                df_domande_rating_headers, df_domande_rating_options, df_dropdown, diario,
                                domande, materiale, pages, questionnaires, sections, subsections, path_activities)


def populate_course_modules(activities, course_id, df_domande_multichoice, df_domande_multichoice_opzioni,
                            df_domande_rating_headers, df_domande_rating_options, df_dropdown, diario, domande,
                            materiale, pages, questionnaires, sections, subsections, path_activities):
    # Import once all questionnaires not yet created since we don't know the correct relation between the questionnaire and the course
    # make sure to exclude the questionnaires that are present in 'Percorso'
    try:
        if len(sections) > 0:
            # Percorso Formativo
            course_update_sections(course_id, sections, subsections, activities, questionnaires, \
                                   domande, df_domande_rating_options, df_domande_rating_headers, df_domande_multichoice,
                                   df_domande_multichoice_opzioni, pages, \
                                   df_dropdown, materiale)
        elif len(diario) > 0:
            # Diario Lezione
            questionnaires = questionnaires[(questionnaires["IdMoodle"] == 0) & (~questionnaires["QSTN_Id"].isin(path_activities["IdObjectLong"]))]
            populate_course_diario(course_id, diario, materiale, questionnaires, domande, df_domande_multichoice,
                                   df_domande_multichoice_opzioni, pages, \
                                   df_dropdown, df_domande_rating_options, df_domande_rating_headers)
        else:
            # populate courses of type <> Percorso/Diario
            questionnaires = questionnaires[(questionnaires["IdMoodle"] == 0) & (~questionnaires["QSTN_Id"].isin(activities["IdObjectLong"]))]
            add_course_modules_no_sections(course_id, 1, questionnaires, domande, df_domande_multichoice,
                                           df_domande_multichoice_opzioni, pages, \
                                           df_dropdown, df_domande_rating_options, df_domande_rating_headers, materiale)
    finally:
        df_activities.to_csv("../resources/courses/courses_activities.csv", index=False)
        df_materiale.to_csv("../resources/courses/materiale.csv", index=False)
        df_questionnaires.to_csv("../resources/questionnaire/questionnaires.csv", index=False)
        print("saving new data ..............")

def update_dataframe(dataframe, search_key, search_value, key_to_update, value_to_update):
    dataframe.loc[dataframe[search_key] == search_value, [key_to_update]] = value_to_update

def get_course(shortname):
    functionname = "core_course_get_courses_by_field"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    params = {"field" : "shortname", "value": shortname}
    res = requests.post(serverurl, params=params)
    result = json.loads(res.content)
    print(result)
    return result['courses'][0]["id"] if len(result['courses']) > 0 else -1
  
def core_course_update_courses(course_id, num_sections):    
    functionname = "core_course_update_courses"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    courses = {}
    courses["courses[{}][id]".format(0)] = course_id
    courses["courses[{}][numsections]".format(0)] = num_sections
    res = requests.post(serverurl, data=courses)
    result = json.loads(res.content)

def populate_course_diario(course_id, diario, materiale, questionnaires, domande_all, domande_multichoice, domande_opzioni, pages, df_dropdown, \
                           df_domande_rating_options, df_domande_rating_headers):
    row_mat = namedtuple('row_mat', ['customfield_duration_hours', 'customfield_duration_mins', 'Description'])
    r = row_mat(0, 0, '')
    embedded = []
    # Add Section name and description
    curr_id = 0
    for row in diario.itertuples():
        extract = clean_text(row.PREV_ProgrammaSvolto)
        programma_svolto = extract[1].replace('"',"'")
        embedded_material = extract[0]
        embedded.extend(embedded_material)
        subprocess.check_output(moosh_sudo + ' moosh -n section-config-set -s {} course {} name "{}"'.format(curr_id + 1, course_id, row.Section_Name), shell=True).decode("utf-8").strip()
        subprocess.check_output(moosh_sudo + ' moosh -n section-config-set -s {} course {} summary "{}"'.format(curr_id + 1, course_id,
                                                                                                                programma_svolto), shell=True).decode("utf-8").strip()
        # Add material embedded in the section's description
        for single_material in embedded_material:
            material = materiale[materiale["FLDS_id"] == int(single_material)].to_dict('records')
            module_id, module_type = define_material(course_id, material, r, curr_id + 1)
            update_dataframe(df_materiale, "FLDS_id", int(single_material), "IdMoodle", module_id)
        # Add Attendance activity inside this section
        attendance_id = mod_attendance_add_attendance(course_id, curr_id + 1, row.Section_Name)
        # Add attendance's session
        mod_attendance_add_session(attendance_id, row.start, row.duration)
        curr_id = curr_id + 1
    # Add all material of the community
    for material in materiale.itertuples():
        if str(material.FLDS_id) not in embedded:
            curr_material = materiale[materiale["FLDS_id"] == int(material.FLDS_id)].to_dict('records')
            define_material(course_id, curr_material, r, 0, stealth=1)
    # Add questionnaires
    questionnaires_builder(course_id, df_domande_rating_headers, df_domande_rating_options, df_dropdown,
                           domande_all, domande_multichoice, domande_opzioni, pages, questionnaires, curr_id, stealth=1)

def course_update_sections(course_id, sections, df_subsections, df_activities, questionnaires, domande, \
                           df_domande_rating_options, df_domande_rating_headers, df_domande_multichoice, df_domande_multichoice_opzioni, pages, \
                           df_dropdown, materiale):
    curr_id = 1
    row_mat = namedtuple('row_mat', ['customfield_duration_hours', 'customfield_duration_mins', 'Description'])
    r = row_mat(0, 0, '')
    for row in sections.itertuples():
        extract = clean_text(row.Description)
        description = extract[1].replace('"',"'")
        embedded_material = extract[0]

        subprocess.check_output(moosh_sudo + ' moosh -n section-config-set -s {} course {} name "{}"'.format(curr_id, course_id, row.Name), shell=True).decode("utf-8").strip()
        subprocess.check_output(moosh_sudo + ' moosh -n section-config-set -s {} course {} summary "{}"'.format(curr_id, course_id, description), shell=True).decode("utf-8").strip()
        curr_id += 1
        subsections = df_subsections[df_subsections["Id_section"] == row.Id].sort_values(by="DisplayOrder")
        # In case the activities reside inside the subsections
        if len(subsections) > 0:
            prev_order = 0
            for subrow in subsections.itertuples():
                extract = clean_text(subrow.Description)
                description = extract[1].replace('"',"'")
                embedded_material = extract[0]

                subprocess.check_output(moosh_sudo + ' moosh -n section-config-set -s {} course {} name "{}"'.format(curr_id, course_id, subrow.Name.replace('"','\\"')), shell=True).decode("utf-8").strip()
                subprocess.check_output(moosh_sudo + ' moosh -n section-config-set -s {} course {} summary "{}"'.format(curr_id, course_id, description), shell=True).decode("utf-8").strip()
                # add material embedded inside the current section description
                create_embedded_resources(course_id, curr_id, embedded_material, materiale, r)
                # Add activities inside section
                activities = df_activities[(df_activities["IdUnit"] == subrow.Id_section) & (df_activities["IdCommunity"] == subrow.IdCommunity) & (df_activities["IdPath"] == subrow.IdPath) & (df_activities["DisplayOrder"] < int(subrow.DisplayOrder)) & (df_activities["DisplayOrder"] > prev_order) ]
                add_course_modules(course_id, curr_id - 1, activities, questionnaires, domande, df_domande_multichoice, df_domande_multichoice_opzioni, pages, \
                                   df_dropdown, df_domande_rating_options, df_domande_rating_headers, materiale)
                prev_order = subrow.DisplayOrder
                curr_id += 1

            activities = df_activities[(df_activities["IdUnit"] == subrow.Id_section) & (df_activities["IdCommunity"] == subrow.IdCommunity) & (df_activities["IdPath"] == subrow.IdPath) & (df_activities["DisplayOrder"] > prev_order) ]
            add_course_modules(course_id, curr_id - 1, activities, questionnaires, domande, df_domande_multichoice, df_domande_multichoice_opzioni, pages, \
                               df_dropdown, df_domande_rating_options, df_domande_rating_headers, materiale)
        # In case the activities are part of the section(unit)
        else:
            activities = df_activities[(df_activities["IdUnit"] == row.Id) & (df_activities["IdCommunity"] == row.IdCommunity) & (df_activities["IdPath"] == row.IdPath)]
            add_course_modules(course_id, curr_id - 1, activities, questionnaires, domande, df_domande_multichoice, df_domande_multichoice_opzioni, pages, \
                               df_dropdown, df_domande_rating_options, df_domande_rating_headers, materiale)
            # add material embedded inside the current section description
            create_embedded_resources(course_id, curr_id - 1, embedded_material, materiale, r)
    # Consider adding only the questionnaires not inside the sections since they are already added
    #& (questionnaires["IdMoodle"] == 0)
    questionnaires = questionnaires[(~questionnaires["QSTN_Id"].isin(df_activities["IdObjectLong"])) ] #(questionnaires["QSTN_Tipo"] == 0) &
    questionnaires_builder(course_id, df_domande_rating_headers, df_domande_rating_options, df_dropdown,
                           domande, df_domande_multichoice, df_domande_multichoice_opzioni, pages, questionnaires, curr_id-1, stealth=1)

def create_embedded_resources(course_id, curr_id, embedded_material, materiale, r):
    for single_material in embedded_material:
        if single_material == "all":
            for mat in materiale.itertuples():
                material = materiale[materiale["FLDS_id"] == int(mat.FLDS_id)].to_dict('records')
                module_id, module_type = define_material(course_id, material, r, curr_id)
                update_dataframe(df_materiale, "FLDS_id", int(mat.FLDS_id), "IdMoodle", module_id)
        else:
            material = materiale[materiale["FLDS_id"] == int(single_material)].to_dict('records')
            module_id, module_type = define_material(course_id, material, r, curr_id)
            update_dataframe(df_materiale, "FLDS_id", int(single_material), "IdMoodle", module_id)


########################################################################## Activities Services ####################################################

def get_scorm_access_information(scormid):
    functionname = "mod_scorm_get_scorm_access_information"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    params = {"scormid" : scormid}
    res = requests.post(serverurl, data=params)
    return res.content

def get_scorms_by_courses():
    functionname = "mod_scorm_get_scorms_by_courses"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    courses ={}
    res = requests.post(serverurl, data=courses)
    return json.loads(res.content)

def files_upload(params):
    functionname = "core_files_upload"
    serverurl = moodle_url  + '&wsfunction=' + functionname    
    files = {'file': open('../requirements-pip.txt', 'rb')}
    res = requests.post(serverurl, params=params, files=files)
    print(json.loads(res.content))
    return res.content
      
def add_module_scorm(course_id, section_id, name, path, duration_hours=0, duration_min=0, intro="", stealth=0):
    visibleoncoursepage = int(not stealth)
    if not os.path.exists(path):
        output = subprocess.check_output(moosh_sudo + ' moosh -n activity-add -n "{}" --section {} -o " \
    --customfield_duration_hours={} --customfield_duration_mins={} --completion=2 --completionview=1 --completionscoredisabled=1 --completionstatusrequired=4 \
    --intro={} --hidebrowse=1 --visibleoncoursepage={}"  scorm {}'.format(name, section_id, duration_hours, duration_min, intro, visibleoncoursepage, course_id), shell=True).decode("utf-8").strip()
    else:
        output = subprocess.check_output(moosh_sudo + ' moosh -n activity-add -n "{}" --section {} -o "--packagefilepath={} \
        --customfield_duration_hours={} --customfield_duration_mins={} --completion=2 --completionview=1 --completionscoredisabled=1 --completionstatusrequired=4 \
        --intro={} --hidebrowse=1 --visibleoncoursepage={} " scorm {}'.format(name, section_id, path, duration_hours, duration_min, intro, visibleoncoursepage, course_id), shell=True).decode("utf-8").strip()
    return output
    
def add_module_resource(course_id, section_id, name, path, duration_hours=0, duration_min=0, intro="", stealth=0):
    functionname = "local_modcustomfields_add_resource"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    params ={"courseid" : int(course_id), "sectionid": section_id, \
             "resourcename": name, "path": path, "stealth": stealth, \
             'duration_hours': duration_hours, 'duration_min':duration_min}
    res = requests.post(serverurl, params=params, data={"intro": intro})
    print(res.content)
    res = json.loads(res.content)
    if 'exception' in res:
        log_generator(res)
        return 0
    return res["moduleid"]

def add_module_questionnaire(course_id, section_id, questionnaires, domande, df_domande_rating_options, df_domande_rating_headers, df_dropdown, domande_multichoice, domande_opzioni, stealth=0):
    res = {"moduleid":0}
    try:
        for questionnaire in questionnaires.itertuples():
            my_questions = domande[domande["QSTN_Id"] == questionnaire.QSTN_Id].sort_values(by="LKQD_NumeroDomanda")
            functionname = "local_modcustomfields_create_questionnaire"
            serverurl = moodle_url  + '&wsfunction=' + functionname
            quiz_intro = str(questionnaire.QSML_Descrizione)
            params = {"courseid" : course_id, "sectionid": section_id, \
                     "quizname": questionnaire.QSML_Nome, "quizintro": quiz_intro, "qperpage": questionnaire.QSTN_nQuestionsPerPage, \
                     'opendate': questionnaire.opendate, 'closedate':questionnaire.closedate, "stealth" : stealth}
            questions = {}
            for i, question in enumerate(my_questions.itertuples()):
                questions["questions[{}][tipo]".format(i)] = question.DMND_Tipo
                soup = BeautifulSoup(str(question.DMML_Testo), 'html.parser')
                questions["questions[{}][name]".format(i)] =  str(soup.get_text()) + '...'
                questions["questions[{}][description]".format(i)] = question.DMML_Testo
                questions["questions[{}][required]".format(i)] = question.LKQD_isObbligatorio
                questions["questions[{}][position]".format(i)] = question.LKQD_NumeroDomanda
                if question.DMND_Tipo == 4: # dropdown
                    questions["questions[{}][options]".format(i)] = df_dropdown[df_dropdown["DMML_Id"] == question.DMML_Id]
                elif question.DMND_Tipo == 3: # multichoice
                    questions["questions[{}][single]".format(i)] = domande_multichoice[domande_multichoice["DMMT_DMML_Id"] == question.DMML_Id].to_dict("records")[0]["single"]
                    opts = []
                    for opt in domande_opzioni[domande_opzioni["DMMT_DMML_Id"] == question.DMML_Id].sort_values(by="DMMO_NumeroOpzione")["answer_questionnaire"]:
                        opts.append(opt)
                    questions["questions[{}][max_responses]".format(i)] = int(domande_multichoice[domande_multichoice["DMMT_DMML_Id"] == question.DMML_Id].to_dict("records")[0]["DMMT_NumeroMaxRisposte"])
                    questions["questions[{}][options]".format(i)] = "@@".join(opts)
                elif question.DMND_Tipo == 1: # rating
                    headers = df_domande_rating_headers[df_domande_rating_headers["DMRT_DMML_Id"] == question.DMML_Id]
                    questions["questions[{}][precise]".format(i)] = headers.to_dict('records')[0]["DMRT_MostraND"] if len(headers) > 0 else 0
                    opts = []
                    for opt in df_domande_rating_options[df_domande_rating_options["DMRT_DMML_Id"] == question.DMML_Id].sort_values(by="DMRO_NumeroOpzione")["DMRO_TestoMin"]:
                        opts.append(opt)
                    questions["questions[{}][options]".format(i)] = '@@'.join(opts)
                    opts = []
                    for opt in headers.sort_values(by="DMRI_Indice")["DMRI_Testo"]:
                        opts.append(opt)
                    questions["questions[{}][headers]".format(i)] = '["' + '","'.join(opts) + '"]'
            if len(my_questions) == 0:
                return -1
            print("inside questionnaire" + str(questionnaire.QSTN_Id))
            res = requests.post(serverurl, params=params, data=questions)
            print(res.content)
            res = json.loads(res.content)
            if 'exception' in res:
                log_generator(res, questionnaire.QSTN_Id)
                res = {"moduleid":0}
    except Exception as err:
        print(err)
        log_generator(err, questionnaire.QSTN_Id)
        res = {"moduleid":0}
    finally:
        return res["moduleid"]

def add_module_quiz(course_id, section_id, questionnair, domande_all, domande_multichoice, domande_opzioni, pages, df_dropdown, restricted_by_activity_id=None, duration_hours=0, duration_min=0, stealth=0):
    res = {"moduleid":0}
    try:
        functionname = "local_modcustomfields_create_quiz"
        serverurl = moodle_url  + '&wsfunction=' + functionname
        params ={"courseid" : course_id, "sectionid": section_id, \
                 "quizname": questionnair[0]["QSML_Nome"], "quizintro": questionnair[0]["QSML_Descrizione"], "attempts": questionnair[0]["MaxAttempts"], \
                 "qperpage": questionnair[0]["QSTN_nQuestionsPerPage"], "timeopen": questionnair[0]["opendate"], "timeclose": questionnair[0]["closedate"], \
                 "israndom": questionnair[0]["israndom"], "maxquestions": questionnair[0]["maxquestions"], "restricted_by_activity_id":2, 'duration_hours': duration_hours, \
                 'duration_min':duration_min, 'pages': pages, 'stealth' : stealth}
        questions = {}
        for row in domande_all.itertuples():
            soup = BeautifulSoup(str(row.DMML_Testo), 'html.parser')
            questions["questions[{}][name]".format(row[0])] =  soup.get_text() + "..."
            questions["questions[{}][text]".format(row[0])] = row.DMML_Testo
            questions["questions[{}][tipo]".format(row[0])] = row.DMND_Tipo
            if row.DMND_Tipo == 4: # dropdown
                questions["questions[{}][answers]".format(row[0])] = df_dropdown[df_dropdown["DMML_Id"] == row.DMML_Id]
            elif row.DMND_Tipo == 3: # multichoice
                questions["questions[{}][single]".format(row[0])] = domande_multichoice[domande_multichoice["DMMT_DMML_Id"] == row.DMML_Id].to_dict("records")[0]["single"]
                opts = []
                for opt in domande_opzioni[domande_opzioni["DMMT_DMML_Id"] == row.DMML_Id].sort_values(by="DMMO_NumeroOpzione")["answer"]:
                    opts.append(opt)
                questions["questions[{}][answers]".format(row[0])] = "@@".join(opts)
        if len(domande_all) == 0:
            return -1
        print("inside quiz" + str(questionnair[0]["QSTN_Id"]))
        res = requests.post(serverurl, params=params, data=questions)
        print(res.content)
        res = json.loads(res.content)
        if 'exception' in res:
            log_generator(res, questionnair[0]["QSTN_Id"])
            res = {"moduleid":0}
    except Exception as err:
        print(err)
        log_generator(err, questionnair[0]["QSTN_Id"])
        res = {"moduleid":0}
    finally:
        return res["moduleid"]

def add_course_modules(course_id, section_id, activities, questionnaires, domande_all, domande_multichoice, domande_opzioni, pages, df_dropdown, \
                       df_domande_rating_options, df_domande_rating_headers, materiale):
    module_id = 0
    module_type = ""
    for row in activities.itertuples():
        if row.CodeModule == "SRVMATER":
            material = materiale[materiale["FLDS_id"] == row.IdObjectLong].to_dict('records')
            module_id, module_type = define_material(course_id, material, row, section_id)
            update_dataframe(df_materiale, "FLDS_id", row.IdObjectLong, "IdMoodle", module_id)
        elif row.CodeModule == "SRVQUST":
            questionnair = questionnaires[questionnaires["QSTN_Id"] == row.IdObjectLong].to_dict('records')
            doman_all = domande_all[domande_all["QSTN_Id"] == row.IdObjectLong].sort_values(by="LKQD_NumeroDomanda")
            pag = "@@".join(pages[(pages["QSML_QSTN_Id"] == row.IdObjectLong) & (pages["QSPG_NomePagina"].notna())]["QSPG_NomePagina"].to_dict().values())
            if len(questionnair) > 0 and len(doman_all) > 0:
                if questionnair[0]["QSTN_Tipo"] == 7 or questionnair[0]["QSTN_Tipo"] == 1:
                    module_type = "quiz"
                    module_id = add_module_quiz(course_id, section_id, questionnair, doman_all, domande_multichoice, domande_opzioni, pag, df_dropdown, duration_hours=row.customfield_duration_hours, duration_min=row.customfield_duration_mins)
                elif questionnair[0]["QSTN_Tipo"] == 0:
                    module_type = "questionnaire"
                    questionnair = questionnaires[questionnaires["QSTN_Id"] == row.IdObjectLong]
                    module_id = add_module_questionnaire(course_id, section_id, questionnair, doman_all, df_domande_rating_options, df_domande_rating_headers, df_dropdown, domande_multichoice, domande_opzioni)
                update_dataframe(df_questionnaires, "QSTN_Id", row.IdObjectLong, "IdMoodle", module_id)
        # update df
        update_dataframe(df_activities, "Id", row.Id, "IdMoodle", module_id)
        update_dataframe(df_activities, "Id", row.Id, "moduleMoodle", module_type)


def add_course_modules_no_sections(course_id, section_id, questionnaires, domande_all, domande_multichoice, domande_opzioni, pages, df_dropdown, \
                                   df_domande_rating_options, df_domande_rating_headers, materiale):
    row_mat = namedtuple('row_mat', ['customfield_duration_hours', 'customfield_duration_mins', 'Description'])
    row = row_mat(0, 0, '')
    if len(materiale) > 0 :
        for material in materiale.itertuples():
            my_material = materiale[materiale["FLDS_id"] == material.FLDS_id].to_dict('records')
            module_id, module_type = define_material(course_id, my_material, row, section_id)
            update_dataframe(df_materiale, "FLDS_id", material.FLDS_id, "IdMoodle", module_id)
    questionnaires_builder(course_id, df_domande_rating_headers, df_domande_rating_options, df_dropdown,
                           domande_all, domande_multichoice, domande_opzioni, pages, questionnaires, section_id, stealth=1)


def questionnaires_builder(course_id, df_domande_rating_headers, df_domande_rating_options, df_dropdown, domande_all,
                           domande_multichoice, domande_opzioni, pages, questionnaires, section_id, stealth=0):
    row_mat = namedtuple('row_mat', ['customfield_duration_hours', 'customfield_duration_mins', 'Description'])
    row = row_mat(0, 0, '')
    module_id = 0
    if len(questionnaires) > 0:
        for questionnair in questionnaires.itertuples():
            doman_all = domande_all[domande_all["QSTN_Id"] == questionnair.QSTN_Id].sort_values(by="LKQD_NumeroDomanda")
            pag = "@@".join(pages[(pages["QSML_QSTN_Id"] == questionnair.QSTN_Id) & (pages["QSPG_NomePagina"].notna())][
                                "QSPG_NomePagina"].to_dict().values())
            if len(questionnair) > 0 and len(doman_all) > 0 :
                if questionnair.QSTN_Tipo == 7 or questionnair.QSTN_Tipo == 1:
                    questionnaire = questionnaires[questionnaires["QSTN_Id"] == questionnair.QSTN_Id].to_dict('records')
                    module_id = add_module_quiz(course_id, section_id, questionnaire, doman_all, domande_multichoice,
                                    domande_opzioni, pag, df_dropdown, duration_hours=row.customfield_duration_hours,
                                    duration_min=row.customfield_duration_mins, stealth=stealth)
                elif questionnair.QSTN_Tipo == 0:
                    questionnaire = questionnaires[questionnaires["QSTN_Id"] == questionnair.QSTN_Id]
                    module_id = add_module_questionnaire(course_id, section_id, questionnaire, doman_all, df_domande_rating_options,
                                             df_domande_rating_headers, df_dropdown, domande_multichoice,
                                             domande_opzioni, stealth=stealth)
                update_dataframe(df_questionnaires, "QSTN_Id", questionnair.QSTN_Id, "IdMoodle", module_id)


def define_material(course_id, material, row, section_id, stealth=0, service=""):
    module_id = 0
    module_type = ""
    if len(material) > 0:
        # download resource files for activities
        #tag_course(course_id, 6)
        #if not os.path.exists(MOODLE_UPLOAD_L_ + "/" + str(int(material[0]["FLDS_CMNT_id"]))):
        #    main(int(material[0]["FLDS_CMNT_id"]), MOODLE_UPLOAD_L_)

        is_scorm = int(material[0]["FLDS_isSCORM"])
        path = "{}/{}/{}.stored".format(MOODLE_UPLOAD_L_, int(material[0]["FLDS_CMNT_id"]), material[0]["FLDS_GUID"])
        module_type = "scorm" if is_scorm == 1 else "resource"
        if int(material[0]["IdMoodle"]) != 0 and service == "wiki":
            return int(material[0]["IdMoodle"]), module_type

        if is_scorm == 1:
            module_id = add_module_scorm(course_id, section_id, material[0]["FLDS_nome"],
                             path,
                             duration_hours=row.customfield_duration_hours,
                             duration_min=row.customfield_duration_mins,
                             intro=row.Description,
                             stealth=stealth)
        else:
            module_id = add_module_resource(course_id, section_id, material[0]["FLDS_nome"],
                                         path,
                                         duration_hours=row.customfield_duration_hours,
                                         duration_min=row.customfield_duration_mins,
                                         intro=row.Description,
                                         stealth=stealth)
    return module_id, module_type


def mod_attendance_add_attendance(course_id, sectionid, attendance_name):
    functionname = "mod_attendance_add_attendance"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    params ={"courseid" : course_id, "name": attendance_name, "sectionid": sectionid}
    res = requests.post(serverurl, params=params)
    result = json.loads(res.content)
    print(result)
    return result["attendanceid"]

def mod_attendance_add_session(attendanceid, sessiontime, duration):
    functionname = "mod_attendance_add_session"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    params ={"attendanceid" : attendanceid, "sessiontime": int(sessiontime), "duration": int(duration)}
    res = requests.post(serverurl, params=params)
    print(res.content)
    return json.loads(res.content)

def mod_resource_view_resource(resourceid):
    functionname = "mod_resource_view_resource"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    params ={"resourceid" : resourceid}
    res = requests.post(serverurl, params=params)
    print(res.content)
    return json.loads(res.content)

def core_course_get_course_module(course_module_id):
    functionname = "core_course_get_course_module"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    params ={"cmid" : course_module_id}
    res = requests.post(serverurl, params=params)
    print(res.content)
    return json.loads(res.content)

def add_block(category_id):
    output = subprocess.check_output(moosh_sudo + " moosh -n block-add categorycourses {}[all] progress course-view-* content 0".format(category_id))

############################################################### Users Services ####################################################################

def core_user_create_users(dataframe):
    functionname = "core_user_create_users"   
    serverurl = moodle_url  + '&wsfunction=' + functionname
    users = {}
    for row in dataframe.itertuples():
        print(row)
        users["users[{}][username]".format(row[0])] = str(row.username) #username MUST be lowercase
        users["users[{}][firstname]".format(row[0])] = str(row.firstname)
        users["users[{}][lastname]".format(row[0])] = str(row.lastname)
        users["users[{}][city]".format(row[0])] = str(row.city)
        #users["users[{}][phone1]".format(row[0])] = row[7]
        #users["users[{}][phone2]".format(row[0])] = row[8]
        users["users[{}][email]".format(row[0])] = str(row.email)
        users["users[{}][password]".format(row[0])] = "Password1."
        users["users[{}][customfields][0][type]".format(row[0])] = "codice_fiscale"
        users["users[{}][customfields][0][value]".format(row[0])] = str(row.codice_fiscale)
    res = requests.post(serverurl, data=users)
    created_users = json.loads(res.content)
    df_created_users = pd.DataFrame(created_users)
    df_created_users = df_created_users.rename(columns={"username":"usernameMoodle"})
    dataframe["username"] = dataframe["username"].astype('str')
    result = dataframe.merge(df_created_users, left_on="username", right_on="usernameMoodle")
    result["moodleUserId"] = result["id"]
    if os.path.exists("../resources/users/users_{}.csv".format(instance)):
        result.to_csv("../resources/users/users_{}.csv".format(instance), index=False, mode='a', header=False)
    else:
        result.to_csv("../resources/users/users_{}.csv".format(instance), index=False)
    return created_users

def core_user_delete_users(user_list):
    functionname = "core_user_delete_users"   
    serverurl = moodle_url  + '&wsfunction=' + functionname
    userids = {}
    for key, val in enumerate(user_list):
        userids["userids[{}]".format(key)] = int(val)
    res = requests.post(serverurl, data=userids)
    return json.loads(res.content)

def core_user_get_users_by_field(username):
    functionname = "core_user_get_users_by_field"   
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    params = {"field" : "username"}
    values = {}
    values["values[0]"] = username
    res = requests.post(serverurl, params=params, data=values)
    return json.loads(res.content)

def enrol_manual_enrol_users(dataframe):
    functionname = "enrol_manual_enrol_users"
    serverurl = moodle_url  + '&wsfunction=' + functionname 
    enrolments = {}
    for row in dataframe.itertuples():
        if int(row.roleId) != 0:
            print(row)
            enrolments["enrolments[{}][courseid]".format(row[0])] = int(row.idCourseMoodle)
            enrolments["enrolments[{}][roleid]".format(row[0])] = int(row.roleId)
            enrolments["enrolments[{}][userid]".format(row[0])] = int(row.moodleUserId)
    res = requests.post(serverurl, data=enrolments)
    print(res.content)
    return res.content

def add_group_members(dataframe):
    functionname = "core_group_add_group_members"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    dataframe = dataframe[dataframe["TPRL_nome"].str.contains("edizione")]
    if len(dataframe) == 0 :
        return
    members = {}
    for iter, row in enumerate(dataframe.itertuples()):
        print(row)
        number = re.findall('[0-9]+', row.TPRL_nome)
        group_id = "{}{}".format(int(row.idCourseMoodle), number[0])
        members["members[{}][groupid]".format(iter)] = int(group_id)
        members["members[{}][userid]".format(iter)] = int(row.moodleUserId)
    res = requests.post(serverurl, data=members)
    return json.loads(res.content)

def create_groups(df_courses):
    df_courses = df_courses[df_courses["idCourseMoodle"].notna()]
    functionname = "core_group_create_groups"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    groups = {}
    iter = 0
    for course in df_courses.itertuples():
        print(int(course.idCourseMoodle))
        for number in range(1,26):
            group_name = "Partecipante {} edizione".format(number)
            group_id = "{}{}".format(int(course.idCourseMoodle), number)
            groups["groups[{}][idnumber]".format(iter)] = int(group_id)
            groups["groups[{}][name]".format(iter)] = group_name
            groups["groups[{}][description]".format(iter)] = group_name
            groups["groups[{}][courseid]".format(iter)] = int(course.idCourseMoodle)
            iter = iter + 1
    res = requests.post(serverurl, data=groups)
    ret = json.loads(res.content)
    print(ret)
    return ret

def core_cohort_create_cohorts(df_cohorts, courses):
    df_cohorts = df_cohorts.merge(courses[["IdCommunity", "idCourseMoodle"]], left_on="RLPC_CMNT_id", right_on="IdCommunity")
    functionname = "core_cohort_create_cohorts"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    cohorts = {}
    for row in df_cohorts.itertuples():
        print(row)
        cohorts["cohorts[{}][categorytype][type]".format(row[0])] = "id"
        cohorts["cohorts[{}][categorytype][value]".format(row[0])] = "2723"
        cohorts["cohorts[{}][name]".format(row[0])] = "Partecipante {}".format(row[0]) #@str(row.TPRL_nome)
        cohorts["cohorts[{}][idnumber]".format(row[0])] = str(row.name)
    print(cohorts)
    res = requests.post(serverurl, data=cohorts)
    print(res.content)
    return json.loads(res.content)

########################################################### Tag Courses

def tag_course(course_id, tag_id):
    functionname = "local_modcustomfields_tag_course"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"courseid" : int(course_id), "tagid" : int(tag_id)}
    res = requests.post(serverurl, params=params)
    print(res.content)
    return json.loads(res.content)
