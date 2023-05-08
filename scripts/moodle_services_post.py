import requests, json, os
import pandas as pd
from bs4 import BeautifulSoup
from isodate import parse_duration
from collections import namedtuple
from config import instance_params
from moodle_services import define_material, tag_course
from utils import extract_file_id, log_generator, generate_activity_link, ENDPOINT_MODULE, SCORM_PLAYER, SCORM_PLAYER_OLD, FILE_REPO

MOODLE_UPLOAD_L_ = instance_params["materials"]
token = instance_params["token"]
moodle_url = instance_params["url"] + "?wstoken=" + token + "&moodlewsrestformat=json"
instance = instance_params["instance"]

def create_blocks(df_courses):
    for course in df_courses.itertuples():
        functionname = "local_modcustomfields_create_progress_block"
        serverurl = moodle_url  + '&wsfunction=' + functionname
        params ={"courseid" : int(course.idCourseMoodle)}
        res = requests.post(serverurl, params=params)
        print(res.content)
        result = json.loads(res.content)
########################################################################## Stivities services

def activities_dependencies(df_activities, df_dependencies):
    functionname = "local_modcustomfields_update_activity_dependency"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    activities = df_activities[(df_activities["IdMoodle"].notna()) & (df_activities["IdMoodle"] != 0)]
    for dependency in df_dependencies.itertuples():
        print(dependency)
        source_activity_id = dependency.IdSource
        target_activity_id = dependency.IdDestination

        moodle_source = activities[activities["IdActivity"] == source_activity_id].to_dict('records')
        moodle_target = activities[activities["IdActivity"] == target_activity_id].to_dict('records')
        print(moodle_source)
        print(moodle_target)
        if len(moodle_source) > 0 and len(moodle_target) > 0:
            moodle_source_activity_id = moodle_source[0]["IdMoodle"]
            moodle_source_activity_module = moodle_source[0]["moduleMoodle"]
            moodle_target_activity_id = moodle_target[0]["IdMoodle"]
            moodle_target_activity_module = moodle_target[0]["moduleMoodle"]
            if moodle_source_activity_id is not None and moodle_target_activity_id is not None and \
               pd.notna(moodle_source_activity_id) and pd.notna(moodle_target_activity_id) and \
               moodle_source_activity_id != '' and moodle_target_activity_id != '':
                params = {"sourceactivity" : int(moodle_source_activity_id), "sourcemodule" : moodle_source_activity_module, \
                         "targetactivity" : int(moodle_target_activity_id), "targetmodule" : moodle_target_activity_module}
                print(params)
                requests.post(serverurl, params=params)

def activities_completion(df_activities, df_users, df_status):
    df_activities = df_activities[(df_activities["IdMoodle"].notna()) & (df_activities["IdMoodle"] != 0)]
    print(df_activities)
    for activity in df_activities.itertuples():
        moodle_activity_id = activity.IdMoodle
        module_name = activity.moduleMoodle
        l3_activity_id = activity.Id # Id of the subactivity
        # Find the proper course module id
        if moodle_activity_id != 0:
            cmid = get_course_module_by_instance(module_name, moodle_activity_id)
            if cmid == 0:
                continue
            # Filter completion status of the activity for all users
            status_of_activity = df_status[(df_status["IdSubActivity"] == l3_activity_id)]
            status_of_activity = status_of_activity.merge(df_users, left_on="IdPerson", right_on="PRSN_id")
            print(status_of_activity)
            for usage in status_of_activity.itertuples():
                user_moodle = usage.moodleUserId
                activities_completion_request(user_moodle, cmid)

def activities_completion_request(userid, cmid, time=None):
    functionname = "core_completion_override_activity_completion_status"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params ={"userid":int(userid), "cmid" : int(cmid), "newstate": 1}
    res = requests.post(serverurl, params=params)
    print(res.content)
    result = json.loads(res.content)

def get_course_module_by_instance(module, instance):
    try:
        functionname = "core_course_get_course_module_by_instance"
        serverurl = moodle_url  + '&wsfunction=' + functionname
        params ={"module" : module, "instance" : int(instance)}
        res = requests.post(serverurl, params=params)
        result = json.loads(res.content)
        print(result)
        return result["cm"]["id"]
    except Exception as err:
        print("get_course_module_by_instance {} {}: {}".format(module, instance, err))
        log_generator(err, "get_course_module_by_instance {} {}".format(module, instance))
        return 0

################################################################################### SCORM Services

def import_scorm_tracks(df_file, df_activities, df_scorm_tracks, df_users):
    activities = df_activities[(df_activities["moduleMoodle"] == "scorm")]
    #print(activities)
    for activity in activities.itertuples():
        id_instance = activity.IdMoodle
        id_materiale = activity.IdObjectLong
        file = df_file[df_file["FLDS_id"] == id_materiale].to_dict('records')
        if len(file) > 0:
            file_guid = file[0]["FLDS_GUID"]
            #print(df_scorm_tracks[(df_scorm_tracks["fileguid"] == file_guid)][["fileguid", "ACTIVITY_ID"]])
            # Find Moodle sco activities inside each file scorm
            moodle_scoes = mod_scorm_get_scorm_scoes(id_instance)
            #print(moodle_scoes)
            for sco in moodle_scoes["scoes"]:
                if sco["scormtype"] == "sco":
                    # Find L3 sco activities
                    #print(file_guid , sco["identifier"])
                    scorm_tracks = df_scorm_tracks[(df_scorm_tracks["fileguid"] == file_guid) & (df_scorm_tracks["ACTIVITY_ID"] == sco["identifier"])]
                    #.encode('utf-8').strip()
                    scorm_users = df_users[df_users["PRSN_id"].isin(scorm_tracks["LEARNER_ID"])]
                    for user  in scorm_users.itertuples():
                        scorm_tracks_users = scorm_tracks[scorm_tracks["LEARNER_ID"] == user.PRSN_id] #.merge(df_users, left_on="LEARNER_ID", right_on="PRSN_id")
                        #print(scorm_tracks_users)
                        for index, track in enumerate(scorm_tracks_users.itertuples()):
                            xml = track.XML
                            user_id = user.moodleUserId
                            soup = BeautifulSoup(xml, 'html.parser')
                            tracks = {
                                "x.start.time" : int(track.time),
                                "cmi.core.lesson_status" : soup.lessonstatus.text if soup.lessonstatus is not None else "",
                                "cmi.success_status" : soup.successstatus.text if soup.successstatus is not None else "",
                                "cmi.completion_status" : soup.completionstatus.text if soup.completionstatus is not None else "",
                                "cmi.core.exit" : soup.exit.text if soup.exit is not None else "",
                                "cmi.suspend_data" : soup.suspenddata.text if soup.suspenddata is not None else "",
                                "cmi.core.total_time" : str(parse_duration(soup.totaltime.text)) if soup.totaltime is not None else "",
                                "cmi.core.session_time" : str(parse_duration(soup.sessiontime.text)) if soup.sessiontime is not None else "",
                                "cmi.core.score.raw" : soup.score.text if soup.score is not None else ""
                            }
                            insert_scorm_tracks(sco["id"], index + 1, user_id, int(track.time), tracks)

def insert_scorm_tracks(scoid, attempt, user_id, time, tracks):
    functionname = "local_modcustomfields_insert_scorm_tracks"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    all_tracks = {}
    iter = 0
    for key, val in tracks.items():
        all_tracks["tracks[{}][element]".format(iter)] = key
        all_tracks["tracks[{}][value]".format(iter)] = val
        iter = iter + 1
    params  = {"scoid" : int(scoid), "attempt" : attempt, "userid" : int(user_id), "time" : time}
    print(params)
    res = requests.post(serverurl, params=params, data=all_tracks)
    print(res.content)
    return json.loads(res.content)

def mod_scorm_get_scorm_scoes(scorm_id):
    functionname = "mod_scorm_get_scorm_scoes"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"scormid" : int(scorm_id)}
    res = requests.post(serverurl, params=params)
    return json.loads(res.content)

def tag_courses(df_courses):
    for course in df_courses.itertuples():
        path = "{}/{}".format(MOODLE_UPLOAD_L_, int(course.idCourseMoodle))
        print(path)
        if os.path.exists(path):
            tag_course(course.idCourseMoodle, 6)

################################################################################### WIKI services

def add_wiki(course_id, name, desc, content, user_id):
    functionname = "local_modcustomfields_add_wiki"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"courseid" : int(course_id), "name" : name, "desc" : desc, "content" : content, "userid" : user_id}
    print(params)
    res = requests.post(serverurl, params=params)
    print(res.content)
    return json.loads(res.content)

def add_sub_wiki(course_id, wiki_id, user_id, name, content, time):
    functionname = "local_modcustomfields_add_subwiki"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"courseid" : int(course_id), "wikiid" : wiki_id, "title" : name, \
               "userid" : user_id, "time" : time}
    res = requests.post(serverurl, params=params, data={"content" : content})
    print(res.content)
    return json.loads(res.content)

def add_sub_wiki_history(wiki_id, subwikiid, title, subwiki_history):
    functionname = "local_modcustomfields_add_sub_wiki_history"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"wikiid" : wiki_id, "subwikiid" : subwikiid, "title" : title}
    history = {}
    for iter, version in enumerate(subwiki_history):
        history["history[{}][content]".format(iter)] = version["content"]
        history["history[{}][time]".format(iter)] = version["time"]
        history["history[{}][userid]".format(iter)] = version["userid"]
    res = requests.post(serverurl, params=params, data=history)
    print(res.content)
    return json.loads(res.content)

def add_wikis(df_courses, df_wiki, df_wiki_sections, df_topics, df_topics_history, df_users, df_materiale):
    df_wiki = df_wiki.merge(df_courses, left_on="WIKI_CMNT_id", right_on="IdCommunity")
    for wiki in df_wiki.itertuples():
        print(wiki.WIKI_id)
        sections = df_wiki_sections[df_wiki_sections["WKSZ_WIKI_id"] == wiki.WIKI_id].sort_values(by="WKSZ_nome")
        sections = sections.merge(df_users, left_on="WKSZ_PRSN_id", right_on="PRSN_id")
        sections_titles = ""
        user_id = 2
        wiki_name = wiki.WIKI_nome
        if len(sections) > 0:
            sections_titles = "</br></br>".join(sections["WKSZ_nome"].apply(lambda x: "<h1>[[" + x + "]]</h1>").tolist())
            if len(sections.loc[sections["WKSZ_isDefault"] == 1, :]) > 0:
                user_id = sections.loc[sections["WKSZ_isDefault"] == 1, :].to_dict('records')[0]["moodleUserId"]
                wiki_name = sections.loc[sections["WKSZ_isDefault"] == 1, :].to_dict('records')[0]["WKSZ_nome"]
            else:
                user_id = sections.iloc[0, :]["moodleUserId"]
                wiki_name = sections.iloc[0, :]["WKSZ_nome"]
        # create wiki
        wiki_id = add_wiki(wiki.idCourseMoodle, wiki.WIKI_nome, wiki_name, sections_titles, user_id)["id"]
        for section in sections.itertuples():
            topics = df_topics[df_topics["WKTP_WKSZ_id"] == section.WKSZ_id].sort_values(by="WKTP_dataInserimento")
            topics = topics.merge(df_users, left_on="WKTP_PRSN_id", right_on="PRSN_id")
            all_topics = str(section.WKSZ_Descrizione) +  "</br></br>".join(topics["WKTP_nome"].apply(lambda x: "<h1>[[" + x + "]]</h1>").tolist())
            add_sub_wiki(wiki.idCourseMoodle, wiki_id, section.moodleUserId, section.WKSZ_nome, all_topics, section.WKSZ_dataInserimento)
            add_topics(df_materiale, df_topics_history, topics, df_users, wiki.idCourseMoodle, wiki_id)

def add_topics(df_materiale, df_topics_history, topics, df_users, idCourseMoodle, wiki_id):
    row_mat = namedtuple('row_mat', ['customfield_duration_hours', 'customfield_duration_mins', 'Description'])
    row = row_mat(0, 0, '')
    for topic in topics.itertuples():
        topic_hist = df_topics_history[df_topics_history["WKTH_WKTP_id"] == topic.WKTP_id]
        topic_hist = topic_hist.merge(df_users, left_on="WKTH_PRSN_id", right_on="PRSN_id").sort_values(by="WKTH_dataModifica")
        subwiki_history = []
        topic_text = ""
        if len(topic_hist) == 0:
            # Create material embedded in the topic's description and replace the L3 links with the newly created modules
            extract = replace_links_wiki(topic.WKTP_contenuto, df_materiale, idCourseMoodle, row)
            topic_text = extract
        for iter, top in enumerate(topic_hist.itertuples()):
            topic_text = topic.WKTP_contenuto
            if iter == len(topic_hist) - 1:
                # Create material embedded in the topic's description and replace the L3 links with the newly created modules
                extract = replace_links_wiki(topic.WKTP_contenuto, df_materiale, idCourseMoodle, row)
                topic_text = extract
            subwiki_history.append({"content": topic_text, "time": top.WKTH_dataModifica, "userid":top.moodleUserId})
        subwikiid = add_sub_wiki(idCourseMoodle, wiki_id, topic.moodleUserId, topic.WKTP_nome, topic_text, topic.WKTP_dataInserimento)
        add_sub_wiki_history(wiki_id, subwikiid["id"], subwikiid["title"], subwiki_history)

def replace_links_wiki(text, df_materiale, idCourseMoodle, row):
    new_text = ""
    resources = {}
    soup = BeautifulSoup(str(text), "html.parser")
    try:
        for link in soup.find_all('a'):
            if link is not None:
                href = link.get('href')
                new_text = extract_resource(href, df_materiale, idCourseMoodle, row, resources)
                if ENDPOINT_MODULE in href and new_text != "":
                    link['href'] = link['href'].replace(href, new_text)
    except Exception as err:
        log_generator("Error Message inside replace_links_wiki:  %s ", err)
        return new_text
    return str(soup)

def extract_resource(href, df_materiale, idCourseMoodle, row, resources):
    link = ""
    file_id = 0
    if ENDPOINT_MODULE in href:
        parts = href.split("?")
        if SCORM_PLAYER in parts[0] :
            file_id = extract_file_id(parts[1], "fileId")
        elif SCORM_PLAYER_OLD in parts[0]:
            file_id = extract_file_id(parts[1], "FileID")
        elif FILE_REPO in parts[0]:
            file_id = extract_file_id(parts[1], "FileID")
        if file_id not in resources.keys():
            material = df_materiale[df_materiale["FLDS_id"] == int(file_id)].to_dict('records')
            module_id, module_type = define_material(idCourseMoodle, material, row, 0, stealth=1, service="wiki")
            cm_id = get_course_module_by_instance(module_type, module_id)
            link = generate_activity_link(cm_id, module_type)
            resources[file_id] = link
        else:
            link = resources[file_id]
    return link

#################################################################################### QUIZ RESPONSES services

def questionnaire_import_responses_quiz(df_questionnaire, df_users, df_risposte_domande_random_quiz, df_risposte_multichoice):
    df_quiz_activities = df_questionnaire[df_questionnaire["IdMoodle"] != 0]
    #df_users = df_users[df_users["PRSN_id"] == 53611]
    for quiz in df_quiz_activities.itertuples():
        domande = df_risposte_domande_random_quiz[df_risposte_domande_random_quiz["RSQS_QSTN_Id"] == quiz.QSTN_Id]
        log_generator("Importing responses of quiz: {}, id module in Moodle: {}, nr domande: {}".format(quiz.QSTN_Id, quiz.IdMoodle, len(domande)))
        if len(domande) > 0:
            users = df_users[df_users["PRSN_id"].isin(domande["RSQS_PRSN_Id"])].drop_duplicates(subset=["PRSN_id"])
            for user in users.itertuples():
                domande_of_user = domande[domande["RSQS_PRSN_Id"] == user.PRSN_id]
                print(domande_of_user)
                if len(domande_of_user) > 0:
                    # Start new attempt by forcing questions
                    print(int(user.moodleUserId))
                    attempt = mod_quiz_start_attempt(quiz.IdMoodle, int(user.moodleUserId), domande_of_user, domande_of_user.to_dict('records')[0]["time_start"])
                    # Populate the responses of the questions
                    answers = df_risposte_multichoice.merge(domande_of_user, left_on=["RSOM_RSQS_Id", "DMMT_DMML_Id"], right_on=["RSQS_Id", "DMML_Id"])
                    process_quiz_attempt(attempt["id"], attempt["uniqueid"], answers, domande_of_user.to_dict('records')[0]["time_end"])

def save_quiz_attempt(attempt_id):
    functionname = "mod_quiz_save_attempt"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"attemptid" : attempt_id}
    versions = {}
    versions["data[{}][name]".format(0)] = "slot"
    versions["data[{}][value]".format(0)] = "1"
    versions["data[{}][name]".format(1)] = "questionusageid"
    versions["data[{}][value]".format(1)] = "71"
    versions["data[{}][name]".format(2)] = "1_answer"
    versions["data[{}][value]".format(2)] = "0"
    versions["data[{}][name]".format(3)] = "2_answer"
    versions["data[{}][value]".format(3)] = "2"
    versions["data[{}][name]".format(4)] = "3_answer"
    versions["data[{}][value]".format(4)] = "3"
    res = requests.post(serverurl, params=params, data=versions)
    print(res.content)
    return json.loads(res.content)

def process_quiz_attempt(attempt_id, attempt_unique_id, df_answers, time_end):
    functionname = "mod_quiz_process_attempt"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"attemptid" : attempt_id, "finishattempt":"1", "time_end" : time_end}
    data = {}
    iter = 0
    for answer in df_answers.itertuples():
        data["data[{}][name]".format(iter)] = "q{}:{}_answer".format(attempt_unique_id, answer.LKQD_NumeroDomanda)
        data["data[{}][value]".format(iter)] = "{}".format(int(answer.DMMO_NumeroOpzione) - 1)
        iter = iter + 1
        data["data[{}][name]".format(iter)] = "q{}:{}_:sequencecheck".format(attempt_unique_id, answer.LKQD_NumeroDomanda)
        data["data[{}][value]".format(iter)] = "1"
        iter = iter + 1
    res = requests.post(serverurl, params=params, data=data)
    print(res.content)
    res = json.loads(res.content)
    return res

def mod_quiz_start_attempt(quizid, user_id, questions_list, time_start):
    functionname = "mod_quiz_start_attempt"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"quizid" : int(quizid), "userid" : user_id, "time_start" : time_start}
    forcequestions = {}
    for i, question in enumerate(questions_list.itertuples()):
        soup = BeautifulSoup(str(question.DMML_Testo)[:20], 'html.parser')
        forcequestions["forcequestions[{}][slot]".format(i)] = int(question.LKQD_NumeroDomanda)
        forcequestions["forcequestions[{}][value]".format(i)] = soup.get_text(strip=True) + "_" + str(question.DMML_Id)
    print(forcequestions)
    res = requests.post(serverurl, params=params, data=forcequestions)
    print(res.content)
    res = json.loads(res.content)
    if "exception" in res:
        print(res["message"])
        return res["message"]
    return res["attempt"]

def mod_quiz_get_attempt_data(attempt_id):
    functionname = "mod_quiz_get_attempt_data"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"attemptid" : attempt_id, "page":0}
    res = requests.post(serverurl, params=params)
    res = json.loads(res.content)
    print(res["nextpage"])
    print(res)
    for question in res["questions"]:
        print(question.keys())
        print(question["slot"])
        print(question["type"])
        print(question["page"])
        print(question["sequencecheck"])
        print(question["number"])
        print(question["status"])
        print(question["settings"])

    if "exception" in res:
        print(res["message"])
        return res["message"]
    return res

############################################################################ Questionnaire Responses Services

def questionnaire_import_responses_questionnaire(df_questionnaire, df_users, df_risposte_domande_questionnaire, df_risposte_multichoice, df_domande, df_risposte_rating, df_risposte_short_answer, df_risposte_numeric):
    df_quiz_activities = df_questionnaire[df_questionnaire["IdMoodle"] != 0]
    print(df_quiz_activities)
    for questionnaire in df_quiz_activities.itertuples():
        id_moodle = questionnaire.IdMoodle
        if id_moodle != 0:
            import_single_quest_responses(df_domande, df_risposte_domande_questionnaire, df_risposte_multichoice,
                                          df_risposte_rating, df_risposte_short_answer, df_risposte_numeric, df_users, questionnaire, id_moodle)


def import_single_quest_responses(df_domande, df_risposte_domande_questionnaire, df_risposte_multichoice,
                                  df_risposte_rating, df_risposte_short_answer, df_risposte_numeric, df_users, questionnaire, id_moodle):
    domande = df_risposte_domande_questionnaire[
        df_risposte_domande_questionnaire["RSQS_QSTN_Id"] == questionnaire.QSTN_Id]
    log_generator("Importing questionaire responses for quest: {} id module in Moodle: {}, nr domande: {}".format(
        questionnaire.QSTN_Id, id_moodle, len(domande)))
    if len(domande) > 0:
        for name, domande_of_user in domande.groupby("RSQS_Id"):
            user = df_users[df_users["PRSN_id"].isin(domande_of_user["RSQS_PRSN_Id"])]
            if len(user) == 0:
                continue
            # print(domande_of_user)
            # get responses of rating questions
            domande_of_user_rating = domande_of_user.merge(df_risposte_rating, left_on=["RSQS_Id", "DMML_Id"],
                                                           right_on=["RSRT_RSQS_Id", "DMRT_DMML_Id"])
            temp = pd.DataFrame(domande_of_user_rating.groupby("DMRT_DMML_Id")["option"].apply(';;;'.join))
            temp = temp.reset_index()
            answers_rating = temp.merge(df_domande, left_on="DMRT_DMML_Id", right_on="DMML_Id")
            # get responses of short answer questions
            answers_of_user_short_answer = domande_of_user.merge(df_risposte_short_answer,
                                                                 left_on=["RSQS_Id", "DMML_Id"],
                                                                 right_on=["RSTL_RSQS_Id", "DMML_Id"])
            # print(answers_of_user_short_answer)
            # get responses of multichoice
            domande_of_user_multichoice = domande_of_user.merge(df_risposte_multichoice, left_on=["RSQS_Id", "DMML_Id"],
                                                                right_on=["RSOM_RSQS_Id", "DMMT_DMML_Id"])
            temp = pd.DataFrame(domande_of_user_multichoice.groupby("DMMT_DMML_Id")["DMMO_Testo"].apply(';;;'.join))
            temp = temp.reset_index()
            answers_of_user_multichoice = temp.merge(df_domande, left_on="DMMT_DMML_Id", right_on="DMML_Id")
            # print(answers_of_user_multichoice)

            # get responses of numeric questions
            answers_of_user_numeric = domande_of_user.merge(df_risposte_numeric,
                                                                 left_on=["RSQS_Id", "DMML_Id"],
                                                                 right_on=["RSNM_RSQS_Id", "DMML_Id"])
            print(answers_of_user_numeric)

            if len(answers_rating) > 0 or len(answers_of_user_short_answer) > 0 or len(answers_of_user_multichoice) > 0 or len(answers_of_user_numeric) > 0:
                # print(answers_rating["option"].to_dict())
                # Submit questionnaire responses
                mod_questionnaire_submit_questionnaire_response(int(id_moodle), int(user.moodleUserId),
                                                                domande_of_user.to_dict('records')[0]["time_start"],
                                                                answers_rating, answers_of_user_short_answer,
                                                                answers_of_user_multichoice, answers_of_user_numeric)


def mod_questionnaire_submit_questionnaire_response(questionnaireid, userid, time, answers_rating, answers_of_user_short_answer, answers_of_user_multichoice, answers_of_user_numeric):
    functionname = "local_modcustomfields_generate_questionnaire_responses"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"questionnaireid" : questionnaireid, "userid": userid, "time": time}
    questions = {}
    iter = 0
    print(questionnaireid)
    for answer in answers_rating.itertuples():
        soup = BeautifulSoup(str(answer.DMML_Testo)[:20], 'html.parser')
        questions["questions[{}][name]".format(iter)] = soup.get_text(strip=True) + "_" + str(answer.DMML_Id)
        questions["questions[{}][values]".format(iter)] = answer.option
        questions["questions[{}][position]".format(iter)] = answer.LKQD_NumeroDomanda
        iter = iter + 1
    for answer in answers_of_user_short_answer.itertuples():
        soup = BeautifulSoup(str(answer.DMML_Testo)[:20], 'html.parser')
        questions["questions[{}][name]".format(iter)] = soup.get_text(strip=True) + "_" + str(answer.DMML_Id)
        questions["questions[{}][values]".format(iter)] = answer.RSTL_Testo
        questions["questions[{}][position]".format(iter)] = answer.LKQD_NumeroDomanda
        iter = iter + 1
    for answer in answers_of_user_multichoice.itertuples():
        soup = BeautifulSoup(str(answer.DMML_Testo)[:20], 'html.parser')
        questions["questions[{}][name]".format(iter)] = soup.get_text(strip=True) + "_" + str(answer.DMML_Id)
        questions["questions[{}][values]".format(iter)] = answer.DMMO_Testo
        questions["questions[{}][position]".format(iter)] = answer.LKQD_NumeroDomanda
        iter = iter + 1
    for answer in answers_of_user_numeric.itertuples():
        soup = BeautifulSoup(str(answer.DMML_Testo)[:20], 'html.parser')
        questions["questions[{}][name]".format(iter)] = soup.get_text(strip=True) + "_" + str(answer.DMML_Id)
        questions["questions[{}][values]".format(iter)] = answer.RSNM_Numero
        questions["questions[{}][position]".format(iter)] = answer.LKQD_NumeroDomanda
        iter = iter + 1
    print(questions)
    res = requests.post(serverurl, params=params, data=questions)
    print(res.content)
