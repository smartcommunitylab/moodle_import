import requests, json, os
from bs4 import BeautifulSoup
from isodate import parse_duration
from collections import namedtuple
from config import instance_params
from moodle_services import define_material
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
            if moodle_source_activity_id is not None and moodle_target_activity_id is not None:
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
        print(l3_activity_id)
        # Find the proper course module id
        if moodle_activity_id != 0:
            cmid = get_course_module_by_instance(module_name, moodle_activity_id)
        # Filter completion status of the activity for all users
        status_of_activity = df_status[(df_status["IdSubActivity"] == l3_activity_id) & (df_status["Completion"] == 100)]
        status_of_activity = status_of_activity.merge(df_users, left_on="IdPerson", right_on="PRSN_id")
        print(status_of_activity)
        for usage in status_of_activity.itertuples():
            user_moodle = usage.moodleUserId
            activities_completion_request(user_moodle, cmid, usage.time)

def import_scorm_tracks(df_file, df_activities, df_scorm_tracks, df_users):
    activities = df_activities[df_activities["moduleMoodle"] == "scorm"]
    for activity in activities.itertuples():
        id_instance = activity.IdMoodle
        id_materiale = activity.IdObjectLong
        file = df_file[df_file["FLDS_id"] == id_materiale].to_dict('records')
        if len(file) > 0:
            file_guid = file[0]["FLDS_GUID"]
            # Find Moodle sco activities inside each file scorm
            moodle_scoes = mod_scorm_get_scorm_scoes(id_instance)
            #print(moodle_scoes)
            for sco in moodle_scoes["scoes"]:
                if sco["scormtype"] == "sco":
                    # Find L3 sco activities
                    #print(file_guid , sco["identifier"])
                    scorm_tracks = df_scorm_tracks[(df_scorm_tracks["fileguid"] == file_guid) & (df_scorm_tracks["ACTIVITY_ID"] == sco["identifier"].encode('utf-8').strip())]
                    scorm_tracks_users = scorm_tracks.merge(df_users, left_on="LEARNER_ID", right_on="PRSN_id")
                    #print(scorm_tracks_users)
                    for index, track in enumerate(scorm_tracks_users.itertuples()):
                        xml = track.XML
                        user_id = track.moodleUserId
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
                        insert_scorm_tracks(sco["id"], 1, user_id, int(track.time), tracks)

def activities_completion_request(userid, cmid, time):
    functionname = "core_completion_override_activity_completion_status"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params ={"userid":int(userid), "cmid" : int(cmid), "newstate": 1}
    res = requests.post(serverurl, params=params)
    print(res.content)
    result = json.loads(res.content)

def get_course_module_by_instance(module, instance):
    functionname = "core_course_get_course_module_by_instance"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params ={"module" : module, "instance" : int(instance)}
    res = requests.post(serverurl, params=params)
    result = json.loads(res.content)
    print(result)
    return result["cm"]["id"]

################################################################################### SCORM Services

def insert_scorm_tracks(scoid, attempt, user_id, time, tracks):
    functionname = "local_modcustomfields_insert_scorm_tracks"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    all_tracks = {}
    iter = 0
    for key, val in tracks.items():
        all_tracks["tracks[{}][element]".format(iter)] = key
        all_tracks["tracks[{}][value]".format(iter)] = val
        iter = iter + 1
    params  = {"scoid" : scoid, "attempt" : attempt, "userid" : user_id, "time" : time}
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

def tag_course(course_id, tag_id):
    functionname = "local_modcustomfields_tag_course"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"courseid" : int(course_id), "tagid" : int(tag_id)}
    res = requests.post(serverurl, params=params)
    print(res.content)
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
        sections = df_wiki_sections[df_wiki_sections["WKSZ_WIKI_id"] == wiki.WIKI_id].sort_values(by="WKSZ_dataInserimento")
        sections = sections.merge(df_users, left_on="WKSZ_PRSN_id", right_on="PRSN_id")
        sections_titles = ""
        user_id = 2
        wiki_name = wiki.WIKI_nome
        if len(sections) > 0:
            sections_titles = "</br></br>".join(sections["WKSZ_nome"].apply(lambda x: "<h1>[[" + x + "]]</h1>").tolist())
            user_id = sections.loc[sections["WKSZ_isDefault"] == 1, :]["moodleUserId"]
            wiki_name = sections.loc[sections["WKSZ_isDefault"] == 1, :]["WKSZ_nome"]
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
            module_id, module_type = define_material(idCourseMoodle, material, row, 0, stealth=1)
            cm_id = get_course_module_by_instance(module_type, module_id)
            link = generate_activity_link(cm_id, module_type)
            resources[file_id] = link
        else:
            link = resources[file_id]
    return link

#################################################################################### QUIZ RESPONSES services
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

def process_quiz_attempt(attempt_id, attempt_unique_id):
    functionname = "mod_quiz_process_attempt"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"attemptid" : attempt_id, "finishattempt":"1", }
    versions = {}
    versions["data[{}][name]".format(0)] = "q{}:1_answer".format(attempt_unique_id)
    versions["data[{}][value]".format(0)] = "1"
    versions["data[{}][name]".format(1)] = "q{}:1_:sequencecheck".format(attempt_unique_id)
    versions["data[{}][value]".format(1)] = "1"
    versions["data[{}][name]".format(2)] = "q{}:2_answer".format(attempt_unique_id)
    versions["data[{}][value]".format(2)] = "0"
    versions["data[{}][name]".format(3)] = "q{}:2_:sequencecheck".format(attempt_unique_id)
    versions["data[{}][value]".format(3)] = "1"
    versions["data[{}][name]".format(4)] = "q{}:3_answer".format(attempt_unique_id)
    versions["data[{}][value]".format(4)] = "2"
    versions["data[{}][name]".format(5)] = "q{}:3_:sequencecheck".format(attempt_unique_id)
    versions["data[{}][value]".format(5)] = "1"
    versions["data[{}][name]".format(6)] = "q{}:4_answer".format(attempt_unique_id)
    versions["data[{}][value]".format(6)] = "1"
    versions["data[{}][name]".format(7)] = "q{}:4_:sequencecheck".format(attempt_unique_id)
    versions["data[{}][value]".format(7)] = "1"
    res = requests.post(serverurl, params=params, data=versions)
    res = json.loads(res.content)
    print(res)
    return res

def mod_quiz_start_attempt(quizid):
    functionname = "mod_quiz_start_attempt"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"quizid" : quizid} #, "forcenew":1
    res = requests.post(serverurl, params=params)
    res = json.loads(res.content)
    print(res)
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

def mod_questionnaire_submit_questionnaire_response(questionnaireid, userid, cmid, sec):
    functionname = "mod_questionnaire_submit_questionnaire_response"
    serverurl = moodle_url  + '&wsfunction=' + functionname
    params  = {"questionnaireid" : questionnaireid, "surveyid":questionnaireid, "userid": userid, "cmid": cmid, "sec":sec, "completed":1, \
               "rid": 24, "submit": 1, "action": "submit"}
    responses = {}
    responses["responses[{}][name]".format(0)] = "q1_2".format()
    responses["responses[{}][value]".format(0)] = "1"
    res = requests.post(serverurl, params=params, data=responses)
    res = json.loads(res.content)
    print(res)
    if "exception" in res:
        print(res["message"])
        return res["message"]
    return res

#attempt_id = mod_quiz_start_attempt(276) #attempt_id=66
#mod_quiz_get_attempt_data(attempt_id)
#process_quiz_attempt(attempt_id["id"], attempt_unique_id["uniqueid"])
#save_quiz_attempt(attempt_id)

#questionnaire responses
#mod_questionnaire_submit_questionnaire_response(816, 2, 13814,0)
