import requests, json
from config import instance_params

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

#activities_completion_request(2, 10470, 1639037887)
#8167
