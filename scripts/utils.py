from bs4 import BeautifulSoup
import logging, sys
from config import instance_params

logging.basicConfig(filename="logging.log", level=logging.INFO, filemode="a", format='%(levelname)s: %(asctime)s: %(message)s')
logging.getLogger('chardet.charsetprober').setLevel(logging.INFO)

ENDPOINT_MODULE = "formazioneadistanza.provincia.tn.it/Modules/"
ENDPOINT_WIKI = "formazioneadistanza.provincia.tn.it/Wiki/"
ENDPOINT_MATERIAL = "formazioneadistanza.provincia.tn.it/Modules/"
SCORM_PLAYER_OLD = "ScormPlayerLoader"
SCORM_PLAYER = "GenericPlayerInline"
FILE_REPO = "File.repository"

def clean_text(text):
    resources = []
    soup = BeautifulSoup(str(text), 'html.parser')
    try:
        for link in soup.find_all('a'):
            if link is not None:
                href = link.get('href')
                if ENDPOINT_WIKI in href:
                    link.decompose()
                elif ENDPOINT_MODULE in href:
                    extract_resources(href, resources)
                    link.decompose()
    except Exception as err:
        logging.info("Error Message inside clean_text:  %s ", err)
        return resources, str(soup)
    return resources, str(soup)

def extract_resources(href, resources):
    if ENDPOINT_MODULE in href:
        parts = href.split("?")
        if SCORM_PLAYER in parts[0] :
            file_id = extract_file_id(parts[1], "fileId")
            if file_id not in resources:
                resources.append(file_id)
        elif SCORM_PLAYER_OLD in parts[0]:
            file_id = extract_file_id(parts[1], "FileID")
            if file_id not in resources:
                resources.append(file_id)
        elif FILE_REPO in parts[0]:
            file_id = extract_file_id(parts[1], "FileID")
            if file_id not in resources:
                resources.append(file_id)
    return resources

def extract_file_id(href, type):
    file_id = ""
    if href.strip() == "":
        return file_id
    parameters = href.split("&")
    for param in parameters:
        name_value = param.split("=")
        name = name_value[0]
        value = name_value[1]
        if name == type:
            return value
    return file_id

def generate_activity_link(cm_id, cm_type):
    cm_link = "{}mod/{}/view.php?id={}".format(instance_params["moodle_url"], cm_type, cm_id)
    return cm_link

def log_generator(message, entity_id = None):
    logging.info("Error Message :  %s %s", message, entity_id)

def role_mapping(role_id):
    print("sourse: " + str(role_id))
    roles_map_existing = {
         "-4": 6, # {'Utente Non autenticato':"guest"},
         "-3": 7, # {'Passante':"user"},
         "-2": 2, # {'Creatore':"coursecreator"},
          "1": 1, # {'Amministratore Comunità':"manager"},
          "4": 6, # {'Guest':"guest"},
          "8": 4, # {'Tutor':"teacher"},
         "29": 3, # {'Docente':"editingteacher"},
         "15": 5# {'Partecipante':"student"}
    }
    result = roles_map_existing.get(str(role_id), 0)
    print("target: " + str(result))
    return result

'''
         "10": 'Segreteria',
         "16": 'Esterno',
         "17": 'Copisteria',
         "18": 'Collaboratore',
'''
