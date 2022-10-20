from bs4 import BeautifulSoup
import logging, sys
from config import instance_params
from base64 import b64encode, b64decode
from Crypto.Cipher import DES
from Crypto.Util.Padding import pad, unpad

logging.basicConfig(filename="logging.log", level=logging.INFO, filemode="a", format='%(levelname)s: %(asctime)s: %(message)s')
logging.getLogger('chardet.charsetprober').setLevel(logging.INFO)

ENDPOINT_MODULE = "formazioneadistanza.provincia.tn.it/Modules/"
ENDPOINT_WIKI = "formazioneadistanza.provincia.tn.it/Wiki/"
ENDPOINT_WIKI2 = "formazioneadistanza.provincia.tn.it/wiki/"
ENDPOINT_MATERIAL = "formazioneadistanza.provincia.tn.it/modules/communitydiary"
ENDPOINT_QUESTIONNAIRE = "formazioneadistanza.provincia.tn.it/questionari"
SCORM_PLAYER_OLD = "ScormPlayerLoader"
SCORM_PLAYER = "GenericPlayerInline"
FILE_REPO = "File.repository"

def clean_text(text):
    resources = []
    questionnaires = []
    soup = BeautifulSoup(str(text), 'html.parser')
    try:
        for link in soup.find_all('a'):
            if link is not None:
                href = link.get('href')
                if ENDPOINT_WIKI in href or ENDPOINT_WIKI2 in href:
                    link.decompose()
                elif ENDPOINT_QUESTIONNAIRE in href:
                    extract_questionnaires(href, questionnaires)
                    link.decompose()
                elif ENDPOINT_MATERIAL in href:
                    resources.append("all")
                    link.decompose()
                elif ENDPOINT_MODULE in href:
                    extract_resources(href, resources)
                    link.decompose()
    except Exception as err:
        logging.info("Error Message inside clean_text:  %s ", err)
        return resources, str(soup), questionnaires
    return resources, str(soup), questionnaires

def extract_resources(href, resources):
    file_id = 0
    if ENDPOINT_MODULE in href:
        parts = href.split("?")
        if SCORM_PLAYER in parts[0] :
            file_id = extract_file_id(parts[1], "fileId")
        elif SCORM_PLAYER_OLD in parts[0]:
            file_id = extract_file_id(parts[1], "FileID")
        elif FILE_REPO in parts[0]:
            file_id = extract_file_id(parts[1], "FileID")
        if file_id not in resources and file_id != 0:
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
    logging.info("Message :  %s %s", message, entity_id)

def role_mapping(role_id):
    roles_map_existing = {
         "-4": 6, # {'Utente Non autenticato':"guest"},
         "-3": 7, # {'Passante':"user"},
         "-2": 2, # {'Creatore':"coursecreator"},
          "1": 1, # {'Amministratore Comunita':"manager"},
          "4": 6, # {'Guest':"guest"},
          "8": 4, # {'Tutor':"teacher"},
         "29": 3, # {'Docente':"editingteacher"},
         "15": 5,# {'Partecipante':"student"}
         "44": 5, # Formazione permanente
         "10": 10, # Segreteria
         "16": 11, # Esterno
         "17": 12, # Copisteria
         "18": 13, # Collaboratore
    }
    result = roles_map_existing.get(str(role_id), 5)
    return result


def extract_questionnaires(href, resources):
    qid = 0
    if ENDPOINT_QUESTIONNAIRE in href:
        info = href.split("?")[1]
        qid = decrypt_str(info)
    if qid not in resources and qid != 0:
        resources.append(qid)
    return resources


def decrypt_str(element):
    element = element[element.find("=") + 1:].split("&")[0]
    elements = decrypt(element).split("&")
    for el in elements:
        param = el.split("=")
        if param[0] == "idq":
            return int(param[1])
    return 0


def decrypt(element):
    try:
        key = "germanic".encode("utf-8")
        iv_bytes = [0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF]
        iv = bytearray(iv_bytes)
        cipher = DES.new(key, DES.MODE_CBC, iv)
        decrypted_value = unpad(cipher.decrypt(b64decode(element)), 8)
        return decrypted_value.decode("utf-8")
    except Exception as err:
        log_generator(err, "Error during qid decryption: {}".format(element))
        return 0