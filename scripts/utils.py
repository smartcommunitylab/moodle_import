from bs4 import BeautifulSoup
import logging

logging.basicConfig(filename="logging.log", level=logging.INFO, filemode="a", format='%(levelname)s: %(asctime)s: %(message)s')
logging.getLogger('chardet.charsetprober').setLevel(logging.INFO)

ENDPOINT = "formazioneadistanza.provincia.tn.it/Modules/"
SCORM_PLAYER_OLD = "ScormPlayerLoader"
SCORM_PLAYER = "GenericPlayerInline"
FILE_REPO = "File.repository"

def clean_text(text):
    resources = []
    soup = BeautifulSoup(text, 'html.parser')
    for link in soup.find_all('a'):
        href = link.get('href')
        extract_resources(href, resources)
        if ENDPOINT in href:
            link.decompose()
    return resources, str(soup)

def extract_resources(href, resources):
    if ENDPOINT in href:
        parts = href.split("?")
        if SCORM_PLAYER in parts[0] :
            file_id = extract_file_id(parts[1], "fileId")
            if file_id not in resources:
                resources.append(file_id)
        if SCORM_PLAYER_OLD in parts[0]:
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
    parameters = href.split("&")
    for param in parameters:
        name_value = param.split("=")
        name = name_value[0]
        value = name_value[1]
        if name == type :
            file_id = value
    return file_id

def log_generator(message):
    logging.info("Error Message : %s", message)

