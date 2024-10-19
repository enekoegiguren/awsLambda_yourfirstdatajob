import requests
import json
import pandas as pd

from credentials import get_secret
from datetime import datetime

client_id, client_secret = get_secret()


def get_token(client_id, client_secret):

    # URL de l'API
    url = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire"

    # En-têtes de la requête
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    # Corps de la requête
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,  # Remplacez par votre identifiant client
        'client_secret': client_secret,     # Remplacez par votre clé secrète
        'scope': 'api_offresdemploiv2 o2dsoffre'  # Remplacez par les scopes que vous souhaitez
    }

    # Envoyer la requête POST pour obtenir le token
    response = requests.post(url, headers=headers, data=data)

    # Vérifier la réponse
    if response.status_code == 200:
        token_data = response.json()  # Convertir la réponse en JSON
        access_token = token_data['access_token']  # Extraire le token d'accès
        return access_token
    else:
        return f"Erreur : {response.status_code} - {response.text}"

def search_job_offers(token, mots_cles, range_value, beginning_date=None, ending_date=None):
    # URL de l'API pour rechercher des offres d'emploi avec le mot clé spécifié et la plage donnée
    if beginning_date == None or ending_date == None:
        url = f"https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search?range={range_value}&motsCles={mots_cles}"
    #curl --request GET \
    else:
        url = f'https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search?range={range_value}&motsCles={mots_cles}&minCreationDate={beginning_date}&maxCreationDate={ending_date}'


    # En-têtes de la requête
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'  # Ajouter le token d'accès
    }

    # Envoyer la requête GET
    response = requests.get(url, headers=headers)

    # Vérifier la réponse
    if response.status_code in [200, 206]:
        job_offers = response.json()  # Convertir la réponse en JSON
        return job_offers  # Retourner les données des offres d'emploi
    else:
        return f"Erreur : {response.status_code} - {response.text}"


def get_offers_data(max_results, mots_cles, client_id, client_secret, beginning_date=None, ending_date=None):
    all_offers = []
    mots_cles = mots_cles
    max_results = max_results
    step = 50

    for i in range(0, max_results, step):

        range_value = f'{i}-{i+step-1}'
        token = get_token(client_id, client_secret)
        job_offers = search_job_offers(token, mots_cles, range_value, beginning_date, ending_date)
        if isinstance(job_offers, dict):
            offers = job_offers.get('resultats', [])
            
            for offer in offers:
                simplified_offer = {
                    'id': offer.get('id'),
                    'title': offer.get('intitule'),
                    'description': offer.get('description'),
                    'date_creation':offer.get('dateCreation'),
                    'date_actualization':offer.get('dateActualisation'),
                    'place':offer.get('lieuTravail'),
                    'contract_type':offer.get('typeContrat'),
                    'contract_nature':offer.get('natureContrat'),
                    'experience_bool':offer.get('experienceExige'),
                    'experience':offer.get('experienceLibelle'),
                    'salary':offer.get('salaire'),
                    'company_field':offer.get('secteurActiviteLibelle'),
                    'competencies':offer.get('competences')
                }
                
            
                place = simplified_offer.get('place', {})
                latitude = place.get('latitude')
                longitude = place.get('longitude')
                code_postal = place.get('codePostal')
                competencies = simplified_offer.get('competenices', {})
                competency_list = {f'competencie_{i + 1}': comp['libelle'] for i, comp in enumerate(competencies)}
                
                salary = simplified_offer.get('salary', {})
                salary_libelle = salary.get('libelle')
                
                extracted_offer = {
                    'id': simplified_offer.get('id'),
                    'title': simplified_offer.get('title'),
                    'code_postal': code_postal,
                    'latitude': latitude,
                    'longitude': longitude,
                    'description': simplified_offer['description'],
                    'date_creation':simplified_offer['date_creation'],
                    'date_actualization':simplified_offer.get('date_actualization'),
                    'contract_type':simplified_offer.get('contract_type'),
                    'contract_nature':simplified_offer.get('contract_nature'),
                    'experience_bool':simplified_offer.get('experience_bool'),
                    'experience':simplified_offer.get('experience'),
                    'salary':salary_libelle,
                    'company_field':simplified_offer.get('company_field')
                }
                
                extracted_offer.update(competency_list)
                all_offers.append(extracted_offer)
                

                
    return all_offers

def get_beginning_ending_dates(date1_str, date2_str):
    # Convertir les chaînes de caractères en objets datetime
    date_format = "%Y-%m-%d"
    date1 = datetime.strptime(date1_str, date_format)
    date2 = datetime.strptime(date2_str, date_format)
    
    # Trouver la date minimale et maximale
    min_date = min(date1, date2)
    max_date = max(date1, date2)
    
    # Formater les dates au format ISO 8601 avec heure fixe à 00:00:00Z
    beginning_date = min_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    ending_date = max_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    return beginning_date, ending_date



#token = get_token(client_id, client_secret)
def get_data(min_date,
             max_date,
             range,
             mots,
             client_id,
             client_secret):
    
    beginning_date, ending_date = get_beginning_ending_dates(min_date,max_date)
    all_offers = get_offers_data(range, mots, client_id, client_secret, beginning_date, ending_date)

    df = pd.DataFrame(all_offers)

    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d")


    df.to_parquet(f'output_file_{min_date}_{max_date}_{timestamp}.parquet', index=False)

get_data('2024-10-01',
         '2024-10-17',
         100,
         'data',
         client_id,
         client_secret
         )



