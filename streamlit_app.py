import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime
import logging
import io

# Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ValueSerpAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.valueserp.com/search"
    
    def search(self, query, start=0):
        """Effectue une recherche via l'API ValueSerp"""
        params = {
            'api_key': self.api_key,
            'q': query,
            'location': 'France',
            'gl': 'fr',
            'hl': 'fr',
            'google_domain': 'google.fr',
            'start': start,
            'num': 100,
            'output': 'json',
            'include_html': 'false',
            'device': 'desktop'
        }
        
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la requête API: {str(e)}")
            return None

def scrape_google_urls(query, max_results=200, progress_bar=None):
    """Scrape les résultats Google via ValueSerp"""
    results = []
    
    api_key = st.secrets["VALUESERP_API_KEY"]
    api = ValueSerpAPI(api_key)
    
    # Calculer le nombre de requêtes nécessaires (100 résultats par requête)
    num_requests = (max_results + 99) // 100  # Arrondi supérieur
    
    for i in range(num_requests):
        start = i * 100
        if progress_bar:
            progress = (start + 100) / max_results
            progress_bar.progress(min(progress, 1.0))
        
        logger.info(f"Récupération des résultats {start+1} à {min(start+100, max_results)}")
        
        response_data = api.search(query, start=start)
        
        if not response_data:
            logger.error("Pas de réponse de l'API")
            break
            
        organic_results = response_data.get('organic_results', [])
        
        if not organic_results:
            logger.warning(f"Aucun résultat trouvé à partir de l'index {start}")
            break
            
        for position, result in enumerate(organic_results, start=start+1):
            if len(results) >= max_results:
                break
            url = result.get('link')
            if url:
                results.append({
                    "Position": position,
                    "URL": url
                })
        
        if len(results) >= max_results:
            break
            
        logger.info(f"Trouvé {len(organic_results)} résultats pour la page {i+1}")
        time.sleep(1)
    
    logger.info(f"Scraping terminé. Nombre total de résultats: {len(results)}")
    return results[:max_results]

def create_csv_files(dataframes):
    """Crée des fichiers CSV pour chaque dataframe et les stocke en mémoire."""
    output_files = {}
    
    for sheet_name, df in dataframes.items():
        clean_sheet_name = sheet_name.replace(" ", "_")[:31] + ".csv"
        
        output = io.BytesIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)
        
        output_files[clean_sheet_name] = output
    
    return output_files

def main():
    st.title("🔍 Scraper Google Search via ValueSerp")
    
    if "VALUESERP_API_KEY" not in st.secrets:
        st.error("Clé API ValueSerp manquante. Veuillez configurer vos secrets Streamlit.")
        return
    
    # Liste des villes
    default_cities = """Paris
Paris 1er arrondissemeeent2
Paris 2e arrondissement
Paris 3e arrondissement
Paris 4e arrondissement
Paris 5e arrondissement
Paris 6e arrondissement
Paris 7e arrondissement
Paris 8e arrondissement
Paris 9e arrondissement
Paris 10e arrondissement
Paris 11e arrondissement
Paris 12e arrondissement
Paris 13e arrondissement
Paris 14e arrondissement
Paris 15e arrondissement
Paris 16e arrondissement
Paris 17e arrondissement
Paris 18e arrondissement
Paris 19e arrondissement
Paris 20e arrondissement
Marseille
Lyon
Toulouse
Nice
Nantes
Strasbourg
Montpellier
Bordeaux
Lille
Rennes
Reims
Saint-Etienne
Toulon
Le Havre
Grenoble
Dijon
Angers
Nimes
Villeurbanne
Clermont-Ferrand
Saint-Denis
Le Mans
Aix-en-Provence
Brest
Tours
Amiens
Limoges
Annecy
Perpignan
Boulogne-Billancourt
Metz
Besancon
Orleans
Saint-Denis
Rouen
Argenteuil
Mulhouse
Montreuil
Caen
Nancy
Saint-Paul
Roubaix
Tourcoing
Nanterre
Vitry-sur-Seine
Avignon
Creteil
Poitiers
Dunkerque
Asnieres-sur-Seine
Courbevoie
Versailles
Colombes
Fort-de-France
Aulnay-sous-Bois
Saint-Pierre
Rueil-Malmaison
Pau
Aubervilliers
Champigny-sur-Marne
Le Tampon
Antibes
Saint-Maur-des-Fosses
Cannes
Drancy
Merignac
Saint-Nazaire
Colmar
Issy-les-Moulineaux
Noisy-le-Grand
Evry-Courcouronnes
Levallois-Perret
Troyes
Neuilly-sur-Seine
Sarcelles
Venissieux
Clichy
Pessac
Ivry-sur-Seine
Cergy
Quimper
La Rochelle
Beziers
Ajaccio
Saint-Quentin
Niort
Villejuif
Hyeres
Pantin
Chambery
Le Blanc-Mesnil
Lorient
Les Abymes
Montauban
Sainte-Genevieve-des-Bois
Suresnes
Meaux
Valence
Beauvais
Cholet
Chelles
Bondy
Frejus
Clamart
Narbonne
Bourg-en-Bresse
Fontenay-sous-Bois
Bayonne
Sevran
Antony
Maisons-Alfort
La Seyne-sur-Mer
Epinay-sur-Seine
Montrouge
Saint-Herblain
Calais
Vincennes
Macon
Villepinte
Martigues
Bobigny
Cherbourg-en-Cotentin
Vannes
Massy
Brive-la-Gaillarde
Arles
Corbeil-Essonnes
Saint-Andre
Saint-Ouen-sur-Seine
Albi
Belfort
Evreux
La Roche-sur-Yon
Saint-Malo
Bagneux
Chateauroux
Noisy-le-Sec
Salon-de-Provence
Le Cannet
Vaulx-en-Velin
Livry-Gargan
Angouleme
Sete
Puteaux
Thionville
Rosny-sous-Bois
Saint-Laurent-du-Maroni
Alfortville
Istres
Gennevilliers
Wattrelos
Talence
Blois
Tarbes
Castres
Garges-les-Gonesse
Saint-Brieuc
Arras
Douai
Compiegne
Melun
Reze
Saint-Chamond
Bourgoin-Jallieu
Gap
Montelimar
Thonon-les-Bains
Draguignan
Chartres
Joue-les-Tours
Saint-Martin-dHeres
Villefranche-sur-Saone
Chalon-sur-Saone
Mantes-la-Jolie
Colomiers
Anglet
Pontault-Combault
Poissy
Savigny-sur-Orge
Bagnolet
Lievin
Nevers
Gagny
Le Perreux-sur-Marne
Stains
Chalons-en-Champagne
Conflans-Sainte-Honorine
Montlucon
Palaiseau
Laval
Saint-Priest
LHay-les-Roses
Brunoy
Chatillon
Sainte-Marie
Bastia
Lens
Chambery
Saint-Benoit
Le Port
Saint-Leu
Noumea"""
    
    # Interface utilisateur
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            query = st.text_input(
                "Entrez votre terme de recherche",
                value="",
                placeholder="Exemple: avocat",
                help="Tapez votre terme de recherche principal"
            )
        
        with col2:
            cities = st.text_area(
                "Liste des villes (une par ligne)",
                value=default_cities,
                height=100,
                help="Entrez les villes, une par ligne"
            )
        
        max_results = st.select_slider(
            "Nombre de résultats à récupérer par ville",
            options=[10, 20, 30, 50, 100, 200],
            value=200,
            help="Choisissez le nombre de résultats Google à récupérer par ville"
        )
        
        # Calcul des coûts
        cities_list = [city.strip() for city in cities.split('\n') if city.strip()]
        num_requests_per_city = (max_results + 99) // 100
        total_requests = num_requests_per_city * len(cities_list)
        cost_per_request = 0.001
        estimated_cost = total_requests * cost_per_request
        
        # Informations de coût dans la sidebar
        st.sidebar.title("Estimation des coûts")
        st.sidebar.write(f"Nombre de villes: {len(cities_list)}")
        st.sidebar.write(f"Requêtes par ville: {num_requests_per_city}")
        st.sidebar.write(f"Total requêtes: {total_requests}")
        st.sidebar.write(f"Coût estimé: ${estimated_cost:.3f}")
        
        search_button = st.button("🔍 Lancer les recherches")
        
        if search_button:
            if not query or not cities_list:
                st.error("Veuillez entrer un terme de recherche et au moins une ville")
                return
            
            progress_bar = st.progress(0)
            all_results = {}
            
            for i, city in enumerate(cities_list):
                full_query = f"{query} {city}"
                st.text(f"Recherche en cours pour : {full_query}")
                
                data = scrape_google_urls(full_query, max_results, progress_bar)
                if data:
                    df = pd.DataFrame(data)
                    all_results[full_query] = df
                
                progress = (i + 1) / len(cities_list)
                progress_bar.progress(progress)
            
            if all_results:
                st.success(f"Recherches terminées ! Résultats trouvés pour {len(all_results)} villes.")
                
                csv_files = create_csv_files(all_results)
                
                for filename, csv_buffer in csv_files.items():
                    st.download_button(
                        label=f"📥 Télécharger {filename}",
                        data=csv_buffer,
                        file_name=filename,
                        mime="text/csv"
                    )
                
                for query_city, df in all_results.items():
                    with st.expander(f"Aperçu des résultats pour : {query_city}"):
                        st.dataframe(df)
            else:
                st.error("Aucun résultat trouvé.")

if __name__ == "__main__":
    main()
