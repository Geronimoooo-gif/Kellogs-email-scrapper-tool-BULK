import scrapy
import re
import streamlit as st
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse
import pandas as pd
import io
import subprocess
import sys
import tempfile
import os
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from lxml import html

def process_single_url(url, timeout=30):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(f"""
import scrapy
import re
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse
import html
from lxml import html as lxml_html

class EmailSpider(scrapy.Spider):
    name = "email_spider"

    def __init__(self, url=None, *args, **kwargs):
        super(EmailSpider, self).__init__(*args, **kwargs)
        self.url = "{url}"
        parsed_url = urlparse(self.url)
        base_url = f"{{parsed_url.scheme}}://{{parsed_url.netloc}}"
        
        self.start_urls = [
            base_url,
            f"{{base_url}}/contact/",
            self.url
        ]
        self.all_emails = set()

    def closed(self, reason):
        if self.all_emails:
            print(f"FINAL_RESULTS:{{','.join(self.all_emails)}}", flush=True)
        else:
            print("FINAL_RESULTS:", flush=True)

    def parse(self, response):
        try:
            # Utiliser lxml pour un parsing plus rapide
            tree = lxml_html.fromstring(response.body)
            
            # Extraire le texte directement avec XPath
            text_content = ' '.join(tree.xpath('//text()'))
            
            # Pattern combiné pour tous les types d'emails
            email_pattern = r'[a-zA-Z0-9._%+-]+(?:@|&#64;|&#x40;|%40|＠)[a-zA-Z0-9.-]+\.[a-zA-Z]{{2,}}'
            
            # Trouver tous les emails en une seule passe
            all_emails = set(re.findall(email_pattern, text_content))
            
            # Décodage HTML pour les emails encodés
            decoded_emails = set(html.unescape(email) for email in all_emails)
            
            if decoded_emails:
                print(f"Emails trouvés sur {{response.url}}: {{list(decoded_emails)}}", flush=True)
                self.all_emails.update(decoded_emails)
            
        except Exception as e:
            print(f"Erreur lors du parsing: {{str(e)}}", flush=True)
        
        return None

process = CrawlerProcess(settings={{
    "LOG_ENABLED": False,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "ROBOTSTXT_OBEY": False,
    "CONCURRENT_REQUESTS": 32,
    "CONCURRENT_REQUESTS_PER_DOMAIN": 16,
    "DOWNLOAD_TIMEOUT": 15,
    "COOKIES_ENABLED": False,
    "RETRY_ENABLED": False,
    "DOWNLOAD_DELAY": 0,
}})

process.crawl(EmailSpider, url="{url}")
process.start()
""")

    try:
        process = subprocess.Popen([sys.executable, f.name],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 universal_newlines=True)
        
        stdout, stderr = process.communicate(timeout=timeout)
        
        os.unlink(f.name)
        
        # Debug: afficher la sortie complète
        print("Stdout:", stdout)
        print("Stderr:", stderr)
        
        # Extraire les résultats
        for line in stdout.splitlines():
            if line.startswith("FINAL_RESULTS:"):
                emails = line.replace("FINAL_RESULTS:", "").strip()
                return emails if emails else ""
        
        return ""
        
    except subprocess.TimeoutExpired:
        process.kill()
        return "Timeout"
    except Exception as e:
        print(f"Erreur dans process_single_url: {str(e)}")
        return f"Erreur: {str(e)}"

# Déplacer process_csv en dehors de process_single_url
def process_csv(df, progress_bar, status_text):
    """Traite le fichier CSV avec du multiprocessing"""
    df_result = df.copy()
    
    if 'Mail' not in df_result.columns:
        df_result['Mail'] = ''
    
    total_urls = len(df_result)
    processed = 0
    
    # Nombre de workers (processus parallèles)
    max_workers = multiprocessing.cpu_count() * 2
    
    # Créer un dictionnaire des URLs à traiter
    url_dict = {index: row['URL'].strip() 
                for index, row in df_result.iterrows() 
                if pd.notna(row['URL'])}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Soumettre les tâches
        future_to_url = {executor.submit(process_single_url, url): (index, url) 
                        for index, url in url_dict.items()}
        
        # Traiter les résultats au fur et à mesure
        for future in as_completed(future_to_url):
            index, url = future_to_url[future]
            try:
                emails = future.result()
                df_result.loc[index, 'Mail'] = emails
            except Exception as e:
                print(f"Error processing URL {url}: {str(e)}")
                df_result.loc[index, 'Mail'] = f"Erreur: {str(e)}"
            
            processed += 1
            progress_bar.progress(processed / total_urls)
            status_text.text(f"Traitement de l'URL {processed}/{total_urls}")
    
    return df_result

def main():
    st.title("Scraper d'adresses email")

    if 'crawler_running' not in st.session_state:
        st.session_state.crawler_running = False

    col1, col2 = st.columns(2)

    with col1:
        url_input = st.text_input("Entrez une URL à scraper :")
        if st.button("Scanner une URL"):
            if url_input:
                try:
                    with st.spinner('Scan en cours...'):
                        emails = process_single_url(url_input)
                        if emails:
                            st.success(f"Adresses email trouvées : {emails}")
                        else:
                            st.warning("Aucune adresse email trouvée.")
                except Exception as e:
                    st.error(f"Erreur : {str(e)}")
            else:
                st.error("Veuillez entrer une URL valide.")

    with col2:
        uploaded_file = st.file_uploader("Ou téléchargez un fichier CSV", type=['csv'])
        if uploaded_file is not None:
            try:
                # Lire le CSV
                df = pd.read_csv(uploaded_file)
                
                if 'URL' not in df.columns:
                    st.error("Le fichier CSV doit contenir une colonne 'URL'")
                else:
                    total_urls = len(df)
                    st.write(f"Nombre total d'URLs à traiter : {total_urls}")
                    
                    if st.button("Scanner les URLs du CSV"):
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Traiter le CSV et obtenir les résultats
                        results_df = process_csv(df, progress_bar, status_text)
                        
                        # Statistiques des résultats
                        emails_found = results_df['Mail'].notna().sum()
                        success_rate = (emails_found / total_urls) * 100
                        
                        # Afficher les statistiques
                        st.write(f"URLs traitées avec succès : {emails_found}/{total_urls} ({success_rate:.2f}%)")
                        
                        # Convertir en CSV
                        csv = results_df.to_csv(index=False)
                        
                        # Afficher un aperçu des résultats
                        st.write("Aperçu des résultats :")
                        st.write(results_df)
                        
                        # Bouton de téléchargement
                        st.download_button(
                            label="Télécharger les résultats",
                            data=csv,
                            file_name="resultats_scraping.csv",
                            mime="text/csv"
                        )
                        
                        status_text.text("Traitement terminé!")

            except Exception as e:
                st.error(f"Erreur lors du traitement du fichier : {str(e)}")

    st.markdown("""
    ### Instructions d'utilisation :
    1. **Pour une URL unique** : 
       - Entrez l'URL dans le champ de texte
       - Cliquez sur "Scanner une URL"
    
    2. **Pour plusieurs URLs** :
       - Préparez un fichier CSV avec une colonne 'URL'
       - Téléchargez le fichier
       - Cliquez sur "Scanner les URLs du CSV"
       - Téléchargez les résultats
    
    **Note** : Le scraper scanne automatiquement :
    - L'URL fournie
    - La page d'accueil
    - La page contact (/contact/)
    """)

if __name__ == "__main__":
    main()
