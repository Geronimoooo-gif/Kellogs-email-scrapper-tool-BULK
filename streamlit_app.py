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

def process_single_url(url):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(f"""
import scrapy
import re
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse
import html

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
            # Décode le contenu HTML
            html_content = response.body.decode(response.encoding or 'utf-8')
            
            # Recherche les emails encodés en ASCII (par exemple &#64; pour @)
            ascii_content = html.unescape(html_content)
            
            # Nettoie le HTML
            html_without_scripts = re.sub(r'<script.*?>.*?</script>', '', ascii_content, flags=re.DOTALL)
            html_without_styles = re.sub(r'<style.*?>.*?</style>', '', html_without_scripts, flags=re.DOTALL)
            text_content = re.sub(r'<[^>]+>', ' ', html_without_styles)
            cleaned_text = ' '.join(text_content.split())
            
            # Pattern pour les emails standards et potentiellement encodés
            email_pattern = r'[a-zA-Z0-9._%+-]+[@＠]{{1}}[a-zA-Z0-9.-]+\.[a-zA-Z]{{2,}}'
            emails_found = re.findall(email_pattern, cleaned_text)
            
            # Pattern pour détecter les emails avec @ encodé en ASCII
            ascii_pattern = r'[a-zA-Z0-9._%+-]+(?:&#64;|&#x40;|%40)[a-zA-Z0-9.-]+\.[a-zA-Z]{{2,}}'
            ascii_emails = re.findall(ascii_pattern, html_content)
            
            # Décode les emails trouvés avec @ encodé
            decoded_ascii_emails = [html.unescape(email) for email in ascii_emails]
            
            # Combine et nettoie tous les emails trouvés
            all_found_emails = set(emails_found + decoded_ascii_emails)
            
            if all_found_emails:
                print(f"Emails trouvés sur {{response.url}}: {{list(all_found_emails)}}", flush=True)
                self.all_emails.update(all_found_emails)
            
        except Exception as e:
            print(f"Erreur lors du parsing: {{str(e)}}", flush=True)
        
        return None

process = CrawlerProcess(settings={{
    "LOG_ENABLED": True,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "ROBOTSTXT_OBEY": False,
}})

process.crawl(EmailSpider, url="{url}")
process.start()
""")

    try:
        # Exécuter le script temporaire
        process = subprocess.Popen([sys.executable, f.name],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 universal_newlines=True)
        
        stdout, stderr = process.communicate()
        
        # Supprimer le fichier temporaire
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
        
    except Exception as e:
        print(f"Erreur dans process_single_url: {str(e)}")
        return f"Erreur: {str(e)}"

def process_csv(df, progress_bar, status_text):
    """Traite le fichier CSV"""
    # Créer une copie du DataFrame pour éviter les problèmes de modification
    df_result = df.copy()
    
    # S'assurer que la colonne Mail existe
    if 'Mail' not in df_result.columns:
        df_result['Mail'] = ''
    
    total_urls = len(df_result)
    
    for index, row in df_result.iterrows():
        status_text.text(f"Traitement de l'URL {index + 1}/{total_urls}")
        if pd.notna(row['URL']):
            try:
                print(f"Processing URL: {row['URL']}")  # Debug
                url = row['URL'].strip()
                emails = process_single_url(url)
                print(f"Emails found: {emails}")  # Debug
                
                # Mettre à jour directement le DataFrame
                df_result.loc[index, 'Mail'] = emails
                
            except Exception as e:
                print(f"Error processing URL: {str(e)}")  # Debug
                df_result.loc[index, 'Mail'] = f"Erreur: {str(e)}"
        
        progress_bar.progress((index + 1) / total_urls)
    
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
                    if st.button("Scanner les URLs du CSV"):
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Traiter le CSV et obtenir les résultats
                        results_df = process_csv(df, progress_bar, status_text)
                        
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
