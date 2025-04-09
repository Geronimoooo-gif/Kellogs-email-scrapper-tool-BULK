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
import chardet
from concurrent.futures import ThreadPoolExecutor, as_completed
from lxml import html

def filter_emails(emails_string):
    if not emails_string or emails_string == "":
        return ""
    
    # Liste des domaines à exclure
    excluded_domains = ["@ovh.com", "@ovh.net", "@simplebo.fr", "@mediateur-consommation-avocat.fr"]
    
    # Liste des préfixes à exclure
    excluded_prefixes = [
        "postmaster", "webmaster", "webmestre", 
        "dpo", "rgpd", "dpd", "sales", "serviceclient"
    ]
    
    filtered_emails = []
    
    # Traiter chaque email
    for email in emails_string.split(','):
        email = email.strip()
        if not email:
            continue
        
        # Vérifier si l'email contient un domaine exclu
        if any(domain in email.lower() for domain in excluded_domains):
            continue
        
        # Vérifier si l'email commence par un préfixe exclu
        if any(email.lower().startswith(prefix + "@") for prefix in excluded_prefixes):
            continue
        
        filtered_emails.append(email)
    
    return ','.join(filtered_emails)

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
            f"{{base_url}}/mentions-legales/",  # Page mentions légales
            self.url
        ]
        self.all_emails = set()
        self.all_phones = set()

    def closed(self, reason):
        results = {{
            'emails': ','.join(self.all_emails) if self.all_emails else '',
            'phones': ','.join(self.all_phones) if self.all_phones else ''
        }}
        print(f"FINAL_RESULTS:{{results}}", flush=True)

    def parse(self, response):
        try:
            # Décode le contenu HTML
            html_content = response.body
            encoding = response.encoding or 'utf-8'  # Si l'encodage est absent, on utilise UTF-8 par défaut
            html_content = html_content.decode(encoding, errors='replace')  # Remplace ou ignore les erreurs
            
            # Supprime le contenu des balises script
            html_without_scripts = re.sub(r'<script.*?>.*?</script>', '', html_content, flags=re.DOTALL)
            
            # Utiliser lxml pour parser le HTML nettoyé
            tree = lxml_html.fromstring(html_without_scripts)
            
            # Extraire le texte
            text_content = ' '.join(tree.xpath('//text()'))
            
            # Pattern pour les emails
            email_pattern = r'[a-zA-Z0-9._%+-]+(?:@|&#64;|&#x40;|%40|＠)[a-zA-Z0-9.-]+\.[a-zA-Z]{{2,}}'
            
            # Pattern pour les numéros de téléphone français
            phone_patterns = [
                r'(?:(?:\+|00)33|0)\s*[1-9](?:[\s.-]*\d{{2}}){{4}}',  # Format standard
                r'(?:(?:\+|00)33|0)\s*[1-9](?:&nbsp;\d{{2}}){{4}}',   # Format avec &nbsp;
            ]
            
            # Trouver tous les emails
            all_emails = set(re.findall(email_pattern, text_content))
            
            # Trouver tous les numéros de téléphone
            all_phones = set()
            for pattern in phone_patterns:
                phones = re.findall(pattern, text_content)
                all_phones.update(phones)
            
            # Décodage HTML pour les emails
            decoded_emails = set(html.unescape(email) for email in all_emails)
            
            # Nettoyage des numéros de téléphone
            cleaned_phones = set()
            for phone in all_phones:
                # Nettoyer le numéro
                clean_phone = re.sub(r'[^\d+]', '', phone)
                
                # Convertir +33 en 0
                if clean_phone.startswith('+33'):
                    clean_phone = '0' + clean_phone[3:]
                elif clean_phone.startswith('0033'):
                    clean_phone = '0' + clean_phone[4:]
                
                # Vérifier si c'est un numéro français valide
                if len(clean_phone) == 10 and clean_phone.startswith(('01', '02', '03', '04', '05', '06', '07', '08', '09')):
                    # Formater le numéro
                    formatted_phone = f"{{clean_phone[0:2]}} {{clean_phone[2:4]}} {{clean_phone[4:6]}} {{clean_phone[6:8]}} {{clean_phone[8:10]}}"
                    cleaned_phones.add(formatted_phone)
            
            if decoded_emails:
                print(f"Emails trouvés sur {{response.url}}: {{list(decoded_emails)}}", flush=True)
                self.all_emails.update(decoded_emails)
            
            if cleaned_phones:
                print(f"Téléphones trouvés sur {{response.url}}: {{list(cleaned_phones)}}", flush=True)
                self.all_phones.update(cleaned_phones)
            
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

def run_scraper(url):
    process.crawl(EmailSpider, url=url)
    process.start()

# Search Function (Pseudo-code, please replace with actual implementation)
def search_emails_in_engines(emails):
    # This function would perform searches on specified engines and return results.
    found_emails = []
    for email in emails:
        # Replace with your actual search logic for Google, Bing, etc.
        print(f"Searching for {email} in search engines...")
        # Simulate search:
        found_emails.append(email)  # This is a placeholder; replace with actual results.
    return found_emails

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
                results_str = line.replace("FINAL_RESULTS:", "").strip()
                try:
                    results = eval(results_str)
                    # Filtrer les emails ici
                    filtered_emails = filter_emails(results['emails'])
                    return filtered_emails, results['phones']
                except:
                    return "", ""
        
        return "", ""
        
    except subprocess.TimeoutExpired:
        process.kill()
        return "Timeout", "Timeout"
    except Exception as e:
        print(f"Erreur dans process_single_url: {str(e)}")
        return f"Erreur: {str(e)}", f"Erreur: {str(e)}"

def read_csv_with_encoding(uploaded_file):
    # Lire les 100 premiers KB pour détecter l'encodage
    raw_data = uploaded_file.read(100000)
    uploaded_file.seek(0)  # Retour au début du fichier

    # Détecter l'encodage
    result = chardet.detect(raw_data)
    detected_encoding = result['encoding']

    # Lire le fichier avec l'encodage détecté
    df = pd.read_csv(uploaded_file, encoding=detected_encoding)
    return df
    
def process_csv(df, progress_bar, status_text):
    """Traite le fichier CSV avec du multiprocessing"""
    df_result = df.copy()
    
    if 'Mail' not in df_result.columns:
        df_result['Mail'] = ''
    if 'Telephone' not in df_result.columns:
        df_result['Telephone'] = ''
    
    # Obtenir la liste des URLs à traiter
    urls = df['URL'].dropna().tolist()
    total_urls = len(urls)
    processed = 0
    
    # Définir le nombre maximum de workers
    max_workers = min(multiprocessing.cpu_count() * 2, 8)  # 8 workers maximum
    
    # Taille des lots (batch)
    batch_size = 100
    
    # Traiter par lots
    for start_idx in range(0, total_urls, batch_size):
        end_idx = min(start_idx + batch_size, total_urls)
        batch_urls = urls[start_idx:end_idx]
        
        status_text.text(f"Traitement du lot {start_idx//batch_size + 1}/{(total_urls-1)//batch_size + 1}...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(process_single_url, url): (i + start_idx, url) 
                             for i, url in enumerate(batch_urls)}
            
            for future in as_completed(future_to_url):
                index, url = future_to_url[future]
                try:
                    emails, phones = future.result()
                    # Filtrer les emails
                    emails = filter_emails(emails)
                    # Mise à jour du DataFrame
                    df_result.loc[index, 'Mail'] = emails
                    df_result.loc[index, 'Telephone'] = phones
                except Exception as e:
                    df_result.loc[index, 'Mail'] = f"ERREUR: {str(e)}"
                    df_result.loc[index, 'Telephone'] = ""
                
                processed += 1
                progress_bar.progress(processed / total_urls)
                status_text.text(f"Traitement des URLs... {processed}/{total_urls}")
    
    return df_result

def main():
    st.title("Scraper d'adresses email et numéros de téléphone")

    if 'crawler_running' not in st.session_state:
        st.session_state.crawler_running = False

    col1, col2 = st.columns(2)

    with col1:
        url_input = st.text_input("Entrez une URL à scraper :")
        if st.button("Scanner une URL"):
            if url_input:
                try:
                    with st.spinner('Scan en cours...'):
                        emails, phones = process_single_url(url_input)
                        if emails or phones:
                            if emails:
                                st.success(f"Adresses email trouvées : {emails}")
                            if phones:
                                st.success(f"Numéros de téléphone trouvés : {phones}")
                        else:
                            st.warning("Aucune adresse email ni numéro de téléphone trouvé.")
                except Exception as e:
                    st.error(f"Erreur : {str(e)}")
            else:
                st.error("Veuillez entrer une URL valide.")

    with col2:
        uploaded_file = st.file_uploader("Ou téléchargez un fichier CSV", type=['csv'])
        if uploaded_file is not None:
            try:
                # Lire le CSV
                df = read_csv_with_encoding(uploaded_file)
                
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
                        phones_found = results_df['Telephone'].notna().sum()
                        success_rate = ((emails_found + phones_found) / (total_urls * 2)) * 100
                        
                        # Afficher les statistiques
                        st.write(f"URLs traitées avec succès : {total_urls}")
                        st.write(f"Emails trouvés : {emails_found}")
                        st.write(f"Téléphones trouvés : {phones_found}")
                        st.write(f"Taux de succès : {success_rate:.2f}%")
                        
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
- La page mentions légales (/mentions-legales/)

Les numéros de téléphone détectés sont uniquement les numéros français valides (10 chiffres commençant par 01-09).
""")

if __name__ == "__main__":
    main()
