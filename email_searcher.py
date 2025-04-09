import streamlit as st
from emailfinder.utils.finder import google, bing, baidu, yandex
from emailfinder.utils.exception import GoogleCaptcha, GoogleCookiePolicies, BaiduDetection, YandexDetection
from prompt_toolkit import print_formatted_text, HTML
from pyfiglet import Figlet
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import choice

# User-agent list
user_agents = {
    0: {'User-agent': 'Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Mobile Safari/537.36'},
    1: {'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'},
    2: {'User-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'},
    3: {'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Gecko/20100101 Firefox/89.0'},
    4: {'User-agent': 'Mozilla/5.0 (Linux; Ubuntu; Bionic; rv:64.0) Gecko/20100101 Firefox/64.0'},
}

SEARCH_ENGINES_METHODS = {
    "google": google.search,
    "bing": bing.search,
    "baidu": baidu.search,
    "yandex": yandex.search
}

# Streamlit User Interface
st.title("Email Finder")
st.write("Search for professional emails from a domain using multiple search engines.")

# Handle Input
domain = st.text_input("Enter the domain (e.g., company.com):")
proxy = st.text_input("Enter proxy (optional):")

if st.button("Search Emails"):
    if domain:
        proxy_dict = None
        if proxy:
            st.write('Using Proxies')
            proxy_dict = {"http": proxy, "https": proxy}

        emails = set()
        def search(engine, target, proxy_dict):
            try:
                st.write(f"Searching in {engine}...")
                found_emails = SEARCH_ENGINES_METHODS[engine](target, proxies=proxy_dict)
                if found_emails:
                    st.write(f"{engine} done!")
                return found_emails
            except Exception as ex:
                st.error(f"{engine} error: {ex}")
                return set()

        threads = 4
        with ThreadPoolExecutor(max_workers=threads) as executor:
            future_emails = {executor.submit(search, engine, domain, proxy_dict): engine for engine in SEARCH_ENGINES_METHODS.keys()}
            for future in as_completed(future_emails):
                data = future.result()
                if data:
                    emails = emails.union(data)

        total_emails = len(emails)
        st.write(f"\nTotal emails found: {total_emails}")
        if total_emails > 0:
            for email in emails:
                st.write(email)
        else:
            st.write("No emails found.")

# Displaying a banner (optional)
def show_banner():
    author = "@JosueEncinar"
    description = "Search emails from a domain through search engines."
    usage_example = "emailfinder -d domain.com"
    fonts = ["graffiti", "smshadow", "standard", "cosmic", "speed", "epic"]
    custom_banner = Figlet(font=choice(fonts))
    banner = custom_banner.renderText("eFnDR")
    st.markdown(f"<pre>{banner}</pre>", unsafe_allow_html=True)
    st.write(f"|_ Author: {author}")
    st.write(f"|_ Description: {description}")
    st.write(f"|_ Usage: {usage_example}")

# Optionally show banner
if st.checkbox("Show Banner"):
    show_banner()
