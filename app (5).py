
import streamlit as st
import requests
import re
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, String, MetaData, Table, insert, select, and_
from playwright.sync_api import sync_playwright

# -------------------- DATABASE SETUP --------------------

engine = create_engine("sqlite:///leads.db")
metadata = MetaData()

leads_table = Table(
    "leads", metadata,
    Column("ville", String),
    Column("nom", String),
    Column("url", String),
    Column("email", String),
)

metadata.create_all(engine)


# -------------------- EMAIL SCRAPER --------------------

def extract_emails_from_url(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text()

        emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)

        for a in soup.find_all("a", href=True):
            if a["href"].startswith("mailto:"):
                email = a["href"].replace("mailto:", "").split("?")[0]
                emails.append(email)

        return list(set(emails))
    except:
        return []


# -------------------- GOOGLE MAPS SCRAPER --------------------

def scrape_city(query_base, city, max_results=10):
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        )
        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        search_url = f"https://www.google.com/maps/search/{query_base}+{city}".replace(" ", "+")
        page.goto(search_url)
        time.sleep(5)

        for _ in range(5):
            page.mouse.wheel(0, 1500)
            time.sleep(1)

        listings = page.locator(".hfpxzc")
        count = listings.count()

        for i in range(min(count, max_results)):
            try:
                listings.nth(i).click(timeout=5000)
                time.sleep(4)

                website_btn = page.locator('a[aria-label^="Site Web"]')
                if website_btn.count():
                    url = website_btn.first.get_attribute("href")
                    if url:
                        url = url.split("?")[0]
                        nom = page.locator("h1").first.inner_text()

                        emails = extract_emails_from_url(url)
                        for email in emails:
                            results.append((city, nom, url, email))

                page.go_back()
                time.sleep(random.uniform(1.5, 2.5))
            except:
                continue

        browser.close()

    return results


# -------------------- INTERFACE STREAMLIT --------------------

st.title("📬 Scraper Google Maps + Emails")
st.write("Entrez un métier et une liste de villes pour scraper automatiquement les sites et emails depuis Google Maps.")

metier = st.text_input("🔧 Métier à rechercher", "couvreur")
villes = st.text_area("🏙️ Liste des villes (une par ligne)", "Chartres\nOrléans\nTours")

max_fiches = st.slider("📊 Nombre de fiches max par ville", 5, 30, 10)

if st.button("🚀 Lancer le scraping"):
    with st.spinner("Scraping en cours..."):
        for ville in villes.splitlines():
            ville = ville.strip()
            if ville:
                st.write(f"🔍 **{ville}** en cours...")

                results = scrape_city(metier, ville, max_results=max_fiches)

                with engine.connect() as conn:
                    for ville, nom, url, email in results:
                        stmt = select(leads_table).where(
                            and_(
                                leads_table.c.nom == nom,
                                leads_table.c.ville == ville,
                                leads_table.c.email == email
                            )
                        )
                        if not conn.execute(stmt).fetchone():
                            ins = insert(leads_table).values(ville=ville, nom=nom, url=url, email=email)
                            conn.execute(ins)

        st.success("✅ Scraping terminé !")

# -------------------- AFFICHAGE --------------------

with engine.connect() as conn:
    df = pd.read_sql("SELECT * FROM leads", conn)

st.markdown("## 📋 Résultats enregistrés")
st.dataframe(df, use_container_width=True)
