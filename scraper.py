# -*- coding: utf-8 -*-
"""Morph.io scraper for https://www.liberliber.it/"""

import logging
import os
import re
import ssl # Usato solo per workaround CERTIFICATE_VERIFY_FAILED
import urllib.error
from datetime import datetime
from functools import lru_cache
from random import uniform
from time import sleep

import lxml
import scraperwiki
from bs4 import BeautifulSoup as bs
from sqlalchemy.exc import OperationalError


# Workaround for: urllib.error.URLError: <urlopen error 
# [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:748)>
ssl._create_default_https_context = ssl._create_unverified_context


# morph.io requires this db filename, but scraperwiki doesn't nicely
# expose a way to alter this.
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'

    
def soupify(url, parser='html.parser'):
    """Scarica un url e restituisce un oggetto BeautifulSoup."""
    
    user_agent = 'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'
    
    sleep(round(uniform(1, 3), 2)) # Non stressare il server
    
    try:
        content = scraperwiki.scrape(url, None, user_agent)
    except urllib.error.HTTPError as e:
        logging.error(f'{e} on {url}')
        # Cosa fare in questo caso?
    else:
        return bs(content, parser)


def id_from_soup(soup):
    """Restituisce l'id della pagina a partire da un oggetto BeautifulSoup."""
    
    id = soup.find('link', rel='shortlink')['href'].replace('https://www.liberliber.it/online/?p=', '')
    
    try:
        return int(id)
    except ValueError:
        return id

    
@lru_cache(maxsize=128)
def get_author_id(url):
    """
    Cerca di recuperare l'id dell'autore nel db a partire dall'url della scheda.
    Se qualsiasi cosa va storta apre l'url e recupera l'id da lì. Usa lru_cache.
    """
    
    try:
        id = scraperwiki.sql.select(f'id FROM autori WHERE url IS "{url}"')[0]['id']
    except Exception:
        soup = soupify(url)
        id = scrape_author_data(soup)
        
    return id


def scrape_url(url):
    """
    Determina se l'url è relativo a una lettera, un autore o un'opera ed esegue
    la relativa funzione. Utilizzato con MODE url=... per recuperare pagine
    specifiche.
    """
    
    l = len(url.replace('https://www.liberliber.it/', '').rstrip('/').split('/'))
    # Lettera es. URL:
    # https://www.liberliber.it/online/autori/autori-a/
    #                              1     2       3
    if l == 3:
        scrape_letter(url)
    # Autore es. URL:
    # https://www.liberliber.it/online/autori/autori-a/antonio-abati/
    #                              1     2       3           4
    elif l == 4:
        scrape_author(url)
    # Opera es. URL:
    # https://www.liberliber.it/online/autori/autori-a/antonio-abati/delle-frascherie-di-antonio-abati-fasci-tre/
    #                              1     2       3           4                   5
    elif l == 5:
        scrape_book(url)
    else:
        logging.info(f"Can't identify URL {url}")


def scrape_letter(url):
    # Es. URL: https://www.liberliber.it/online/autori/autori-a/
    logging.info(f'Scraping URL: {url}')
    soup = soupify(url)
    for i in soup.select('div.post-content ul li a'):
        logging.info(f'Scraping author: {i.get_text()}')
        scrape_author(i['href'])


def scrape_author(url):
    """
    Cerca se sono presenti nella pagina opere dell'autore e nel caso raccoglie
    i dati sull'autore e i link delle opere sui quali esegue scrape_book().
    """
    
    # Es. URL https://www.liberliber.it/online/autori/autori-a/antonio-abati/
    soup = soupify(url)
    # Cerca se sono presenti nella pagina opere dell'autore
    # li.ll_autore_elenco_libro esclude la musica che ha come classe
    # ll_autore_elenco_musica
    anchors = soup.select('li.ll_autore_elenco_libro span.ll_autore_elenco_opera_titolo a')
    # Se non ci sono (lista vuota) ci fermiamo qui...
    if not anchors:
        logging.info(f'No books for this author found at {url}')
        return
    # ...altrimenti raccoglie i dati sull'autore
    if scrape_author_data(soup) is None:
        return # Non sono stati trovati dati sull'autore. Ci fermiamo qui.
    # adesso raccogliamo i dati sulle opere
    for i in anchors:
        scrape_book(i['href'])


def scrape_author_data(soup):
    """
    Cerca nella pagina le informazioni su nome autore, ordinamento ed elenco
    e crea una voce nella tabella autori. Restituisce l'id dell'autore (utile
    in get_author_id() oppure None se non sono stati trovati dati nella pagina
    es. Autore anonimo).
    """
    
    id = id_from_soup(soup)
    
    # L'URL va preso all'interno della pagina perché alla funzione passiamo il
    # Beautifulsoup Object
    url = soup.find('meta', property='og:url')['content']
    
    record = {'id': id, 'url': url}
    for item in ('autore', 'ordinamento', 'elenco'):
        # https://github.com/facelessuser/soupsieve/releases/tag/2.1.0
        # In order to avoid conflicts with future CSS specification changes, non-standard
        # pseudo classes will now start with the :-soup- prefix. As a consequence, :contains()
        # will now be known as :-soup-contains(), though for a time the deprecated form of
        # :contains() will still be allowed with a warning that users should migrate over to
        # :-soup-contains().
        divs = soup.select(f'div.ll_metadati_etichetta:-soup-contains("{item}:") + div.ll_metadati_dato')
        if divs:
            record[item] = divs[0].get_text()

    # Non processiamo le schede autore vuote Es.
    # https://www.liberliber.it/online/autori/autori-a/autore-anonimo/
    # Attenzione: le opere degli autori anonimi non vanno cercate qui.
    if len(record) < 3:
        logging.info(f'No author data found at {url}')
        return
    
    scraperwiki.sql.save(unique_keys=['id'], data=record, table_name='autori')
    return id


def scrape_book(url):
    """Cerca nella pagina le informazioni sull'opera e i file associati."""
    
    # Es. URL https://www.liberliber.it/online/autori/autori-a/antonio-abati/delle-frascherie-di-antonio-abati-fasci-tre/
    soup = soupify(url)
    
    # Prima verifichiamo se ci sono libri associati all'opera
    # anchors = soup.select('div.post-content div.ll_opera_riga:-soup-contains("Scarica gratis") ~ a')
    # Escludiamo link a yeerida e audiolibri => :has(:not(img.ll_ebook_epub_yeerida,img.redirect_libroparlato)
    anchors = soup.select('div.post-content div.ll_opera_riga:-soup-contains("Scarica gratis") ~ a:has(:not(img.ll_ebook_epub_yeerida,img.redirect_libroparlato))')
    # Se non ci sono (lista vuota) non andiamo oltre...
    if not anchors:
        logging.info(f'No books at {url}')
        return
    
    # ...altrimenti raccogliamo le informazioni sull'opera
    id = id_from_soup(soup)
    record = {'id': id, 'url': url}
    
    headers = (
            u'titolo',
            u'sottotitolo',
            u'titolo per ordinamento',
            u'autore',
            u'descrizione breve',
            u'opera di riferimento',
            u'licenza',
            u'cura',
            u'data pubblicazione',
            u'opera elenco',
            u'ISBN',
            u'soggetto BISAC',
            u'affidabilità',
            u'impaginazione',
            u'pubblicazione',
            u'revisione',
            u'traduzione',
            #u'album',
            #u'artista',
            #u'etichetta',
            #u'genere',
            #u'tipo registrazione',
            )
    
    for i in headers:
        for j in soup.select(u'div.ll_metadati_etichetta:-soup-contains("' + i + u':")'):
            if j.get_text() == 'soggetto BISAC:':
                for l in filter(None, re.split('([A-Z, ]+ / .*?[a-z](?=[A-Z]))', j.next_sibling.get_text())):
                    make_bisac(id, l)
            elif j.get_text() == 'autore:':
                # Del campo autore non ci serve il testo, ma il link che dopo useremo per recuperare l'id
                # da inserire in record['autore']
                try:
                    auth_url = j.next_sibling.find('a')['href']
                # ATTENZIONE! Atlante universale di geografia antica e moderna non ha link nel campo autore
                # https://www.liberliber.it/online/autori/autori-a/atlante-universale-di-geografia-antica-e-moderna/atlante-universale-di-geografia-antica-e-moderna/
                # Soluzione sporchissima: prendiamo l'url dell'autore partendo dall'url dell'opera sperando
                # che Liber Liber non cambi metodo di denominazione delle pagine.
                except TypeError: # se j.next_sibling.find('a') restituisce None non ha ['href']
                    auth_url =  url[:url.rstrip('/').rfind('/')]
            elif j.get_text() == i + ':':
                record[i] = j.next_sibling.get_text()

    # Non processiamo oltre le schede relative al traduttore o altri rimandi. Es.
    # https://www.liberliber.it/online/autori/autori-a/vittorio-alfieri/la-guerra-di-catilina-la-guerra-di-giugurta/
    # Vittorio Alfieri ha tradotto “La guerra di Catilina” e “La guerra di Giugurta” di Gaius Sallustius Crispus in italiano.
    if len(record) < 3:
        logging.info(f'No book data at {url}')
        return
    
    record['autore_id'] = get_author_id(auth_url)
    scraperwiki.sql.save(unique_keys=['id'], data=record, table_name='opere')
    
    # Poi i file associati all'opera
    for idx, item in enumerate(anchors):
        img = item.find('img')
        record = {
            'id': f'{id}-{idx}',
            'opera_id': id,
            'formato': img['alt'].lower(),
            'url': item['href'],
            }
        scraperwiki.sql.save(unique_keys=['id'], data=record, table_name='file')
    
    # File musicali
    # for idx, item in enumerate(soup.select('ul.ll_musica_elenco_mp3 li a, ul.ll_musica_elenco_ogg li a')):
        # record = {
            # 'id': f'{id}-m{idx}',
            # 'opera_id': id,
            # 'brano': item.get_text(),
            # 'formato': item['href'][-3:],
            # 'url': item['href'],
            # }
        # scraperwiki.sql.save(unique_keys=['id'], data=record, table_name='file')


def make_bisac(book_id, bisac):
    """
    Crea una voce di relazione tra book_id e bisac. Se la stringa bisac non è
    presente nel database crea la relativa voce.
    """
    
    # cerca se la stringa bisac è gia presente nel db
    try:
        bisac_id = scraperwiki.sql.select(f'id as n FROM bisac WHERE bisac IS "{bisac}"')[0]['n']
    except (OperationalError, IndexError) as e:
        # se non esiste ancora il db o la tabella impostiamo a 0 l'indice per 
        # la voce nella tabella bisac
        if isinstance(e, OperationalError):
            bisac_id = 0
        # se la stringa bisac non è ancora presente in tabella (scraperwiki.sql.select
        # ha restituito una lista vuota) contiamo quante righe ci sono nella tabella 
        # bisac per sapere quale id dobbiamo dare alla nuova voce
        else: # IndexError
            bisac_id = scraperwiki.sql.select('COUNT(id) as n FROM bisac')[0]['n']
        
        # quindi salviamo la nuova voce bisac
        record = {'id': bisac_id, 'bisac': bisac}
        scraperwiki.sql.save(unique_keys=['id'], data=record, table_name='bisac')
        
    
    # contiamo quante righe ci sono nella tabella di relazione per sapere quale 
    # id dobbiamo dare alla nuova voce
    try:
        rel_id = scraperwiki.sql.select('COUNT(id) as n FROM rel_bisac_opere')[0]['n']
    except OperationalError:
        # non esiste ancora il db o la tabella: impostiamo a 0 l'indice
        rel_id = 0
    
    # salviamo la voce di relazione per rel_bisac_opere
    record = {'id': rel_id, 'bisac_id': bisac_id, 'opera_id': book_id}
    scraperwiki.sql.save(unique_keys=['id'], data=record, table_name='rel_bisac_opere')


def build_db(letters='#ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
    """
    Funzione per la costituzione iniziale del database. Esegue scrape_letter()
    per ciascuna lettera fornita come parametro (su tutte le lettere se non si
    indica il parametro).
    """
    
    for i in letters:
        scrape_letter(f'https://www.liberliber.it/online/autori/autori-{i}')


def parse_feed(url='https://www.liberliber.it/online/feed/'):
    """
    Analizza il feed Atom di Liber Liber e cerca nei nuovi post link alle schede
    di opere da inserire nel database. Se li trova esegue scrape_book() su di
    essi. Salva nel database la data dell'ultimo post analizzato.
    Da utilizzare per l'aggiornamento quotidiano del database.
    """
    
    datetime_fmt = '%a, %d %b %Y %H:%M:%S %z'
    
    # saved_pubDate = scraperwiki.sql.get_var('lastpost') doesn't seem to work so...
    try:
        q = scraperwiki.sql.select('value FROM myvar WHERE name IS "lastpost"')
        saved_pubDate = q[0]['value']
    except (OperationalError, IndexError): # Index Error se db e tabella ci sono ma non il valore
        saved_pubDate = 'Sat, 01 Jan 2022 00:00:00 +0000'
    else:
        logging.info(f'Saved pubDate is {saved_pubDate}')
    
    saved_pubDate = datetime.strptime(saved_pubDate, datetime_fmt)
    tmp_pubDate = saved_pubDate
    soup = soupify(url, 'lxml')
    
    items = soup.find_all('item')
    for i in items:
        cur_pubDate = i.find('pubdate').get_text()
        cur_pubDate = datetime.strptime(cur_pubDate, datetime_fmt)
        if cur_pubDate > saved_pubDate:
            try:
                link = i.find('p', class_='ll_dl').find('a')['href']
            except AttributeError: #'NoneType' object has no attribute 'find'
                logging.info(f"Nothing to dowload at {i.find('title').get_text()}")
            else:
                logging.info(f'Found {link}')
                scrape_book(link)
            
            if cur_pubDate > tmp_pubDate:
                tmp_pubDate = cur_pubDate
                
    if tmp_pubDate > saved_pubDate:
        # scraperwiki.sql.save_var('lastpost', tmp_pubDate) doesn't seem to work
        tmp_pubDate = tmp_pubDate.strftime(datetime_fmt)
        scraperwiki.sql.save(unique_keys=['name'],
            data={'name': 'lastpost', 'value': tmp_pubDate},
            table_name='myvar')


def main():
    scrape_mode = os.environ.get('MORPH_MODE', 'feed')
    
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', 
        level=logging.INFO)
    
    logging.info('Started')
    
    if scrape_mode == 'feed':
        parse_feed()
    elif scrape_mode.startswith('url='):
        for i in scrape_mode[4:].split(','):
           print(i)
           scrape_url(i)
    elif scrape_mode.startswith('build='):
        build_db(scrape_mode[6:])
    else:
        build_db()
        
    logging.info('Finished')


if __name__ == "__main__":
    main()
