# coding: utf-8
import scraperwiki, lxml.html, cssselect
import re

user_agent = 'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'

def parse_lettera(url):
    html = scraperwiki.scrape(url, None, user_agent)
    root = lxml.html.fromstring(html)
    anchors = root.cssselect('div.post-content ul li a')
    for idx, item in enumerate(anchors):
        record = {
            'id' : '{:s}{:03d}'.format(url[-1], idx),
            'autore' : '',
            'ordinamento' : item.text_content(),
            'elenco' : '',
            'url' : item.attrib['href'],
            }
        scraperwiki.sqlite.save(unique_keys=['id'], data=record, table_name='autori')

def parse_scheda_autore(author_id, url): 
    html = scraperwiki.scrape(url, None, user_agent)
    root = lxml.html.fromstring(html)
      
    anchors = root.cssselect('span.ll_autore_elenco_opera_titolo a')
    for idx, item in enumerate(anchors):
        record = {
            'id' : '{:s}-{:04d}'.format(author_id, idx),
            'autore_id' : author_id,
            'titolo' : item.text,
            'url' : item.attrib['href'],
            'titolo per ordinamento' : '',
            }
        scraperwiki.sqlite.save(unique_keys=['id'], data=record, table_name='opere')

    record = {'id' : author_id, 'url' : url}
    headers = ['autore', 'ordinamento', 'elenco']
    for item in headers:
        divs = root.cssselect('div.ll_metadati_etichetta:contains("' + item + ':") + div.ll_metadati_dato')
        if divs: record[item] = divs[0].text_content()
    if len(record) > 2: scraperwiki.sqlite.save(unique_keys=['id'], data=record, table_name='autori')

def parse_scheda_opera(book_id, author_id, url):      
    html = scraperwiki.scrape(url, None, user_agent)
    root = lxml.html.fromstring(html)

    for idx, item in enumerate(root.cssselect('div.post-content div.ll_opera_riga:contains("Scarica gratis") ~ a')):
        imgs = item.cssselect('img')        
        record = {
            'id' : '{:s}-{:05d}'.format(book_id, idx),
            'opera_id' : book_id,  
            'formato' : imgs[0].attrib['alt'].lower(),
            'url' : item.attrib['href'],
            }
        scraperwiki.sqlite.save(unique_keys=['id'], data=record, table_name='file')
 
    for idx, item in enumerate(root.cssselect('ul.ll_musica_elenco_mp3 li a, ul.ll_musica_elenco_ogg li a')):
        record = {
            'id' : '{:s}-{:02d}'.format(book_id, idx),
            'opera_id' : book_id,
            'brano' : item.text_content(),
            'formato' : item.attrib['href'][-3:],
            'url' : item.attrib['href'],
            }
 
    record = {'id' : book_id, 'autore_id' : author_id, 'url' : url}
    headers = [u'titolo',
            u'sottotitolo',
            u'titolo per ordinamento',
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
            u'album',
            u'artista',
            u'etichetta',
            u'genere',
            u'tipo registrazione',
           ]
    
    for i in headers:
        for j in root.cssselect(u'div.ll_metadati_etichetta:contains("' + i + u':")'):
            if j.text_content() == 'soggetto BISAC:':
                for l in filter(None, re.split('([A-Z, ]+ / .*?[a-z](?=[A-Z]))', j.getnext().text_content())):
                    make_bisac(book_id, l)
            elif j.text_content() == i + ':':
                record[i] = j.getnext().text_content()

    if len(record) > 3:
        scraperwiki.sqlite.save(unique_keys=['id'], data=record, table_name='opere')

def make_bisac(book_id, bisac):
    try:
        bisac_id = scraperwiki.sql.select('id as n FROM bisac WHERE bisac IS "' + bisac + '"')
        if not bisac_id:
            bisac_id = scraperwiki.sql.select('COUNT(id) as n FROM bisac')
            record = {
                'id' : bisac_id[0]['n'],
                'bisac' : bisac
            }
            scraperwiki.sql.save(unique_keys=['id'], data=record, table_name='bisac')
            
        rel_id = scraperwiki.sql.select('COUNT(id) as n FROM rel_bisac_opere')
        record = {
            'id' : rel_id[0]['n'],
            'bisac_id' : bisac_id[0]['n'],
            'opera_id' : book_id,
        }
        scraperwiki.sql.save(unique_keys=['id'], data=record, table_name='rel_bisac_opere')
    except:
        scraperwiki.sql.save(unique_keys=['id'], data={'id':0,'bisac':bisac}, table_name='bisac')
        record = {
            'id' : 0,
            'bisac_id' : 0,
            'opera_id' : book_id
            }
        scraperwiki.sql.save(unique_keys=['id'], data=record, table_name='rel_bisac_opere')
        
def build_autori(letters='#ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
    for i in letters: parse_lettera('https://www.liberliber.it/online/autori/autori-' + i)
        
def build_opere():
    authors = scraperwiki.sql.select('id, url FROM autori WHERE autore IS NULL OR autore = ""')
    for i in authors: parse_scheda_autore(i['id'], i['url'])

def build_file():
    books = scraperwiki.sql.select('id, autore_id, url FROM opere \
        WHERE "titolo per ordinamento" IS NULL OR "titolo per ordinamento" = ""')
    for i in books: parse_scheda_opera(i['id'], i['autore_id'], i['url'])

build_autori()
build_opere()
build_file()

