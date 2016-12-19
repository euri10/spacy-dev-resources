import click
import re
import requests
from urllib.parse import urljoin
from lxml import html

WIKIDL_BASE = "https://dumps.wikimedia.org/"


def download_file(url):
    local_filename = 'results/'+ url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    return local_filename


@click.command()
@click.option('--directory', default='latest', help='directory output')
@click.option('--language', default='fr', help='language you want to get')
def pywikidl(directory, language):
    """Download wikipedia articles"""
    base_lang = language + "wiki/" + directory
    url = urljoin(WIKIDL_BASE, base_lang)
    page = requests.get(url)
    webpage = html.fromstring(page.content)
    all_links = webpage.xpath('//a/@href')
    print(all_links)
    for link in all_links:
        # https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-pages-articles1.xml-p000000003p000412300.bz2
        pattern = language + "wiki-latest-pages-articles\d+.xml-p\d*p\d*.bz2$"
        if re.match(pattern=pattern, string=link):
            url_dl = url + '/' + link
            download_file(url_dl)

if __name__ == '__main__':
    pywikidl()

