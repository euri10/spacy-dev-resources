import os
import click
import re
import logging
import requests
from urllib.parse import urljoin, urlparse

from gensim.corpora import Dictionary
from gensim.corpora import MmCorpus
from gensim.corpora import WikiCorpus
from gensim.scripts import make_wikicorpus
from lxml import html

logger = logging.getLogger(__name__)
logging.basicConfig()
logging.root.setLevel(level=logging.INFO)

WIKIDL_BASE = "https://dumps.wikimedia.org/"


def download_file(url, directory):
    local_filename = urlparse(url).path.split('/')[-1]
    fn = os.path.join(directory, local_filename)
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(fn, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    return fn


@click.group()
def cli():
    pass


@click.command()
@click.option('--directory', default='dl_latest', type=click.Path(exists=True),
              help='directory download output')
@click.option('--language', default='en', prompt=True,
              help='language you want to get')
def download(directory, language):
    """Download wikipedia articles"""
    base_lang = language + "wiki/latest"
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
            download_file(url_dl, directory)

DEFAULT_DICT_SIZE = 100000

@click.command()
@click.option('--input_directory', '-di', default='dl_latest', type=click.Path(exists=True),
              help='directory input of wiki bz2 files')
@click.option('--output_directory', '-do', default='extract_output', type=click.Path(exists=True),
              help='directory extraction output')
@click.option('--language', default='fr', prompt=True,
              help='language you want to get')
@click.option('--lemmatize', default=False)
@click.option('--default_disk_size', default=DEFAULT_DICT_SIZE)
def extract(input_directory, output_directory, language, lemmatize, default_disk_size):
    filelist = os.listdir(input_directory)
    # used for quick debug, short bz2 extract
    # filelist = ['/home/lotso/PycharmProjects/spacy-dev-resources/pywikitools/dl_latest/frwiki-latest-pages-articles0.xml-p000000003p000412300.bz2']
    for file in filelist:
        if os.path.exists(os.path.join(input_directory, file)):
            filename, file_extension = os.path.splitext(os.path.basename(file))
            # wiki = WikiCorpus(os.path.join(input_directory, file))
            # wiki.save(os.path.join(output_directory, filename + '_corpus.pkl.bz2'))
            inp = os.path.join(input_directory, file)
            wiki = WikiCorpus(inp, lemmatize=lemmatize)
            wiki.dictionary.filter_extremes(no_below=20, no_above=0.1, keep_n=default_disk_size)
            output_bowmm = os.path.join(output_directory, filename + '_bow.mm')
            MmCorpus.serialize(output_bowmm, wiki, progress_cnt=10000)
            output_wordids = os.path.join(output_directory, filename + '_wordids.txt.bz2')
            wiki.dictionary.save_as_text(output_wordids)
            # load back the id->word mapping directly from file
            # this seems to save more memory, compared to keeping the wiki.dictionary object from above
            dictionary = Dictionary.load_from_text(output_wordids)
            del wiki

cli.add_command(download)
cli.add_command(extract)

if __name__ == '__main__':
    cli()
