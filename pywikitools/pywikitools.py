import bz2
import multiprocessing
import os
import uuid

import click
import re
import logging

import numpy as np
import requests
from urllib.parse import urljoin, urlparse

from gensim.corpora import Dictionary
from gensim.corpora import MmCorpus
from gensim.corpora import WikiCorpus
from gensim.corpora.wikicorpus import extract_pages
from gensim.utils import chunkize
from lxml import html

from sift_wiki import filter_wiki

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
    filelist = ['/home/lotso/PycharmProjects/spacy-dev-resources/pywikitools/dl_latest/frwiki-latest-pages-articles0.xml-p000000003p000412300.bz2']
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
            # `id[TAB]word_utf8[TAB]document frequency[NEWLINE]`
            # spacy needs `count word[TAB]count documents[TAB]word_utf8[NEWLINE]`
            # load back the id->word mapping directly from file
            # this seems to save more memory, compared to keeping the wiki.dictionary object from above
            dictionary = Dictionary.load_from_text(output_wordids)
            del wiki

def my_process_article(args):
    """
    Parse a wikipedia article, returning its content as a file without all the garbage
    """
    text, lemmatize, title, pageid = args
    text = filter_wiki(text)
    result = text
    # if lemmatize:
    #     result = utils.lemmatize(text)
    # else:
    #     result = tokenize(text)
    return result, title, pageid


@click.command()
@click.option('--input_directory', '-di', default='dl_latest', type=click.Path(exists=True),
              help='directory input of wiki bz2 files')
@click.option('--output_directory', '-do', default='extract_output', type=click.Path(exists=True),
              help='directory extraction output')
@click.option('--lemmatize', default=False)
@click.option('--default_disk_size', default=DEFAULT_DICT_SIZE)
def extract_articles(input_directory, output_directory, lemmatize, default_disk_size):
    filelist = os.listdir(input_directory)
    # used for quick debug, short bz2 extract
    filelist = ['/home/lotso/PycharmProjects/spacy-dev-resources/pywikitools/dl_latest/frwiki-latest-pages-articles0.xml-p000000003p000412300.bz2']
    for file in filelist:
        if os.path.exists(os.path.join(input_directory, file)):
            filename, file_extension = os.path.splitext(os.path.basename(file))
            # wiki = WikiCorpus(os.path.join(input_directory, file))
            # wiki.save(os.path.join(output_directory, filename + '_corpus.pkl.bz2'))
            inp = os.path.join(input_directory, file)
            texts = ((text, lemmatize, title, pageid) for title, text, pageid in extract_pages(bz2.BZ2File(inp)))
            processes = max(1, multiprocessing.cpu_count() - 1)
            pool = multiprocessing.Pool(processes)
            # process the corpus in smaller chunks of docs, because multiprocessing.Pool
            # is dumb and would load the entire input into RAM at once...
            for group in chunkize(texts, chunksize=10 * processes, maxsize=1):
                for tokens, title, pageid in pool.map(my_process_article, group):  # chunksize=10):
                    with open(os.path.join(
                            '/home/lotso/PycharmProjects/spacy-dev-resources/pywikitools/extract_output/text',
                            title.replace('/','_')), 'w') as f:

                        f.write(tokens)
            pool.terminate()


@click.command()
def frequency():
    path = '/home/lotso/PycharmProjects/spacy-dev-resources/pywikitools/extract_output/frwiki-latest-pages-articles1.xml-p000000003p000412300_wordids.txt.bz2'
    dictionary = Dictionary.load_from_text(path)
    mm = MmCorpus('/home/lotso/PycharmProjects/spacy-dev-resources/pywikitools/extract_output/frwiki-latest-pages-articles1.xml-p000000003p000412300_bow.mm')
    cw = np.zeros(mm.num_terms)
    cd = np.zeros(mm.num_terms)
    warn = int(1/10 * mm.num_docs)
    for count_doc, doc in enumerate(mm):
        if count_doc % warn == 0:
            logger.info('{} over {}'.format(count_doc, mm.num_docs))
        for (word_id, word_freq) in doc:
            cw[word_id] += word_freq
            cd[word_id] += 1
    with open('/home/lotso/PycharmProjects/spacy-dev-resources/pywikitools/extract_output/freq.txt', 'w') as output:
        for i in range(mm.num_terms):
            output.write(str(cw[i])+'\t'+str(cd[i])+'\t'+dictionary[i]+'\n')


cli.add_command(download)
cli.add_command(extract)
cli.add_command(frequency)
cli.add_command(extract_articles)

if __name__ == '__main__':
    cli()
