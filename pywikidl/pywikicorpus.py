import os
import sys
import logging

import click
from gensim.corpora import Dictionary
from gensim.corpora import HashDictionary
from gensim.corpora import MmCorpus
from gensim.corpora import WikiCorpus
from gensim.models import TfidfModel

DEFAULT_DICT_SIZE = 10000


@click.command()
@click.option('--inp', default='./results', help='input directory')
@click.option('--outp', default='./output', help='output directory')
@click.option('--keep_words', default=DEFAULT_DICT_SIZE,
              help='# of words in dictionary')
def pywikicorpus(inp, outp, keep_words):
    """Taken from gensim.scripts, adaptation with click and ability to run from a script for more flexibility"""
    program = os.path.basename(sys.argv[0])
    logger = logging.getLogger(program)

    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s')
    logging.root.setLevel(level=logging.INFO)
    logger.info("running %s" % ' '.join(sys.argv))

    if not os.path.isdir(os.path.dirname(outp)):
        raise SystemExit(
            "Error: The output directory does not exist. Create the directory and try again.")

    online = 'online' in program
    lemmatize = 'lemma' in program
    debug = 'nodebug' not in program

    for bz2article in os.listdir(inp):
        if online:
            dictionary = HashDictionary(id_range=keep_words, debug=debug)
            dictionary.allow_update = True  # start collecting document frequencies
            wiki = WikiCorpus(os.path.join(inp, bz2article),
                              lemmatize=lemmatize, dictionary=dictionary)
            MmCorpus.serialize(os.path.join(outp, bz2article) + '_bow.mm', wiki,
                               progress_cnt=10000)  # ~4h on my macbook pro without lemmatization, 3.1m articles (august 2012)
            # with HashDictionary, the token->id mapping is only fully instantiated now, after `serialize`
            dictionary.filter_extremes(no_below=20, no_above=0.1,
                                       keep_n=DEFAULT_DICT_SIZE)
            dictionary.save_as_text(
                os.path.join(outp, bz2article) + '_wordids.txt.bz2')
            wiki.save(os.path.join(outp, bz2article) + '_corpus.pkl.bz2')
            dictionary.allow_update = False
        else:
            wiki = WikiCorpus(os.path.join(inp, bz2article),
                              lemmatize=lemmatize)  # takes about 9h on a macbook pro, for 3.5m articles (june 2011)
            # only keep the most frequent words (out of total ~8.2m unique tokens)
            wiki.dictionary.filter_extremes(no_below=20, no_above=0.1,
                                            keep_n=DEFAULT_DICT_SIZE)
            # save dictionary and bag-of-words (term-document frequency matrix)
            MmCorpus.serialize(os.path.join(outp, bz2article) + '_bow.mm', wiki,
                               progress_cnt=10000)  # another ~9h
            wiki.dictionary.save_as_text(
                os.path.join(outp, bz2article) + '_wordids.txt.bz2')
            # load back the id->word mapping directly from file
            # this seems to save more memory, compared to keeping the wiki.dictionary object from above
            dictionary = Dictionary.load_from_text(
                os.path.join(outp, bz2article) + '_wordids.txt.bz2')
        del wiki

        # initialize corpus reader and word->id mapping
        mm = MmCorpus(os.path.join(outp, bz2article) + '_bow.mm')

        # build tfidf, ~50min
        tfidf = TfidfModel(mm, id2word=dictionary, normalize=True)
        tfidf.save(os.path.join(outp, bz2article) + '.tfidf_model')

        # save tfidf vectors in matrix market format
        # ~4h; result file is 15GB! bzip2'ed down to 4.5GB
        MmCorpus.serialize(os.path.join(outp, bz2article) + '_tfidf.mm',
                           tfidf[mm], progress_cnt=10000)

        logger.info("finished running %s" % program)


if __name__ == '__main__':
    pywikicorpus()
