# Based on wikicorpus.py from Gensim:
#   https://github.com/piskvorky/gensim/blob/develop/gensim/corpora/wikicorpus.py
# Credits:
#   Radim Rehurek <radimrehurek@seznam.cz>
#   Lars Buitinck <larsmans@gmail.com>

import re
from xml.etree.cElementTree import iterparse  # LXML isn't faster, so let's go with the built-in solution

from html.entities import name2codepoint

from gensim.utils import to_unicode, decode_htmlentities

wikilink_prefix = 'en.wikipedia.org/wiki/'

RE_P0 = re.compile('<!--.*?-->', re.DOTALL | re.UNICODE) # comments
RE_P1 = re.compile('<ref([> ].*?)(</ref>|/>)', re.DOTALL | re.UNICODE) # footnotes
RE_P2 = re.compile("(\n\[\[[a-z][a-z][\w-]*:[^:\]]+\]\])+$", re.UNICODE) # links to languages
RE_P3 = re.compile("{{([^}{]*)}}", re.DOTALL | re.UNICODE) # template
RE_P4 = re.compile("{{([^}]*)}}", re.DOTALL | re.UNICODE) # template
RE_P5 = re.compile('\[(\w+):\/\/(.*?)(( (.*?))|())\]', re.UNICODE) # remove URL, keep description
RE_P6 = re.compile("\[\[:?([^][]*)\|([^][]*)\]\]", re.DOTALL | re.UNICODE) # simplify links, keep description
RE_P6_ex = re.compile("\[\[:?([^][]*)\]\]", re.DOTALL | re.UNICODE) # links without description
RE_P7 = re.compile('\n\[\[[iI]mage(.*?)(\|.*?)*\|(.*?)\]\]', re.UNICODE) # keep description of images
RE_P8 = re.compile('\n\[\[[fF]ile(.*?)(\|.*?)*\|(.*?)\]\]', re.UNICODE) # keep description of files
RE_P9 = re.compile('<nowiki([> ].*?)(</nowiki>|/>)', re.DOTALL | re.UNICODE) # outside links
RE_P10 = re.compile('<math([> ].*?)(</math>|/>)', re.DOTALL | re.UNICODE) # math content
RE_P11 = re.compile('<(.*?)>', re.DOTALL | re.UNICODE) # all other tags
RE_P12 = re.compile('\n(({\|)|(\|-)|(\|}))(.*?)(?=\n)', re.UNICODE) # table formatting
RE_P13 = re.compile('\n(\||\!)(.*?\|)*([^|]*?)', re.UNICODE) # table cell formatting
RE_P14 = re.compile('\[\[Category:[^][]*\]\]', re.UNICODE) # categories
RE_P15 = re.compile('\[\[([fF]ile:|[iI]mage)[^]]*(\]\])', re.UNICODE)

RE_BI = re.compile(r"'''''([^']*?)'''''")
RE_B = re.compile(r"'''(.*?)'''")
RE_IQ = re.compile(r"''\"(.*?)\"''")
RE_I = re.compile(r"''([^']*)''")
RE_QQ = re.compile(r'""(.*?)""')
RE_SECT = re.compile(r'(==+)\s*(.*?)\s*\1')
RE_EMPTY_PARENS = re.compile(r' \(\s*\)')

RE_HTML_ENT = re.compile("&#?(\w+);")

def filter_wiki(raw):
    """
    Filter out wiki mark-up from `raw`, leaving only text. `raw` is either unicode
    or utf-8 encoded string.
    """
    # parsing of the wiki markup is not perfect, but sufficient for our purposes
    # contributions to improving this code are welcome :)
    text = to_unicode(raw, 'utf8', errors='ignore')
    text = decode_htmlentities(text)  # '&amp;nbsp;' --> '\xa0'
    return remove_markup(text)

def remove_markup(text):
    text = re.sub(RE_P2, "", text)

    # TODO: may be desirable to extract captions for files and images and insert them back into the document
    text = remove_template(text)
    text = extract_tag_content(text, [
        re.compile('\[\[[fF]ile:(.*?)(\|[^\]\[]+?)*\|'),
        re.compile('\[\[[iI]mage:(.*?)(\|[^\]\[]+?)*\|')
    ])

    # the wiki markup is recursive (markup inside markup etc) we deal with that by removing
    # markup in a loop, starting with inner-most expressions and working outwards as long as something changes.
    iters = 0
    while True:
        old, iters = text, iters + 1
        text = re.sub(RE_P0, "", text) # remove comments
        text = re.sub(RE_P1, '', text) # remove footnotes
        text = re.sub(RE_P9, "", text) # remove outside links
        text = re.sub(RE_P10, "", text) # remove math content
        text = re.sub(RE_P11, "", text)  # remove all remaining tags

        text = re.sub(RE_P14, '', text) # remove categories

        # inject links
        text = re.sub(RE_P5, '<a href="\\2">\\3</a>', text) # remove urls, keep description
        text = re.sub(RE_P6, '<a href="%s\\1">\\2</a>' % wikilink_prefix, text) # simplify links, keep description only
        text = re.sub(RE_P6_ex, '<a href="%s\\1">\\1</a>' % wikilink_prefix, text)
        # remove table markup
        text = text.replace('||', '\n|') # each table cell on a separate line
        text = re.sub(RE_P12, '\n', text) # remove formatting lines
        text = re.sub(RE_P13, '\n\\3', text) # leave only cell content
        # remove empty mark-up
        text = text.replace('[]', '')

        # formatting
        text = re.sub(RE_BI, r"\1", text)
        text = re.sub(RE_B, r"\1", text)
        text = re.sub(RE_IQ, r'&quot;\1&quot;', text)
        text = re.sub(RE_I, r'&quot;\1&quot;', text)
        text = re.sub(RE_QQ, r"\1", text)

        if old == text or iters > 2: # stop if nothing changed between two iterations or after a fixed number of iterations
            break

    text = re.sub(RE_EMPTY_PARENS, '', text) # remove empty parenthesis (usually left by stripped templates)
    text = text.replace('[', '').replace(']', '') # promote all remaining markup to plain text
    text = html_unescape(text.strip())
    return text

def remove_template(s):
    # Find the start and end position of each template by finding the opening '{{' and closing '}}'
    n_open, n_close = 0, 0
    starts, ends = [], []
    in_template = False
    prev_c = None
    for i, c in enumerate(iter(s)):
        if not in_template:
            if c == '{' and c == prev_c:
                starts.append(i - 1)
                in_template = True
                n_open = 1
        if in_template:
            if c == '{':
                n_open += 1
            elif c == '}':
                n_close += 1
            if n_open == n_close:
                ends.append(i)
                in_template = False
                n_open, n_close = 0, 0
        prev_c = c

    # Remove all the templates
    s = ''.join([s[end + 1:start] for start, end in
                 zip(starts + [None], [-1] + ends)])

    return s

def extract_tag_content(s, tags, include_content=True):
    s = s.replace(u'\u2502','|')
    for t in tags:
        parts = []
        last_match_end = None
        for match in t.finditer(s):
            parts.append(slice(last_match_end,match.start()))

            i = match.end()
            while True:
                next_open = s.find('[[', i)
                next_close = s.find(']]', i)
                if next_open == -1 or next_open > next_close:
                    last_match_end = next_close
                    break
                elif next_close == -1:
                    # unbalanced tags in wikimarkup, bail!
                    last_match_end = i
                    break
                i = next_close+2
            if include_content and match.end() != last_match_end:
                content = s[match.end():last_match_end].strip('] ')
                if content:
                    parts.append(slice(match.end(),last_match_end))
                    if not content.endswith('.'):
                        parts.append('.')
            last_match_end += 2
        parts.append(slice(last_match_end,None))
        s = ''.join(s[p] if type(p) is slice else p for p in parts)

    return s

def html_unescape(text):
    def replace(m):
        span, code = m.group(0), m.group(1)
        try:
            if span[1] == "#":
                return chr(int(code[1:], 16)) if span[2] == "x" else chr(int(code))
            else:
                return chr(name2codepoint[code])
        except:
            return span
    return re.sub(RE_HTML_ENT, replace, text)

def extract_pages(f, filter_namespaces=False):
    """
    Extract pages from a MediaWiki database dump = open file-like object `f`.

    Return an iterable over (str, str, str) which generates (title, content, pageid) triplets.

    """
    elems = (elem for _, elem in iterparse(f, events=("end",)))

    # We can't rely on the namespace for database dumps, since it's changed
    # it every time a small modification to the format is made. So, determine
    # those from the first element we find, which will be part of the metadata,
    # and construct element paths.
    elem = next(elems)
    namespace = get_namespace(elem.tag)
    ns_mapping = {"ns": namespace}
    page_tag = "{%(ns)s}page" % ns_mapping
    text_path = "./{%(ns)s}revision/{%(ns)s}text" % ns_mapping
    title_path = "./{%(ns)s}title" % ns_mapping
    ns_path = "./{%(ns)s}ns" % ns_mapping
    pageid_path = "./{%(ns)s}id" % ns_mapping

    for elem in elems:
        if elem.tag == page_tag:
            title = elem.find(title_path).text
            text = elem.find(text_path).text

            if filter_namespaces:
                ns = elem.find(ns_path).text
                if ns not in filter_namespaces:
                    text = None

            pageid = elem.find(pageid_path).text
            yield title, text or "", pageid     # empty page will yield None

            # Prune the element tree, as per
            # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
            # except that we don't need to prune backlinks from the parent
            # because we don't use LXML.
            # We do this only for <page>s, since we need to inspect the
            # ./revision/text element. The pages comprise the bulk of the
            # file, so in practice we prune away enough.
            elem.clear()
_extract_pages = extract_pages  # for backward compatibility

def normalise_wikilink(s):
    s = s.replace(' ', '_').strip('_').strip()
    if s and s[0].islower():
        s = s[0].upper() + s[1:]
    return s

def normalise_link(s):
    if s.startswith(wikilink_prefix):
        s = wikilink_prefix + normalise_wikilink(s[len(wikilink_prefix):])
    return s

def extract_links(content):
    links_re = re.compile(r'<a href="(.+?)">(.+?)</a>')

    links = []
    offset = 0
    for match in list(links_re.finditer(content)):
        target = match.group(1)
        anchor = match.group(2)
        start = match.start() - offset
        offset += len(match.group())-len(anchor)
        links.append((normalise_link(target), slice(start, start+len(anchor))))

    return links_re.sub(r'\2', content), links
    return links_re.sub(r'\2', content), links