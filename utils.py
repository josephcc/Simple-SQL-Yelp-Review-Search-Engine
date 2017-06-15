import re
import os
from commands import getoutput as CMD
from collections import defaultdict

from settings import *

META = defaultdict(lambda: defaultdict(dict))

class Thing:
    def __init__(self, **args):
        self.keys = []
        for key, val in args.items():
            setattr(self, key, val)
            self.keys.append(key)
    def __repr__(self):
        return {key:getattr(self, key) for key in self.keys}.__repr__()
    def __str__(self):
        return {key:getattr(self, key) for key in self.keys}.__str__()

def loadMetadata(conf, year):
    global META
    if conf == 'CHI':
        path = os.path.join(CHIPath, 'toc', '%d.blocks' % year)
    else:
        path = os.path.join(CSCWPath, 'toc', '%d.blocks' % year)
    blocks = open(path).read().split('--')
    blocks = [block.strip().splitlines() for block in blocks]
    blocks = filter(len, blocks)
    blocks = [{'title': block[0], 'authors': [x.strip() for x in block[1].split(',')], 'pages': block[2][7:], 'doi': block[3]} for block in blocks]
    for block in blocks:
        META[conf][year][int(block['pages'].split('-')[0])] = Thing(**block)

for year in range(2010, 2018):
    loadMetadata('CHI', year)

for year in range(2010, 2018):
    loadMetadata('CSCW', year)


skip = 0
def parsePaper(text, year, conf, pid):
    global skip
    if pid[0] != 'p':
         pid = 'p' + pid
    if '-' not in pid:
        pid = pid.replace('_', '-')
    year = int(year)
    page = int(pid.split('-')[0][1:])
    meta = META[conf][year][page]

    # do better
    if 'Author Keywords' in text:
        kwIdx = text.index('Author Keywords')
    elif 'Author  Keywords' in text:
        kwIdx = text.index('Author  Keywords')
    else:
        skip += 1
        return
    if 'ABSTRACT' in text:
        absIdx = text.index('ABSTRACT')
    elif 'Abstract' in text:
        absIdx = text.index('Abstract')
    else:
        skip += 1
        return
    acmIdx = text.find('ACM Cla')
    genIdx = text.find('\nGeneral ')
    introIdx = text.find('INTRODUCTION')

    if 'ACM Cla' in text and '\nGeneral ' in text and len(filter(lambda x: x > kwIdx, (acmIdx, genIdx))) > 0:
        kwIdx2 = min(filter(lambda x: x > kwIdx, (acmIdx, genIdx)))
    elif 'ACM Cla' in text and 'INTRODUCTION' in text and len(filter(lambda x: x > kwIdx, (acmIdx, introIdx))) > 0:
        kwIdx2 = min(filter(lambda x: x > kwIdx, (acmIdx, introIdx)))
    elif 'ACM Cla' in text:
        kwIdx2 = text.index('ACM Cla')
    elif '\nGeneral ' in text:
        kwIdx2 = text.index('\nGeneral ')
    elif 'INTRODUCTION' in text:
        kwIdx2 = text.index('INTRODUCTION')
    else:
        skip += 1
        print 'cl'
        return

    header = text[0:absIdx]
    keywords = text[kwIdx + 15:kwIdx2]
    keywords = keywords.strip(' \t\n:.,;')
    keywords = keywords.replace('\n', ' ')
    for i in range(10):
        keywords = keywords.replace('  ', ' ')
    keywords = keywords.replace('- ', '')
    if 'Permission to make' in keywords:
        keywords = keywords[:keywords.index('Permission to make')]
    if 'ACM Cla' in keywords:
        keywords = keywords[:keywords.index('ACM Cla')]
    keywords = re.split(r'[,.;]', keywords)
    keywords = map(lambda s: s.lower().strip(), keywords)
    keywords = filter(len, keywords)
    keywords = filter(lambda s: len(s) < 30, keywords)

    abstract = text[absIdx+9:kwIdx].strip()
    abstract = abstract.replace('\n', ' ')
    for i in range(10):
        abstract = abstract.replace('  ', ' ')
    abstract = abstract.replace('- ', '')


    return abstract, keywords, meta.title, meta.authors

def cleanText(text):
    text = text.replace('-\n', '')
    if 'references' in text.lower():
        idx = text.lower().rfind('refernces')
        if idx > 1000:
            text = text[:idx]
    return text


def iter_path(confPath):
    years = filter(lambda s: s.isdigit(), os.listdir(os.path.join(confPath, 'txt')))
    for year in years:
        txts = os.listdir(os.path.join(confPath, 'txt', year))
        txts = filter(lambda s: s.endswith('txt'), txts)
        for txt in txts:
            path = os.path.join(confPath,'txt',  year, txt)
            yield path, year, txt.split('.')[0]

def iterSIGCHI():
    for path, year, pid in iter_path(CHIPath):
        text = open(path).read()
        out = parsePaper(text, year, 'CHI', pid)
        if out == None:
            print 'ERR',
            print year, path, pid
            continue
        abstract, keywords, title, authors = out
        text = cleanText(text)
        yield Thing(**{'path': path, 'year': year, 'pid': pid, 'abstract': abstract, 'keywords': keywords, 'text': text, 'title': title, 'authors': authors})

def iterCSCW():
    for path, year, pid in iter_path(CSCWPath):
        text = open(path).read()
        out = parsePaper(text, year, 'CSCW', pid)
        if out == None:
            print 'ERR',
            print year, path, pid
            continue
        abstract, keywords, title, authors = out
        text = cleanText(text)
        yield Thing(**{'path': path, 'year': year, 'pid': pid, 'abstract': abstract, 'keywords': keywords, 'text': text, 'title': title, 'authors': authors})

if __name__ == '__main__':
    n = 0
    for paper in iterCSCW():
        n += 1
        pass
    for paper in iterSIGCHI():
        n += 1
        pass
        '''
        print paper.year
        print paper.pid
        print paper.keywords
        print paper.abstract
        print
        '''


    print skip, '/', n, ' ', 100.0*skip/n, '%'

