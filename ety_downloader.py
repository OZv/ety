#!/usr/bin/env python
# -*- coding: utf-8 -*-
## ety_downloader.py
## A helpful tool to fetch data from website & generate mdx source file
##
## This program is a free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, version 3 of the License.
##
## You can get a copy of GNU General Public License along this program
## But you can always get it from http://www.gnu.org/licenses/gpl.txt
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
import os
import re
import fileinput
from os import path
from collections import OrderedDict
from datetime import datetime
from urllib3 import PoolManager
from multiprocessing import Pool
from multiprocessing import Manager
from bs4 import SoupStrainer
from bs4 import BeautifulSoup


STEP = 500
MAX_PROCESS = 25
http = PoolManager()


def fullpath(file, suffix='', base_dir=''):
    if base_dir:
        return ''.join([os.getcwd(), path.sep, base_dir, file, suffix])
    else:
        return ''.join([os.getcwd(), path.sep, file, suffix])


def readdata(file, base_dir=''):
    fp = fullpath(file, base_dir=base_dir)
    if not path.exists(fp):
        print(file+" was not found under the same dir of this tool.")
    else:
        fr = open(fp, 'rU')
        try:
            return fr.read()
        finally:
            fr.close()
    return None


def getwordlist(file, base_dir=''):
    words = readdata(file, base_dir)
    if words:
        p = re.compile(r'\s*\n\s*')
        words = p.sub('\n', words).strip()
        return words.split('\n')
    print("Please put valid wordlist under the same dir with this tool.")
    return []


def dump(data, file, mod='w'):
    fname = fullpath(file)
    fw = open(fname, mod)
    try:
        fw.write(data)
    finally:
        fw.close()


def removefile(file):
    if path.exists(file):
        os.remove(file)


def getpage(link, BASE_URL = 'http://www.etymonline.com'):
    r = http.request('GET', ''.join([BASE_URL, link]))
    if r.status == 200:
        return r.data
    else:
        return None


def info(l, s='page'):
    return '%d %ss' % (l, s) if l>1 else '%d %s' % (l, s)


def dumpwords(sdir, words, sfx='', finished=True):
    if len(words):
        f = fullpath('rawhtml.txt', sfx, sdir)
        mod = 'a' if sfx else 'w'
        fw = open(f, mod)
        try:
            for en in words:
                df = en[1]
                if isinstance(df, OrderedDict):
                    text = []
                    for k, v in df.iteritems():
                        if k:
                            text.extend(['<div class="UOk"><span class="mB1">', k, '</span></div>'])
                        v = sorted(v, key=lambda d: d[0])
                        for val in v:
                            if val[0]:
                                text.extend(['<div class="m95">', val[0], '</div>'])
                                vdf = val[1].replace('<div class="FRe">', '<div class="Un2">')
                                text.append(vdf)
                            else:
                                text.append(val[1])
                    df = ''.join(text)
                fw.write('\n'.join([en[0], df, '</>\n']))
        finally:
            fw.close()
    if sfx and finished:
        removefile(fullpath('failed.txt', '', sdir))
        l = -len(sfx)
        cmd = '\1'
        nf = f[:l]
        if path.exists(nf):
            msg = "Found rawhtml.txt in the same dir, delete?(default=y/n)"
            cmd = 'y'#raw_input(msg)
        if cmd == 'n':
            return
        elif cmd != '\1':
            removefile(nf)
        os.rename(f, nf)


def cleansp(html):
    p = re.compile(r'\s+')
    html = p.sub(' ', html)
    p = re.compile(r'<!--[^<>]+?-->')
    html = p.sub('', html)
    p = re.compile(r'\s*<br/>\s*')
    html = p.sub('<br>', html)
    p = re.compile(r'\{(/?span[^\{\}<>]*)\}', re.I)
    html = p.sub(r'<\1>', html)
    p = re.compile(r'(\s*<br>\s*)*(<hr[^>]*>)(\s*<br>\s*)*', re.I)
    html = p.sub(r'\2', html)
    p = re.compile(r'(<br>\s*){2,}', re.I)
    html = p.sub('<div class="Xzi"></div>', html)
    p = re.compile(r'(\s*<br>\s*)*(<(?:/?div[^>]*|br)>)(\s*<br>\s*)*', re.I)
    html = p.sub(r'\2', html)
    p = re.compile(r'\s*(<(?:/?div[^>]*|br)>)\s*', re.I)
    html = p.sub(r'\1', html)
    return html


def makeappdx(page):
    srd = SoupStrainer('div', id='container')
    div = BeautifulSoup(page, parse_only=srd).div
    nav = div.find('div', id='navigation')
    nav.decompose()
    title = div.center.get_text(strip=True)
    div.center.decompose()
    font = div.find_all('font', size='2', color=None)
    for f in font:
        f.unwrap()
    for p in div.find_all('p'):
        p['class'] = 'ZFY'
        p.name = 'div'
    blank = div.find('div', class_='blank')
    if blank:
        blank.decompose()
    ft = div.find('div', id='footer')
    if ft:
        ft.decompose()
    div.attrs.clear()
    div['class'] = 'oH1'
    formatcontent(div)
    text = cleansp(div.encode('iso-8859-1'))
    div.decompose()
    return ''.join(['<div class="xsv">', title, '</div>', text])


def formatcontent(tag):
    for blk in tag.find_all('blockquote'):
        blk.name = 'div'
        last = blk.contents[-1]
        p = re.compile(r'(\[[^\[\]]+?\])\s*$', re.I)
        if last.string and p.search(last.string):
            str = p.sub(r'{span class="Ewg"}\1{/span}', last.string)
            last.string.replace_with(str)
            blk['class'] = 'TA7'
        else:
            blk['class'] = 'YcA'
    for hr in tag.find_all('hr'):
        hr.attrs.clear()
        hr['class'] = 'odM'
    for sp in tag.find_all('span', class_='foreign'):
        sp.attrs.clear()
        sp['class'] = 'myY'
    for a in tag.find_all('a', class_='crossreference'):
        href = a['href']
        a.attrs.clear()
        p = re.compile(r'index.php\?term=([^"<>]+?)\&', re.I)
        m = p.search(href)
        assert m
        a['href'] = ''.join(['entry://', m.group(1)])
    for a in tag.find_all('a', href=re.compile('http://(?!www.etymonline.com)')):
        a['target'] = '_blank'


def getwords(page, mdict, words, dref):
    pgc = SoupStrainer('dl')
    dl = BeautifulSoup(page, parse_only=pgc).dl
    formatcontent(dl)
    for a in dl.find_all('a', href=re.compile('http://www.etymonline.com/[^\.]+\.php$')):
        href = a['href']
        p = re.compile(r'/([^\.]+)\.php', re.I)
        m = p.search(href)
        assert m
        word = ''.join(['appendix-', m.group(1)])
        a['href'] = ''.join(['entry://', word])
        if not word in dref:
            print href
            dref[word] = None
            worddef = makeappdx(getpage(href, ''))
            words.append([word, worddef])
    dts = dl.find_all('dt')
    l = len(dts)
    dds = dl.find_all('dd')
    assert l==len(dds)
    for i in xrange(0, l):
        word = dts[i].a.string.strip()
        dd = dds[i]
        dd.name = 'div'
        dd['class'] = 'FRe'
        worddef = cleansp(dd.encode('utf8'))
        pos = word.find('(')
        prop = None
        if pos > 0:
            p = re.compile(r'\(((?:[a-zA-Z \,\.]+?)?)[\.,]?(\d*)\.?\)', re.I)
            m = p.search(word[pos:].replace('./', '., '))
            assert m
            prop = m.group(1).rstrip()
            if prop:
                prop += '.'
            worddef = [m.group(2), worddef]
            word = word[:pos].rstrip()
        if word in mdict:
            idx = mdict[word]
            df = words[idx][1]
            if isinstance(df, OrderedDict):
                if prop in df:
                    df[prop].append(worddef)
                else:
                    df[prop] = [worddef]
            else:
                if prop:
                    od = OrderedDict()
                    od[''] = [['', '<div class="tHO"></div>'.join([df, ''])]]
                    od[prop] = [worddef]
                    words[idx][1] = od
                else:
                    words[idx][1] = '<div class="tHO"></div>'.join([df, worddef])
        else:
            mdict[word] = len(words)
            if prop!=None:
                od = OrderedDict()
                od[prop] = [worddef]
                words.append([word, od])
            else:
                words.append([word, worddef])
    dl.decompose()


def makewords(urls, mdict, words, dref):
    failed = []
    count = 0
    for url in urls:
        count += 1
        if count % 20 == 0:
            print ".",
        page = None
        try:
            page = getpage(url)
            if page:
                getwords(page, mdict, words, dref)
        except Exception, e:
            import traceback
            print traceback.print_exc()
            print "%s failed" % url
        if not page:
            failed.append(url)
    return failed


def startdownload(urls, failedlist, mdict, words, dref):
    lenr = len(urls)
    leni = lenr + 1
    failed = []
    while lenr>0 and lenr<leni:
        leni = lenr
        failed = makewords(urls, mdict, words, dref)
        urls = failed
        lenr = len(failed)
    failedlist.extend(failed)
    if failedlist:
        msg = "%s failed, retry?(default=y/n)\n" % info(len(failedlist))
        return 'y'#raw_input(msg)
    return 'n'


def fetchdata_and_make_mdx(arg, urls=None, suffix=''):
    alpha = arg['alp']
    dref = arg['dref']
    url = ''.join(['/index.php?l=', alpha, '&allowed_in_frame=0'])
    page = getpage(url)
    mdict = {}
    words = []
    getwords(page, mdict, words, dref)
    if not urls:
        pgi = SoupStrainer('div', class_='paging')
        ul = BeautifulSoup(page, parse_only=pgi).ul
        if ul:
            count = len(ul.find_all('li'))
            urls = [''.join(['/index.php?l=', alpha, '&p=', str(i), '&allowed_in_frame=0']) for i in xrange(1, count)]
        else:
            urls = []
    l = len(urls)
    failedlist = []
    cmd = 'y' if urls else 'n'
    round = 0
    while round<20 and (not cmd or cmd.lower()=='y'):
        print "Block %s: %s downloading...using 1 thread" % (alpha, info(len(urls)))
        cmd = startdownload(urls, failedlist, mdict, words, dref)
        urls = failedlist
        failedlist = []
        round += 1
    f = len(urls)
    print "%s downloaded" % info(l-f),
    mdict.clear()
    if urls:
        dump('\n'.join(urls), ''.join([arg['dir'], 'failed.txt']))
        print ", %s failed, please look at failed.txt." % info(f)
        dumpwords(arg['dir'], words, '.part', False)
    else:
        print ", 0 word failed"
        dumpwords(arg['dir'], words, suffix)


def start(arg):
    import socket
    socket.setdefaulttimeout(120)
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    sdir = arg['dir']
    fp1 = fullpath('rawhtml.txt.part', base_dir=sdir)
    fp2 = fullpath('failed.txt', base_dir=sdir)
    fp3 = fullpath('rawhtml.txt', base_dir=sdir)
    if path.exists(fp1) and path.exists(fp2):
        print ("Continue last failed")
        urls = getwordlist('failed.txt', sdir)
        fetchdata_and_make_mdx(arg, urls, '.part')
    elif not path.exists(fp3):
        print ("New session started")
        fetchdata_and_make_mdx(arg)


def multiprocess_fetcher():
    pl = [chr(i) for i in xrange(ord('a'), ord('z')+1)]
    times = len(pl)
    dir = fullpath('ety')
    if not path.exists(dir):
        os.mkdir(dir)
    for i in xrange(1, times+1):
        subdir = ''.join(['ety', path.sep, '%d'%i])
        subpath = fullpath(subdir)
        if not path.exists(subpath):
            os.mkdir(subpath)
    pool = Pool(MAX_PROCESS)
    leni = times+1
    dref = Manager().dict()
    while 1:
        args = []
        for i in xrange(1, times+1):
            sdir = ''.join(['ety', path.sep, '%d'%i, path.sep])
            file = fullpath(sdir, 'rawhtml.txt')
            if not(path.exists(file) and os.stat(file).st_size):
                param = {}
                param['alp'] = pl[i-1]
                param['dir'] = sdir
                param['dref'] = dref
                args.append(param)
        lenr = len(args)
        if len(args) > 0:
            if lenr >= leni:
                print "The following parts cann't be downloaded:"
                for arg in args:
                    print arg['alp']
                times = -1
                break
            else:
                pool.map(start, args)
        else:
            break
        leni = lenr
    return times


def formatEntry(head, line):
    p = re.compile(r'(_{2,})')
    line = p.sub(r'<span class="x0h">\1</span>', line)
    p = re.compile(r'(<a href="entry://[^<>"]+">[^<>]+</a>)\s*\(([a-zA-Z \,\.]+?\.)\s*\)', re.I)
    line = p.sub(r'\1, <span class="h63">\2</span>', line)
    p = re.compile(r'(<a href="entry://[^<>"]+">[^<>]+</a>)\s*\(([a-zA-Z \,\.]+?\.)(\d+)\s*\)', re.I)
    line = p.sub(r'\1, <span class="h63">\2</span>\3', line)
    p = re.compile(r'(<a href="entry://[^<>"]+">[^<>]+</a>)\s*(\(\d+\))', re.I)
    line = p.sub(r'\1\2', line)
    p = re.compile(r'(<a href="entry://[^<>"]+">[^<>]+</a>)\s*\(([a-zA-Z ]+\.[a-zA-Z \,\.]+?\.?)\s*\)', re.I)
    line = p.sub(r'\1 (<span class="myY">\2</span>)', line)
    p = re.compile(r'\s+')
    line = p.sub(' ', line)
    if head.startswith('appendix-'):
        title = ''
    else:
        title = ''.join(['<div class="SCA">', head, '</div>'])
    return ''.join(['<link rel="stylesheet"href="ety.css"type="text/css"><div class="RmY">', title, line, '</div>'])


def formatabbr(page):
    srd = SoupStrainer('div', id='container')
    div = BeautifulSoup(page, parse_only=srd).div
    nav = div.find('div', id='navigation')
    nav.decompose()
    tbl = div.find('table')
    tbl.name = 'div'
    tbl.attrs.clear()
    tbl['class'] = 'oH1'
    tdr = div.find_all(name=re.compile(r't[dr]', re.I))
    for t in tdr:
        t.unwrap()
    for p in div.find_all('p'):
        p['class'] = 'ZFY'
        p.name = 'div'
    blank = div.find('div', class_='blank')
    if blank:
        blank.decompose()
    ft = div.find('div', id='footer')
    if ft:
        ft.decompose()
    formatcontent(div)
    div.attrs.clear()
    div['class'] = 'RmY'
    text = cleansp(div.encode('iso-8859-1'))
    div.decompose()
    return ''.join(['<link rel="stylesheet"href="ety.css"type="text/css">', text])


def combinefiles(times):
    dir = ''.join(['ety', path.sep])
    fw = open(fullpath(''.join([dir, 'ety.txt'])), 'w')
    fww = open(fullpath(''.join([dir, 'words.txt'])), 'w')
    try:
        for i in xrange(1, times+1):
            sdir = ''.join([dir, '%d'%i, path.sep])
            file = fullpath('rawhtml.txt', base_dir=sdir)
            lines = []
            for line in fileinput.input(file):
                line = line.strip()
                if line == '</>':
                    fw.write('\n'.join([lines[0],
                        formatEntry(lines[0], lines[1]), line, '']))
                    fww.write('\n'.join([lines[0], '']))
                    del lines[:]
                elif line:
                    lines.append(line)
        page = getpage('/abbr.php?allowed_in_frame=0')
        fw.write('\n'.join(['Introduction and abbreviations', formatabbr(page), '</>', '']))
        fww.write('Introduction and abbreviations')
    finally:
        fw.close()
        fww.close()


if __name__=="__main__":
    import sys
    reload(sys)
    sys.setdefaultencoding('utf-8')
    print "Start at %s" % datetime.now()
    times = multiprocess_fetcher()
    if times >= 0:
        combinefiles(times)
    print "Done!"
    print "Finished at %s" % datetime.now()
