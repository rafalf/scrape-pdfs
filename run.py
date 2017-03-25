#!/usr/bin/env python


# win: pip install lxml==3.6.0  (other pip install lxml)
# pip install requests
# pip install beautifulsoup4

import requests
from bs4 import BeautifulSoup
import os
import sys
import getopt
import logging
import time

scrape_url = 'https://www.occ.treas.gov/publications/publications-by-type/other-publications-reports/mortgage-metrics/index-mortgage-metrics.html'
logger = logging.getLogger(os.path.basename(__file__))
root_url = 'https://www.occ.treas.gov'


def scrape(fld, window):

    logger.info('Scraping: {}'.format(scrape_url))
    time_now = time.mktime(time.gmtime())
    sec_window = int(window) * 60 * 60 * 24
    from_time_window = time_now - sec_window

    logger.debug('Time now in seconds: {}'.format(time_now))

    logger.info('Time window (days): {}'.format(window))
    logger.debug('Time window in seconds: {}'.format(sec_window))
    logger.debug('Cut off (from date) point in seconds: {}'.format(from_time_window))

    r = requests.get(scrape_url)

    # create download folder
    if not fld:
        downloads_folder = os.path.join(os.path.dirname(__file__), 'download')
    else:
        downloads_folder = os.path.join(os.path.dirname(__file__), fld)
    if not os.path.isdir(downloads_folder):
        os.mkdir(downloads_folder)

    soup = BeautifulSoup(r.text, 'lxml')
    main = soup.find(id="maincontent")

    collected_pdfs = []

    # latest issue
    tbody = main.find('table')
    for td in tbody.find_all('td')[1:]:
        release_date = td.contents[0]
        links = td.find_all('a')

        str_time = time.strptime(release_date[:10], '%m/%d/%Y')
        doc_release_time = time.mktime(str_time)

        for link in links:
            href = link.get('href')
            logger.debug('Scrape href: {}'.format(href))
            if href.count('.pdf') and not href.count('pub-speech'):
                pdfs_href = root_url + href
                dwn_as = os.path.join(downloads_folder, pdfs_href[pdfs_href.rfind('/') + 1:])
                logger.debug('Release time: {}'.format(doc_release_time))
                break

        if doc_release_time > from_time_window:
            logger.info('Document within window range: {}'.format(href))
            collected_pdfs.append([doc_release_time, pdfs_href, dwn_as])
        else:
            logger.info('Document outside the window range: {}'.format(href))

    #  past issues
    for paragraph in main.find_all('p'):
        release_date = paragraph.contents[0]
        links = paragraph.find_all('a')

        str_time = time.strptime(release_date[:10], '%m/%d/%Y')
        doc_release_time = time.mktime(str_time)

        for link in links:
            href = link.get('href')
            logger.debug('Scrape href: {}'.format(href))
            if href.count('.pdf') and not href.count('pub-speech'):
                pdfs_href = root_url + href
                dwn_as = os.path.join(downloads_folder, pdfs_href[pdfs_href.rfind('/') + 1:])
                logger.debug('Release time: {}'.format(doc_release_time))
                break

        if doc_release_time > from_time_window:
            logger.info('Document within window range: {}'.format(href))
            collected_pdfs.append([doc_release_time, pdfs_href, dwn_as])
        else:
            logger.info('Document outside the window range: {}'.format(href))

    for collected_pdf in collected_pdfs:
        try:
            request = requests.get(collected_pdf[1], timeout=30, stream=True)
            with open(collected_pdf[2], 'wb') as fh:
                for chunk in request.iter_content(1024 * 1024):
                    fh.write(chunk)
            logger.info('Downloaded href: {} as {}'.format(collected_pdf[1], collected_pdf[2]))
        except:
            logger.error('Something went wrong', exc_info=True)


if __name__ == '__main__':
    dwn_folder = None
    verbose = None
    window = 7300  # 20 years

    log_file = os.path.join(os.path.dirname(__file__), 'logs',
                                time.strftime('%d%m%y', time.localtime()) + "_scraper.log")
    file_hndlr = logging.FileHandler(log_file)
    logger.addHandler(file_hndlr)
    console = logging.StreamHandler(stream=sys.stdout)
    logger.addHandler(console)
    ch = logging.Formatter('[%(levelname)s] %(message)s')
    console.setFormatter(ch)
    file_hndlr.setFormatter(ch)

    argv = sys.argv[1:]
    opts, args = getopt.getopt(argv, "o:vw:", ["output=", "verbose", "window="])
    for opt, arg in opts:
        if opt in ("-o", "--output"):
            dwn_folder = arg
        elif opt in ("-v", "--verbose"):
            verbose = True
        elif opt in ("-w", "--window"):
            window = arg

    if verbose:
        logger.setLevel(logging.getLevelName('DEBUG'))
    else:
        logger.setLevel(logging.getLevelName('INFO'))
    logger.debug('CLI args: {}'.format(opts))
    scrape(dwn_folder, window)