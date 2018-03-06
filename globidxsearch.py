#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
   globidxsearch.py - Process search results from https://www.stolp.de/globalindex_en.html
   Copyright (C) 2018 Seth Grover (Seth.D.Grover@gmail.com)

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.

   Written by Seth Grover <Seth.D.Grover@gmail.com>
"""

# global import
from __future__ import print_function
import sys
import pprint
import argparse
import time
import re
import csv
import urllib.parse
from bs4 import BeautifulSoup

try:
    import requests
except ImportError:
    sys.stderr.write('You need to install the requests module first\n')
    sys.stderr.write('(run this in your terminal: "python3 -m pip install requests" or "python3 -m pip install --user requests")\n')
    exit(2)

MAX_ROWS_PER_PAGE = 1000
DEFAULT_SLEEP_SEC = 1
COL_SURNAME = 'Name'
COL_FORNAME = 'Vornamen'
COL_TITLE = 'Was'
COL_PLACE = 'Ort'
COL_YEAR = 'Jarh'
COL_MORE_INFO = '\xa0'
COL_LINK = 'URL'

class Searcher:
  def __init__(self, surname, forename, beginYear, endYear, place, timeout=60, rowsToReq=MAX_ROWS_PER_PAGE, sleepBetween=DEFAULT_SLEEP_SEC, verbose=False, logfile=sys.stderr):
    self.surname = surname
    self.forename = forename
    self.beginYear = beginYear
    self.endYear = endYear
    self.place = place
    self.verbose = verbose
    self.logfile = logfile
    self.timeout = timeout
    self.rowsToReq = rowsToReq
    if (self.rowsToReq > MAX_ROWS_PER_PAGE):
      self.rowsToReq = MAX_ROWS_PER_PAGE
    self.sleepSec = sleepBetween
    self.processedRows = 0
    self.searchedRows = 0
    self.searchedPages = 0
    self.results = []

  def writeLog(self, text):
    if self.verbose:
      self.logfile.write('[%s]: %s\n' % (time.strftime('%Y-%m-%d %H:%M:%S'), text))

  def buildSearchUrl(self, page=0):
    terms = { 'per_page' : self.rowsToReq, \
              'Name' : self.surname, \
              'Vornamen' : self.forename, \
              'Ort' : self.place, \
              'JahrVon' : self.beginYear, \
              'JahrBis' : self.endYear, \
              'order_by' : 'Jahr', \
              'sort' : 'asc' }
    if (page > 0):
      terms['page_l98'] = page
    return urllib.parse.urlencode(terms)

  def expandResults(self):
    oldResults = self.results
    self.results = []
    yearReg = re.compile("^(\d{4})u")
    for resultRow in oldResults:
      rowContents = resultRow
      if resultRow[COL_LINK]:
        time.sleep(self.sleepSec)
        self.writeLog('Downloading: ' + resultRow[COL_LINK])
        request = requests.get(resultRow[COL_LINK], allow_redirects=False)
        soup = BeautifulSoup(request.text, 'lxml')
        table = soup.find("table", attrs={"class":"single_record"})
        for row in table.find("tbody").find_all("tr"):
          label = row.find("th", attrs={"class":"label"}).get_text()
          contents = row.find("td", attrs={"class":"value"}).get_text()
          if (label == 'Jahr'):
            yearMatch = yearReg.match(contents)
            if yearMatch:
              contents = yearMatch.group(1)
          rowContents[label] = contents
      self.results.append(rowContents)

  def searchPage(self, page):
    if (page > 0):
      time.sleep(self.sleepSec)
    searchString = 'https://www.stolp.de/globalindex_en.html?' + self.buildSearchUrl(page)
    self.writeLog('Downloading: ' + searchString)
    request = requests.get(searchString, allow_redirects=False)
    if (page == 0):
      resultCountReg = re.compile("Ergebnisse:\s*(\d+)")
      try:
        self.searchedRows = int(resultCountReg.search(request.text, re.MULTILINE).group(1))
      except (AttributeError, TypeError) as e:
        self.searchedRows = 0
      if (self.searchedRows > 0):
        pageCountReg = re.compile("Seite\s*(\d+)\s*von\s*(\d+)")
        try:
          self.searchedPages = int(pageCountReg.search(request.text, re.MULTILINE).group(2))
        except (AttributeError, TypeError) as e:
          self.searchedPages = 1
      else:
        self.searchedPages = 0

      self.writeLog(str(self.searchedRows) + ' resultant rows in ' + str(self.searchedPages) + ' pages')
      if (self.searchedRows <= 0):
        return

    soup = BeautifulSoup(request.text, 'lxml')
    table = soup.find("table", attrs={"class":"all_records"})
    headers = [th.get_text() for th in table.find("thead").find("tr").find_all("th")]
    for row in table.find("tbody").find_all("tr"):
      rowContents = {}
      for i, cell in enumerate(row.find_all("td")):
        if (headers[i] == COL_MORE_INFO):
          link = cell.find('a')['href']
          if not bool(re.match('^http', link, re.I)):
            link = 'https://www.stolp.de/' + link
          rowContents[COL_LINK] = link
        else:
          rowContents[headers[i]] = cell.get_text()
      self.results.append(rowContents)

  def search(self):
    self.searchPage(0)
    if (self.searchedRows > 0) and (self.searchedPages > 0):
      for page in range(1, self.searchedPages+1):
        self.searchPage(page)
    self.expandResults()
    # pprint.pprint(self.results)
    return(self.results)

####################################################################3

parser = argparse.ArgumentParser(description='Process search results from Globalindex', add_help=False, usage='globalidxsearch.py [options]')
parser.add_argument('-s', metavar='<STR>', type=str, default='', help='surname')
parser.add_argument('-f', metavar='<STR>', type=str, default='', help='forename')
parser.add_argument('-p', metavar='<STR>', type=str, default='', help='place')
parser.add_argument('-b', metavar='<INT>', type=int, default=1500, help='beginning year')
parser.add_argument('-e', metavar='<INT>', type=int, default=2000, help='ending year')
parser.add_argument('-r', metavar='<INT>', type=int, default=MAX_ROWS_PER_PAGE, help='rows per request')
parser.add_argument('-w', metavar='<INT>', type=int, default=DEFAULT_SLEEP_SEC, help='wait between requests (in seconds)')
try:
    parser.add_argument('-o', metavar='<FILE>', type=argparse.FileType('w', encoding='UTF-8'), default=sys.stdout, help='output results file')
except TypeError:
    sys.stderr.write('Python >= 3.4 is required to run this script\n')
    sys.stderr.write('(see https://docs.python.org/3/whatsnew/3.4.html#argparse)\n')
    exit(2)
parser.add_argument("-v", action="store_true", default=False, help="Increase output verbosity [False]")
parser.add_argument('-t', metavar='<INT>', type=int, default=60, help='Timeout in seconds [60]')

# extract arguments from the command line
try:
    parser.error = parser.exit
    args = parser.parse_args()
except SystemExit:
    parser.print_help()
    exit(2)

searcher = Searcher(args.s, args.f, args.b, args.e, args.p, args.t, args.r, args.w, args.v)
results = searcher.search()
if results:
  dict_writer = csv.DictWriter(args.o, sorted(results[0], key=lambda key: results[0][key]))
  dict_writer.writeheader()
  dict_writer.writerows(results)
