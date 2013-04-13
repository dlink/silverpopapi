#!/usr/bin/env python 

import sys
import os
import copy
import urllib2

from lxml import etree

import vlib.conf as conf
from vlib.odict import odict
from vlib.utils import echoized, validate_num_args, str2datetime

DEBUG = 0
VERBOSE = 0

ENVELOPE = '<Envelope><Body></Body></Envelope>'

SP_TYPES = {0: 'text', 1: 'boolean', 2: 'numeric', 3: 'date', 4: 'time',
            5: 'Country', 6: 'Select One', 8: 'Segmenting', 9: 'System',
            12: 'Unknown', 13: 'SMS Opt In', 14: 'SMS Opted Out Date', 
            15: 'SMS Phone Number', 16: 'Phone Number',17: 'Timestamp',
            20: 'Multi-Select' }

class SilverpopApiError(Exception): pass

class SilverpopApi(object):
    
    def __init__(self):
        self.conf = conf.Factory.create().data
        self.jsessionid = None
        self.verbose = VERBOSE

    def process(self, args):
        args = odict(args)
        cmd = args.cmd.lower()

        if cmd == 'getlists':
            return self.getLists(args.relational)
            
        elif cmd == 'getlistmetadata':
            return self.getListMetaData(args.list_id)
            
        elif cmd == 'exportlist':
            return self.exportList(args.list_id)
            
        elif cmd == 'insertupdaterelationaltable':
            return self.InsertUpdateRelationalTable(args.list_id,
                                                      args.csvfile)
        else:
            return 'Unrecognized cmd:', cmd
            
    def login(self):
        '''Wrapper to requests()'''
        username = self.conf.silverpop.username
        password = self.conf.silverpop.password
        params   = {'USERNAME': username, 'PASSWORD': password}
        xrequest = xml_str(self.buildRequestEtree('Login', params))
        xresults = self.request(xrequest, logging_in=True)

        xpath = '/Envelope/Body/RESULT/SESSIONID'
        self.jsessionid = xresults.xpath(xpath)[0].text

    def exportList(self, list_id):
        """Given a list_id, call the ExportList API.
           calls request()

           What's returned is the name of the new downloadable file
           from ftp site.
           
           The file can them be fetched with (TO DO: automate this)

             curl -u username:password \
                  ftp://transfer5.silverpop.com/download/<filename>
        """
        params = {'LIST_ID'      : list_id,
                  'EXPORT_TYPE'  : 'ALL',
                  'EXPORT_FORMAT': 'CSV',
                  'FILE_ENCODING': 'utf-8'}
        xrequest = xml_str(self.buildRequestEtree('ExportList', params))
        xresults = self.request(xrequest)
        xpath = '/Envelope/Body/RESULT/FILE_PATH'
        return xresults.xpath(xpath)[0].text

    def getLists(self, relational_tables=False):
        '''Wrapper to requests()'''
        if relational_tables:
            params = {'VISIBILITY': '1', 'LIST_TYPE': '15'} 
        else:
            params = {'VISIBILITY': '1', 'LIST_TYPE': '2'}
        xrequest = xml_str(self.buildRequestEtree('GetLists', params))
        xresults = self.request(xrequest)
        xpath = '/Envelope/Body/RESULT/LIST'
        Lists = []
        for List in xresults.xpath(xpath):
            id = List.xpath('./ID')[0].text
            name = List.xpath('./NAME')[0].text
            last_modified = List.xpath('./LAST_MODIFIED')[0].text
            last_modified = str2datetime(last_modified, "%m/%d/%y %I:%M %p")
            Lists.append([id, name, last_modified])
        return Lists

    def getListMetaData(self, list_id):
        '''Wrapper to requests()'''
        params = {'LIST_ID': list_id}
        xrequest = xml_str(self.buildRequestEtree('GetListMetaData', params))
        xresults = self.request(xrequest)
        xpath = '/Envelope/Body/RESULT/COLUMNS/COLUMN'
        num = 0
        Columns = []
        Columns.append(['No.', 'Name', 'Type', 'Default_Value', 
                        'Selection_Values'])
        for Column in xresults.xpath(xpath):
            num += 1
            name = Column.xpath('./NAME')[0].text

            e = Column.xpath('./DEFAULT_VALUE') 
            default_value = e[0].text if e else ''

            e = Column.xpath('./TYPE')
            type = SP_TYPES[int(e[0].text)] if e else ''
                
            e = Column.xpath('./SELECTION_VALUES')
            selection_values = [x.text for x in e[0]] if e else ''

            Columns.append([num, name, type, default_value, selection_values])
        return Columns

    def InsertUpdateRelationalTable(self, list_id, csv_file):
        '''Given a List_id and a csv_file
              Column header names must  match List Field names.
              Make Silverpop API Request Call
              Returns Resultant XML
        '''
        import csv
        
        # build xml request from csv_file:
        request = etree.Element('InsertUpdateRelationalTable')
        etree.SubElement(request, 'TABLE_ID').text = list_id
        #etree.SubElement(request, 'LIST_DATE_FORMAT').text = 'yyyy-mm-dd'
        xrows = etree.SubElement(request, 'ROWS')
        for n, row in enumerate(csv.reader(open(csv_file, 'r'))):
            if n == 0:
                header = row
                continue
            xrow = etree.SubElement(xrows, 'ROW')
            for i, c in enumerate(row):
                etree.SubElement(xrow, 'COLUMN', 
                                 attrib={'name': header[i]}).text = c
        envelope = etree.fromstring(ENVELOPE)
        envelope[0].append(request)
        xrequest = xml_str(envelope)
        xresults = self.request(xrequest)
        xpath = '/Envelope/Body/FAILURES/FAILURE'
        return '%s Row(s) Inserted or Updated' % n

    def request(self, xrequest, logging_in=False):
        '''Given a request as XML in a string
              Read url, username, and password from conf
              Log into Silverpop API and gets a session.
              Make Silverpop API Request Call
              (logging_in option only used during login)

              Returns Resultant XML
        '''
        url     = self.conf.silverpop.url
        headers = {'Content-Type': 'text/xml;charset=UTF-8'}        

        # add jsessionid:
        if not logging_in:
            if not self.jsessionid:
                self.login()
            url += ';jsessionid=%s' % self.jsessionid

        if self.verbose:
            print 'xrequest:\n%s' % xrequest

        # Call API:
        req = urllib2.Request(url=url, headers=headers, data=xrequest)
        fp = urllib2.urlopen(req)
        xresults = etree.fromstring(fp.read())
        if self.verbose:
            print 'xresults:\n%s' % xml_str(xresults)
            
        # check results:
        xpath = '/Envelope/Body/RESULT/SUCCESS'
        success = xresults.xpath(xpath)[0].text in ('true', 'TRUE')
        if not success:
            xpath = '/Envelope/Body/Fault/FaultString'
            faultstring = xresults.xpath(xpath)[0].text
            raise SilverpopApiError(faultstring)

        # check for FAILURES (InsertUpdateRelationalTable for one 
        # returns both SUCCESS and FAILURES - go figure
        xpath = '/Envelope/Body/RESULT/FAILURES/FAILURE'
        failures = xresults.xpath(xpath)
        if failures:
            msg = '; '.join(['%s. %s' % (i+1, x.attrib['description']) 
                             for i, x in enumerate(failures)])
            raise SilverpopApiError(msg)

        # Return results as etree:
        return xresults

    def buildRequestEtree(self, request_name, params):
        '''Given the request_name and a dictionary of key:value pairs
           Return a Silverpop API Request object in etree XML

           eq. Given: ('Login', {'USERNAME': 'jojo@mojocom.com',
                                'PASSWORD': 'swordfish'})
           Return

              <Envelope>
                <Body>
                  <Login>
                    <USERNAME>jojo@mojocom.com</USERNAME>
                    <PASSWORD>swordfish</PASSWORD>
                  </Login>
                </Body>
              </Envelope>
        '''
        request = etree.Element(request_name)
        for k, v in params.items():
            etree.SubElement(request, k).text = v
            
        envelope = etree.fromstring(ENVELOPE)
        envelope[0].append(request)
        return envelope

def disp_results(results):
    if not results:
        return

    if isinstance(results, (list, tuple)):
        if isinstance(results[0], (list, tuple)):
            for row in results:
                print ",".join(map(str, row)),
                print
        else:
            print "\n".join(map(str, results))
    elif isinstance(results, dict):
        keys = sorted(results.keys())
        for k in keys:
            print "%s: %s" % (k, results[k])
    else:
        print results


def xml_str(xml):
    return etree.tostring(xml, pretty_print=True)

def disp_results(results):
    '''Display results formated nicely for the console'''
    if isinstance(results, (list, tuple)):
        if isinstance(results[0], (list, tuple)):
            for row in results:
                print ",".join(map(str, row)),
                print
        else:
            print 'e'
            print "\n".join(map(str, results))
    elif isinstance(results, dict):
        keys = sorted(results.keys())
        for k in keys:
            print "%s: %s" % (k, results[k])
    else:
        print results

def syntax():
    prog_name = os.path.basename(sys.argv[0])
    ws = ' '*len(prog_name)
    o = ''
    o += "\n"
    o += "   %s [-v] ExportList           <list_id>\n"      % prog_name
    o += "   %s      GetLists             [ --relational ]\n"     % ws
    o += "   %s      GetListMetaData      <list_id>\n" % ws
    o += "   %s      ImportUpdateRelationalTable <list_id> <csv_file>\n" % ws
    o += "\n"
    return o

def parseArgs():
    import argparse
    p = argparse.ArgumentParser(description="Silverpop API", usage=syntax())
    p.add_argument('-v', dest='verbose', action='store_true',
                   help='verbose')
    sp = p.add_subparsers(dest='cmd')

    q = sp.add_parser('GetLists', help='Get list of tables')
    q.add_argument('--relational', action='store_true',
                   help='list relational tables')

    q = sp.add_parser('GetListMetaData', help='Get table column information')
    q.add_argument('list_id')

    q = sp.add_parser('ExportList', help='Export data to download site')
    q.add_argument('list_id')

    q = sp.add_parser('InsertUpdateRelationalTable', help='Import/Update '
                      'csvfile to relational table')
    q.add_argument('list_id')
    q.add_argument('csvfile')

    args = p.parse_args()
    return vars(args)

if __name__ == '__main__':
    args = parseArgs()
    VERBOSE = args['verbose']

    try:
        disp_results(SilverpopApi().process(args))
    except Exception, e:
        if VERBOSE:
            raise
        print "%s:%s" % (e.__class__.__name__, e)
        

