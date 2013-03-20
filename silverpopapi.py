#!/usr/bin/env python 

import sys
import os
import copy
import urllib2

from lxml import etree

import vlib.conf as conf
from vlib.odict import odict
from vlib.utils import echoized, validate_num_args

DEBUG = 0
VERBOSE = 0

COMMANDS = ('exportlist', 'getlists', 'getlistmetadata', 'importlist')

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

        if cmd == 'getlist':
            return self.getLists(args.relational)
            
        elif cmd == 'getlistmetadata':
            return self.getListMetaData(args.list_id)
            
        elif cmd == 'exportlist':
            return self.exportList(args.list_id)
            
        elif cmd == 'InsertUpdateRelationalTable':
            return self.InsertUpdateRelationalTable(args.list_id,
                                                      args.csvfile)
        else:
            return 'Unrecognized cmd:', cmd
            
    def login(self):
        '''Wrapper to requests()'''
        username = self.conf['silverpop']['username']
        password = self.conf['silverpop']['password']        
        params   = {'USERNAME': username, 'PASSWORD': password}
        xresults = self.request('Login', params)

        xpath = '/Envelope/Body/RESULT/SESSIONID'
        self.jsessionid = xresults.xpath(xpath)[0].text

    def exportList(self, list_id):
        '''Wrapper to requests()'''
        params = {'LIST_ID'      : list_id,
                  'EXPORT_TYPE'  : 'ALL',
                  'EXPORT_FORMAT': 'CSV',
                  'FILE_ENCODING': 'utf-8'}
        xresults = self.request('ExportList', params)
        xpath = '/Envelope/Body/RESULT/FILE_PATH'
        return xresults.xpath(xpath)[0].text

    def getLists(self, relational_tables=False):
        '''Wrapper to requests()'''
        if relational_tables:
            params = {'VISIBILITY': '1', 'LIST_TYPE': '15'} 
        else:
            params = {'VISIBILITY': '1', 'LIST_TYPE': '2'}
        xresults = self.request('GetLists', params)
        xpath = '/Envelope/Body/RESULT/LIST'
        Lists = []
        for List in xresults.xpath(xpath):
            id = List.xpath('./ID')[0].text
            name = List.xpath('./NAME')[0].text
            Lists.append([id, name])
        return Lists

    def getListMetaData(self, list_id):
        '''Wrapper to requests()'''
        params = {'LIST_ID': list_id}
        xresults = self.request('GetListMetaData', params)
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

    def request(self, request_name, params):
        '''Given the request_name and a dictionary of key:value pairs
           Make Silverpop API Request call.
           Return Resultant XML
        '''
        url     = self.conf['silverpop']['url']
        headers = {'Content-Type': 'text/xml;charset=UTF-8'}        

        # add jsessionid:
        if request_name not in ('Login'):
            if not self.jsessionid:
                self.login()
            url += ';jsessionid=%s' % self.jsessionid

        # build request xml:
        xrequest = self.buildRequestEtree(request_name, params)
        if self.verbose:
            print '%s: xrequest:\n%s' % (request_name, xml_pretty(xrequest))

        # Call API:
        req = urllib2.Request(url=url, headers=headers, data=xml_str(xrequest))
        fp = urllib2.urlopen(req)
        xresults = etree.fromstring(fp.read())
        if self.verbose:
            print '%s: xresults:\n%s' % (request_name, xml_pretty(xresults))
            
        # check results:
        xpath = '/Envelope/Body/RESULT/SUCCESS'
        success = xresults.xpath(xpath)[0].text in ('true', 'TRUE')
        if not success:
            xpath = '/Envelope/Body/Fault/FaultString'
            faultstring = xresults.xpath(xpath)[0].text
            raise SilverpopApiError(faultstring)

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
    return etree.tostring(xml)

def xml_pretty(xml):
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
    o += "   %s      ImportUpdateRelTable <list_id> <csv_file>\n" % ws
    o += "\n"
    return o

def parseArgs():
    import argparse
    p = argparse.ArgumentParser(description="Silverpop API", usage=syntax())
    p.add_argument('-v', dest='verbose', action='store_true',
                   help='verbose')
    sp = p.add_subparsers(dest='cmd')

    q = sp.add_parser('GetList', help='Get list of tables')
    q.add_argument('--relational', action='store_true',
                   help='list relational tables')

    q = sp.add_parser('GetListMetaData', help='Get table column information')
    q.add_argument('list_id')

    q = sp.add_parser('ExportList', help='Export data to download site')
    q.add_argument('list_id')

    q = sp.add_parser('InsertUpdateRelTable', help='Import/Update '
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
        

