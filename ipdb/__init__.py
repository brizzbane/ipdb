import re
import netaddr
import iptools
import urllib
from StringIO import StringIO
import zipfile
from collections import defaultdict

from elasticsearch.helpers import bulk
from elasticsearch_dsl import DocType
from elasticsearch import Elasticsearch

asnmatch = re.compile(ur'(?P<startip>\d+),(?P<endip>\d+),.*(?P<asn>AS\d+)(?P<company>.*).')

dburl = 'http://download.maxmind.com/download/geoip/database/asnum/GeoIPASNum2.zip'

asns = defaultdict(lambda :{'ipranges': [], 'company': None, 'asn': None})

es = Elasticsearch()

class IpAsnRangeDoc(DocType):

    class Meta:
        index = 'ips.by_asn'

    def save(self, ** kwargs):
        return super(IpAsnRangeDoc, self).save(** kwargs)

class IpGeoDoc(DocType):

    class Meta:
        index = 'ips.by_geo'

    def save(self, ** kwargs):
        return super(IpGeoDoc, self).save(** kwargs)

def build_db():
    asndb = urllib.urlopen(dburl)
    file = StringIO()
    file.write(asndb.read())
    zip = zipfile.ZipFile(file)
    for itm in zip.namelist():
        uncompressed = zip.read(itm)
        lines = uncompressed.split('\n')
        for line in lines[:-1]:
            match = asnmatch.match(line).groupdict()
            print(match)
            start_ip =  iptools.ipv4.long2ip(int(match['startip']))
            end_ip = iptools.ipv4.long2ip(int(match['endip']))

            cidr = netaddr.iprange_to_cidrs(start_ip, end_ip)[0]
            asns[match['asn']]['ipranges'].append((cidr, start_ip, end_ip))


            asns[match['asn']]['company'] = match['company'].decode('utf-8', 'ignore').rstrip().lstrip()
            asns[match['asn']]['asn'] = match['asn']


    bulk_ranges = []
    print('Building Docs.')
    for itm in asns.itervalues():
        iprangedoc = IpAsnRangeDoc(_id=itm['asn'])
        iprangedoc.owner = itm['company']
        iprangedoc.ranges = []
        iprangedoc.total_ips = 0
        iprangedoc.total_ranges = len(itm['ipranges'])

        for cidr, start_ip, end_ip in itm['ipranges']:
            ipcount = len(cidr)
            iprangedoc.ranges.append({'cidr': str(cidr), 'ip_count': ipcount})
            iprangedoc.total_ips += ipcount

        print('%s, range total %s, ip total %s' % (iprangedoc.owner, iprangedoc.total_ranges, iprangedoc.total_ips))
        bulk_ranges.append(iprangedoc)

    print('Bulk indexing %s docs.' % len(asns))
    bulk(es, (d.to_dict(include_meta=True) for d in bulk_ranges))