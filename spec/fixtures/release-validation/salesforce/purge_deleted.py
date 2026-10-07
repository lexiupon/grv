#!/usr/bin/env python3
"""Opt-in permanent deletion of ONLY already-deleted allowlisted fixture IDs.

Uses existing SOAP emptyRecycleBin permissions; never empties an org-wide bin,
changes permissions, or deletes active/source records. Service failure fails.
"""
import argparse
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

import provision

from private_scope import salesforce_scope
NS = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/', 'sf': 'urn:partner.soap.sforce.com'}


def deleted_ids(sf, obj):
    if obj not in provision.OBJECTS:
        raise SystemExit('Only fixture objects allowed')
    query = f'SELECT Id FROM {obj} WHERE IsDeleted = true'
    path = f'/services/data/v{sf.api}/queryAll/?q=' + urllib.parse.quote(query)
    ids = []
    while path:
        page = sf.get(path)
        ids.extend(row['Id'] for row in page['records'])
        path = page.get('nextRecordsUrl')
    if len(ids) != len(set(ids)) or any(not re.fullmatch(r'[A-Za-z0-9]{18}', x) for x in ids):
        raise SystemExit('Invalid/duplicate fixture deleted IDs')
    return ids


def purge_batch(sf, ids):
    if not ids or len(ids) > 200:
        raise SystemExit('SOAP deletion batch must contain 1..200 pinned IDs')
    body = ('<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:sf="urn:partner.soap.sforce.com"><env:Header><sf:SessionHeader>'
            '<sf:sessionId>' + escape(sf.token) + '</sf:sessionId></sf:SessionHeader>'
            '</env:Header><env:Body><sf:emptyRecycleBin>' +
            ''.join('<sf:ids>' + escape(x) + '</sf:ids>' for x in ids) +
            '</sf:emptyRecycleBin></env:Body></env:Envelope>')
    request = urllib.request.Request(
        sf.base + f'/services/Soap/u/{sf.api}', data=body.encode(), method='POST',
        headers={'Content-Type': 'text/xml; charset=UTF-8', 'SOAPAction': 'emptyRecycleBin'})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read(2 * 1024 * 1024 + 1)
    except urllib.error.HTTPError as error:
        raise SystemExit(f'Fixture recycle cleanup HTTP {error.code}; no permission changes made') from None
    except urllib.error.URLError:
        raise SystemExit('Fixture recycle cleanup transport failed') from None
    if len(payload) > 2 * 1024 * 1024:
        raise SystemExit('SOAP response exceeds bound')
    root = ET.fromstring(payload)
    results = root.findall('.//sf:emptyRecycleBinResponse/sf:result', NS)
    if len(results) != len(ids):
        raise SystemExit('SOAP recycle result cardinality differs')
    for expected, result in zip(ids, results):
        if result.findtext('sf:id', namespaces=NS) != expected or result.findtext('sf:success', namespaces=NS) != 'true':
            codes = [node.text for node in result.findall('sf:errors/sf:statusCode', NS)]
            raise SystemExit('Fixture recycle cleanup refused: ' + ','.join(codes))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--org', required=True)
    parser.add_argument('--expected-org-id', required=True)
    parser.add_argument('--apply', action='store_true', help='irreversibly purge only deleted fixture IDs')
    args = parser.parse_args()
    ORG, ORG_ID = salesforce_scope()
    if args.org != ORG or args.expected_org_id != ORG_ID:
        raise SystemExit('Only pinned disposable org allowed')
    if args.apply and os.environ.get('GRV_SALESFORCE_TEST_MUTATION') != 'allow':
        raise SystemExit('Apply requires explicit fixture mutation authorization')
    display = provision.sf_json(['org', 'display', '-o', args.org])
    if display['id'] != ORG_ID or display['username'] != ORG:
        raise SystemExit('Wrong org identity; no cleanup performed')
    sf = provision.Salesforce(args.org)
    for obj in provision.OBJECTS:
        ids = deleted_ids(sf, obj)
        print(f'{obj}: {len(ids)} deleted fixture records; apply={args.apply}', flush=True)
        if not args.apply:
            continue
        for offset in range(0, len(ids), 200):
            purge_batch(sf, ids[offset:offset + 200])
        if deleted_ids(sf, obj):
            raise SystemExit('Fixture deleted-record cleanup not proven complete')
        print(f'{obj}: deleted fixture records proved absent', flush=True)


if __name__ == '__main__':
    main()
