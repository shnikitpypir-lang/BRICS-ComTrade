#!/usr/bin/env python3
import csv, json, os, time, hashlib, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests

ROOT = Path('/home/user')
RAW = ROOT/'data/raw/comtrade'
PROC = ROOT/'data/processed/block_b'
META = ROOT/'metadata'
RAW.mkdir(parents=True, exist_ok=True); PROC.mkdir(parents=True, exist_ok=True); META.mkdir(parents=True, exist_ok=True)

COUNTRIES = {
    'BRA': {'m49': 76, 'name': 'Brazil'},
    'RUS': {'m49': 643, 'name': 'Russian Federation'},
    'IND': {'m49': 356, 'name': 'India'},
    'CHN': {'m49': 156, 'name': 'China'},
    'ZAF': {'m49': 710, 'name': 'South Africa'},
    'SAU': {'m49': 682, 'name': 'Saudi Arabia'},
    'EGY': {'m49': 818, 'name': 'Egypt'},
    'ETH': {'m49': 231, 'name': 'Ethiopia'},
    'IRN': {'m49': 364, 'name': 'Iran (Islamic Republic of)'},
    'ARE': {'m49': 784, 'name': 'United Arab Emirates'},
    'IDN': {'m49': 360, 'name': 'Indonesia'},
}
YEARS = range(2000, 2026)
FLOWS = {'X': 'Export', 'M': 'Import'}
CMD_CODES = {'TOTAL': 'All commodities', 'AG2': 'HS chapters / 2-digit aggregates'}
BASE = 'https://comtradeapi.un.org/public/v1/preview/C/A/HS'

session = requests.Session()

def request_one(rep, par, flow, year, cmd):
    d = RAW / str(year) / rep / par / flow
    d.mkdir(parents=True, exist_ok=True)
    path = d / f'{cmd}.json'
    if path.exists() and path.stat().st_size > 20:
        return ('cached', str(path))
    params = dict(reporterCode=COUNTRIES[rep]['m49'], partnerCode=COUNTRIES[par]['m49'],
                  flowCode=flow, cmdCode=cmd, period=year, customsCode='C00',
                  motCode=0, partner2Code=0, includeDesc='true')
    url = requests.Request('GET', BASE, params=params).prepare().url
    last = None
    transient_attempts = 0
    while transient_attempts < 8:
        try:
            r = session.get(BASE, params=params, timeout=90)
            last = f'{r.status_code}: {r.text[:300]}'
            if r.status_code == 200:
                obj = r.json()
                obj['_request_url'] = r.url
                obj['_downloaded_at_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                path.write_text(json.dumps(obj, ensure_ascii=False), encoding='utf-8')
                return ('ok', str(path))
            if r.status_code == 403 and 'quota' in r.text.lower():
                m = re.search(r'(\d{2}):(\d{2}):(\d{2})', r.text)
                wait = 3605
                if m:
                    vals = [int(x) for x in m.groups()]
                    if len(vals) == 3: wait = vals[0]*3600 + vals[1]*60 + vals[2] + 10
                    elif len(vals) == 2: wait = vals[0]*60 + vals[1] + 10
                print(f'Quota exhausted; sleeping {wait} seconds', flush=True)
                time.sleep(wait)
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                transient_attempts += 1
                time.sleep(min(60, 2 ** transient_attempts) + 0.2)
                continue
            break
        except Exception as e:
            last = repr(e); transient_attempts += 1; time.sleep(min(60, 2 ** transient_attempts) + 0.2)
    err = d / f'{cmd}.error.txt'; err.write_text(str(last), encoding='utf-8')
    return ('error', f'{rep}-{par}-{flow}-{year}-{cmd}: {last}')

def flatten(obj, rep, par, flow, year, cmd):
    rows=[]
    for x in obj.get('data', []):
        row = {
            'reporter_iso': x.get('reporterISO') or rep,
            'reporter_code': x.get('reporterCode'),
            'reporter_desc': x.get('reporterDesc'),
            'partner_iso': x.get('partnerISO') or par,
            'partner_code': x.get('partnerCode'),
            'partner_desc': x.get('partnerDesc'),
            'year': x.get('refYear', year),
            'period': x.get('period'),
            'flow_code': x.get('flowCode', flow),
            'flow_desc': x.get('flowDesc'),
            'classification_code': x.get('classificationCode'),
            'classification_search_code': x.get('classificationSearchCode'),
            'is_original_classification': x.get('isOriginalClassification'),
            'cmd_code': x.get('cmdCode', cmd),
            'cmd_desc': x.get('cmdDesc'),
            'customs_code': x.get('customsCode'),
            'mot_code': x.get('motCode'),
            'primary_value': x.get('primaryValue'),
            'customs_value': x.get('customsValue'),
            'cifvalue': x.get('CIFValue'),
            'fobvalue': x.get('FOBValue'),
            'net_wgt': x.get('netWgt'),
            'gross_wgt': x.get('grossWgt'),
            'quantity': x.get('qty'),
            'quantity_unit': x.get('qtyUnit'),
            'quantity_unit_desc': x.get('qtyUnitDesc'),
            'is_aggregate': x.get('isAggregate'),
            'is_reported': x.get('isReported'),
            'source_file': None,
        }
        rows.append(row)
    return rows

def main():
    jobs=[]
    codes=list(COUNTRIES)
    for rep in codes:
        for par in codes:
            if rep == par: continue
            for year in YEARS:
                for flow in FLOWS:
                    for cmd in CMD_CODES:
                        jobs.append((rep,par,flow,year,cmd))
    print(f'Jobs: {len(jobs)}')
    counts={'ok':0,'cached':0,'error':0}; errors=[]
    with ThreadPoolExecutor(max_workers=1) as ex:
        futs=[ex.submit(request_one,*j) for j in jobs]
        for i,f in enumerate(as_completed(futs), 1):
            status, msg=f.result(); counts[status]=counts.get(status,0)+1
            if status=='error': errors.append(msg)
            if i % 200 == 0: print(i, counts, flush=True)
    (META/'comtrade_download_errors.txt').write_text('\n'.join(errors), encoding='utf-8')
    # Flatten all successful raw files into one CSV, deduplicated by key.
    out = PROC/'brics11_comtrade_2000_2025_hs2017_queries.csv'
    fields=['reporter_iso','reporter_code','reporter_desc','partner_iso','partner_code','partner_desc','year','period','flow_code','flow_desc','classification_code','classification_search_code','is_original_classification','cmd_code','cmd_desc','customs_code','mot_code','primary_value','customs_value','cifvalue','fobvalue','net_wgt','gross_wgt','quantity','quantity_unit','quantity_unit_desc','is_aggregate','is_reported','source_file']
    seen=set(); n=0
    with out.open('w', newline='', encoding='utf-8') as fh:
        w=csv.DictWriter(fh, fieldnames=fields); w.writeheader()
        for p in sorted(RAW.glob('*/*/*/*/*.json')):
            parts=p.parts
            try:
                year=int(parts[-5]); rep=parts[-4]; par=parts[-3]; flow=parts[-2]; cmd=p.stem
            except Exception: continue
            try: obj=json.loads(p.read_text(encoding='utf-8'))
            except Exception: continue
            for row in flatten(obj,rep,par,flow,year,cmd):
                key=(row['reporter_iso'],row['partner_iso'],row['year'],row['flow_code'],row['cmd_code'])
                if key in seen: continue
                seen.add(key); row['source_file']=str(p.relative_to(ROOT)); w.writerow(row); n+=1
    sha=hashlib.sha256(out.read_bytes()).hexdigest()
    metadata={'source':'UN Comtrade Public API preview','api_base':BASE,'downloaded_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'countries':COUNTRIES,'years':[2000,2025],'flows':FLOWS,'commands':CMD_CODES,'jobs':len(jobs),'download_counts':counts,'rows_processed':n,'processed_file':str(out.relative_to(ROOT)),'processed_sha256':sha,'notes':['2025 may be provisional by reporter.','AG2 and TOTAL queries preserve UN Comtrade API output and classification metadata.','Do not treat this HS query set as fully harmonised HS 2017 for years before 2017; inspect classification_code and is_original_classification.']}
    (META/'comtrade_brics11_2000_2025_manifest.json').write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding='utf-8')
    print('DONE', counts, 'rows', n, 'out', out, 'sha256', sha)

if __name__=='__main__': main()
