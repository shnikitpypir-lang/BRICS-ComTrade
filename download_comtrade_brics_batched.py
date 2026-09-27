#!/usr/bin/env python3
import os, csv, json, time, hashlib, re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

ROOT=Path('/home/user'); RAW=ROOT/'data/raw/comtrade_batched'; PROC=ROOT/'data/processed/block_b'; META=ROOT/'metadata'
RAW.mkdir(parents=True,exist_ok=True); PROC.mkdir(parents=True,exist_ok=True); META.mkdir(parents=True,exist_ok=True)
KEY=os.environ.get('UN_COMTRADE_KEY')
if not KEY: raise SystemExit('UN_COMTRADE_KEY is not set')
COUNTRIES={'BRA':76,'RUS':643,'IND':356,'CHN':156,'ZAF':710,'SAU':682,'EGY':818,'ETH':231,'IRN':364,'ARE':784,'IDN':360}
PARTNERS=','.join(str(x) for x in COUNTRIES.values())
YEARS=range(2000,2026); FLOWS={'X':'Export','M':'Import'}; CMDS=['TOTAL','AG2']
BASE='https://comtradeapi.un.org/data/v1/get/C/A/HS'

s=requests.Session()
def job(rep,year,flow,cmd):
    d=RAW/str(year)/rep/flow; d.mkdir(parents=True,exist_ok=True); p=d/f'{cmd}.json'
    if p.exists() and p.stat().st_size>20:return 'cached',str(p)
    params={'cmdCode':cmd,'period':year,'reporterCode':COUNTRIES[rep],'partnerCode':PARTNERS,'flowCode':flow,'maxRecords':250000,'customsCode':'C00','motCode':0,'partner2Code':0,'subscription-key':KEY}
    last=''; transient=0
    while transient<10:
        try:
            r=s.get(BASE,params=params,timeout=180); last=f'{r.status_code}: {r.text[:300]}'
            if r.status_code==200:
                o=r.json(); o['_request_url']=r.url; o['_downloaded_at_utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()); p.write_text(json.dumps(o,ensure_ascii=False),encoding='utf-8'); return 'ok',str(p)
            if r.status_code==403 and 'quota' in r.text.lower():
                m=re.search(r'(\d{2}):(\d{2}):(\d{2})',r.text)
                wait=3605
                if m:
                    h,mi,se=[int(z) for z in m.groups()]; wait=h*3600+mi*60+se+15
                print(f'Quota exhausted; sleeping {wait} seconds', flush=True)
                time.sleep(wait); continue
            if r.status_code==429:
                m=re.search(r'Try again in (\d+) seconds',r.text); time.sleep(int(m.group(1))+2 if m else 5); continue
            if r.status_code in (500,502,503,504): transient+=1; time.sleep(min(60,2**transient)); continue
            break
        except Exception as e: last=repr(e); transient+=1; time.sleep(min(60,2**transient))
    err=d/f'{cmd}.error.txt'; err.write_text(last,encoding='utf-8'); return 'error',f'{rep}-{year}-{flow}-{cmd}: {last}'

def main():
    jobs=[(rep,y,f,c) for rep in COUNTRIES for y in YEARS for f in FLOWS for c in CMDS]; counts={'ok':0,'cached':0,'error':0}; errors=[]
    print('Jobs:',len(jobs),flush=True)
    with ThreadPoolExecutor(max_workers=4) as ex:
        fs=[ex.submit(job,*x) for x in jobs]
        for i,f in enumerate(as_completed(fs),1):
            st,msg=f.result(); counts[st]+=1
            if st=='error': errors.append(msg)
            if i%50==0: print(i,counts,flush=True)
    (META/'comtrade_batched_errors.txt').write_text('\n'.join(errors),encoding='utf-8')
    fields=['reporter_iso','reporter_code','reporter_desc','partner_iso','partner_code','partner_desc','year','period','flow_code','flow_desc','classification_code','classification_search_code','is_original_classification','cmd_code','cmd_desc','customs_code','mot_code','primary_value','customs_value','cifvalue','fobvalue','net_wgt','gross_wgt','quantity','quantity_unit','quantity_unit_desc','is_aggregate','is_reported','source_file']
    out=PROC/'brics11_comtrade_2000_2025_batched.csv'; seen=set(); n=0
    with out.open('w',newline='',encoding='utf-8') as fh:
        w=csv.DictWriter(fh,fieldnames=fields); w.writeheader()
        for p in sorted(RAW.glob('*/*/*/*.json')):
            try: year=int(p.parts[-4]); rep=p.parts[-3]; flow=p.parts[-2]; cmd=p.stem
            except: continue
            try:o=json.loads(p.read_text(encoding='utf-8'))
            except:continue
            for x in o.get('data',[]):
                riso=x.get('reporterISO') or rep; piso=x.get('partnerISO')
                if riso not in COUNTRIES or piso not in COUNTRIES or riso==piso: continue
                row={'reporter_iso':riso,'reporter_code':x.get('reporterCode'),'reporter_desc':x.get('reporterDesc'),'partner_iso':piso,'partner_code':x.get('partnerCode'),'partner_desc':x.get('partnerDesc'),'year':x.get('refYear',year),'period':x.get('period'),'flow_code':x.get('flowCode',flow),'flow_desc':x.get('flowDesc'),'classification_code':x.get('classificationCode'),'classification_search_code':x.get('classificationSearchCode'),'is_original_classification':x.get('isOriginalClassification'),'cmd_code':x.get('cmdCode',cmd),'cmd_desc':x.get('cmdDesc'),'customs_code':x.get('customsCode'),'mot_code':x.get('motCode'),'primary_value':x.get('primaryValue'),'customs_value':x.get('customsValue'),'cifvalue':x.get('CIFValue'),'fobvalue':x.get('FOBValue'),'net_wgt':x.get('netWgt'),'gross_wgt':x.get('grossWgt'),'quantity':x.get('qty'),'quantity_unit':x.get('qtyUnit'),'quantity_unit_desc':x.get('qtyUnitDesc'),'is_aggregate':x.get('isAggregate'),'is_reported':x.get('isReported'),'source_file':str(p.relative_to(ROOT))}
                key=(riso,piso,row['year'],row['flow_code'],row['cmd_code'])
                if key not in seen:seen.add(key);w.writerow(row);n+=1
    sha=hashlib.sha256(out.read_bytes()).hexdigest(); manifest={'source':'UN Comtrade Data API','endpoint':BASE,'downloaded_at_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'countries':COUNTRIES,'partners':list(COUNTRIES),'years':[2000,2025],'flows':FLOWS,'cmds':CMDS,'jobs':len(jobs),'counts':counts,'rows':n,'output':str(out.relative_to(ROOT)),'sha256':sha,'classification_note':'HS endpoint preserves reported classification; classification_code and is_original_classification must be audited for harmonisation across 2000-2025.'}
    (META/'comtrade_batched_manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8'); print('DONE',counts,'rows',n,'sha256',sha,flush=True)
if __name__=='__main__':main()
