"""回归：原有完整调查剧本在批次化模型下端到端仍可走通，并验证封存快照含批次。"""
import os,sys,tempfile
from datetime import datetime,date
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'..','..','backend'))
os.environ.setdefault('DATABASE_URL','sqlite:///'+tempfile.mktemp(suffix='.db'))
os.environ.setdefault('REDIS_URL','redis://localhost:6399/0')
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('SEED_DEMO','true')
import fakeredis
import src.school_service as school
fake=fakeredis.FakeStrictRedis(decode_responses=True);school.queue=fake
import src.worker as wm;wm.queue=fake
from fastapi.testclient import TestClient
from sqlalchemy import select
from src.app import app
from src.database import Session
from src import models
PASS=[];FAIL=[]
def check(n,c,d=''):
    (PASS if c else FAIL).append(n);print(('  ok  ' if c else ' FAIL ')+n+('' if c else ' — '+d))
def drain():
    while fake.llen('estuary:schools'):
        item=fake.blpop('estuary:schools',timeout=1)
        if item:wm.process_once(item)
with TestClient(app) as client:
    op={'Authorization':'Bearer '+client.post('/auth/login',json={'username':'operator','password':'Estuary@2026'}).json()['token']}
    sv={'Authorization':'Bearer '+client.post('/auth/login',json={'username':'supervisor','password':'Estuary@2026'}).json()['token']}
    with Session() as db:
        cruise=db.scalar(select(models.Cruise).where(models.Cruise.code=='CY031'));cid=cruise.id
        e1=db.scalar(select(models.Transect).where(models.Transect.cruise_id==cid,models.Transect.code=='E1'));e1id=e1.id
    # 改航窗避开潮汐禁航，再开航
    r=client.patch(f'/cruises/{cid}/schedule',json={'depart':'2026-11-18T14:15:00+08:00','arrive':'2026-11-18T17:00:00+08:00'},headers=op);check('调整航窗',r.status_code==200,str(r.status_code))
    r=client.post(f'/cruises/{cid}/launch',headers=sv);check('主管开航',r.status_code==200,str(r.status_code))
    # 模拟回波（自动成批）
    r=client.post(f'/transects/{e1id}/simulate',json={},headers=op);check('模拟回波自动成批',r.status_code==200,str(r.status_code)+r.text[:120])
    ws=client.get('/workspace',headers=op).json()
    check('模拟回波归属一个批次',len({e['batch_id'] for e in ws['echoes'] if e['transect_id']==e1id})==1)
    # 识别+worker+复核
    r=client.post('/schools',json={'transect_id':e1id,'eps':1.2,'min_samples':4},headers=op);aid=r.json()['id'];check('提交识别',r.status_code==200)
    drain()
    with Session() as db:a=db.get(models.SchoolAnalysis,aid)
    check('识别done',a.status=='done',a.error)
    r=client.post(f'/schools/{aid}/review',json={'noise_note':'孤立回波经核对为底质干扰并已标识','decision':'鱼群聚集可用于本次调查图层，保留噪点标注'},headers=sv)
    check('复核通过',r.status_code==200,str(r.status_code))
    # 网具复校通过 + 生物样
    with Session() as db:net=db.scalar(select(models.Net).where(models.Net.code=='N31'));nid=net.id
    r=client.post('/calibrations',json={'net_id':nid,'measured_mm':10.2,'valid_until':'2026-12-31','certificate':'N31-复校-20261118'},headers=sv)
    check('网具复校通过',r.status_code==200 and r.json()['passed'],str(r.status_code));calid=r.json()['id']
    with Session() as db:a=db.get(models.SchoolAnalysis,aid);label=next(x['label'] for x in a.assignments if x['label']>=0)
    r=client.post('/samples',json={'transect_id':e1id,'calibration_id':calid,'cluster_label':label,'species':'模拟鲻鱼','count':12,'mean_length_mm':125},headers=op)
    check('生物样关联',r.status_code==200,str(r.status_code)+r.text[:120])
    # 补航规划->回波->识别->复核->样本->完成
    with Session() as db:gap=db.scalar(select(models.MissingInterval).where(models.MissingInterval.transect_id==e1id));gid=gap.id
    r=client.post('/resurveys',json={'gap_id':gid,'code':'E1-S1'},headers=sv);check('规划补航',r.status_code==200,str(r.status_code));rid=r.json()['id']
    with Session() as db:
        res=db.get(models.Resurvey,rid);cont=res.continuation_id
        t=db.get(models.Transect,cont)
    r=client.post(f'/transects/{cont}/simulate',json={},headers=op);check('补航回波',r.status_code==200,str(r.status_code))
    r=client.post('/schools',json={'transect_id':cont,'eps':1.2,'min_samples':4},headers=op);aid2=r.json()['id']
    drain()
    r=client.post(f'/schools/{aid2}/review',json={'noise_note':'补航孤立回波为底质干扰并已标识','decision':'补航鱼群聚集可用，保留噪点标注'},headers=sv)
    check('补航复核',r.status_code==200,str(r.status_code))
    r=client.post('/samples',json={'transect_id':cont,'calibration_id':calid,'cluster_label':0,'species':'模拟鲻鱼','count':6,'mean_length_mm':120},headers=op)
    check('补航生物样',r.status_code==200,str(r.status_code)+r.text[:120])
    r=client.post(f'/resurveys/{rid}/complete',json={'evidence':'补航回波群落复核与网具抽样齐全，缺测范围已覆盖'},headers=sv)
    check('补航接续完成',r.status_code==200,str(r.status_code))
    # E2 同样补齐
    with Session() as db:
        e2=db.scalar(select(models.Transect).where(models.Transect.cruise_id==cid,models.Transect.code=='E2'));e2id=e2.id
    client.post(f'/transects/{e2id}/simulate',json={},headers=op)
    r=client.post('/schools',json={'transect_id':e2id,'eps':1.2,'min_samples':4},headers=op);aid3=r.json()['id']
    drain()
    r=client.post(f'/schools/{aid3}/review',json={'noise_note':'E2孤立回波为底质干扰并已标识','decision':'E2鱼群聚集可用，保留噪点标注'},headers=sv)
    check('E2复核',r.status_code==200,str(r.status_code))
    r=client.post('/samples',json={'transect_id':e2id,'calibration_id':calid,'cluster_label':0,'species':'模拟鲻鱼','count':8,'mean_length_mm':122},headers=op)
    check('E2生物样',r.status_code==200,str(r.status_code))
    # 结束现场 + 封存
    r=client.post(f'/cruises/{cid}/return',headers=sv);check('返航',r.status_code==200,str(r.status_code))
    r=client.post('/releases',json={'cruise_id':cid,'recipient':'潮汐渔研站资源调查组'},headers=sv)
    check('图层封存',r.status_code==200,str(r.status_code)+r.text[:200]);rel=r.json()
    check('快照每层含batches',all('batches' in L for L in rel['snapshot']['layers']))
    check('快照回波保留原读数字段strength_db',all('strength_db' in e for L in rel['snapshot']['layers'] for e in L['echoes']))
    import json,hashlib
    check('canonical校验和一致',hashlib.sha256(rel['canonical_json'].encode('utf-8')).hexdigest()==rel['sha256'])
    check('canonical可还原快照',json.loads(rel['canonical_json'])==rel['snapshot'])
    # 封存后拒绝更正
    r=client.patch(f'/transects/{e1id}/batches/1/gain',json={'gain_db':34,'note':'封存后改'},headers=op)
    check('封存后拒绝增益更正',r.status_code==409,str(r.status_code))
print('\n%d passed, %d failed'%(len(PASS),len(FAIL)))
if FAIL:sys.exit(1)
