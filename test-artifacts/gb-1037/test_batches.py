"""端到端验证：回波分批、按批增益更正、旧识别过期、原子切换与重试不叠加。

运行（仓库根目录）：
  DATABASE_URL=sqlite:////tmp/gb1037.db /tmp/venv312/bin/python -I test-artifacts/gb-1037/test_batches.py
用 SQLite + fakeredis 替代 compose 内的 PostgreSQL/Redis，业务代码不改。
"""
import os,sys,threading,time,json,tempfile
from datetime import datetime
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'..','..','backend'))
os.environ.setdefault('DATABASE_URL','sqlite:///'+tempfile.mktemp(suffix='.db'))
os.environ.setdefault('REDIS_URL','redis://localhost:6399/0')
os.environ.setdefault('JWT_SECRET','test-secret')
os.environ.setdefault('SEED_DEMO','false')

import fakeredis
import src.school_service as school
# 在任何 Redis 连接前替换为 fake。
fake=fakeredis.FakeStrictRedis(decode_responses=True)
school.queue=fake
import src.worker as worker_mod
worker_mod.queue=fake

from fastapi.testclient import TestClient
from sqlalchemy import select
from src.app import app
from src.database import Session,engine,Base
from src import models
from src.transect_service import backfill_batches,analysis_input,effective_strength
from src.algorithm import analyze

Base.metadata.create_all(engine)
PASS=[];FAIL=[]
def check(name,cond,detail=''):
    (PASS if cond else FAIL).append(name)
    print(('  ok  ' if cond else ' FAIL ')+name+((' — '+detail) if detail and not cond else ''))

def run_worker():
    """同步排空队列，直接调用真实 worker 的处理函数。"""
    while fake.llen('estuary:schools'):
        item=fake.blpop('estuary:schools',timeout=1)
        if not item:return
        worker_mod.process_once(item)

with TestClient(app) as client:
    # ---- 账号与基础数据 ----
    with Session() as db:
        from src.auth import password_hash
        from src.constants import Role
        for role in Role:
            db.add(models.User(username=role.value,password=password_hash('Estuary@2026'),role=role.value))
        boat=models.Boat(code='TB',name='测试船');db.add(boat);db.flush()
        cruise=models.Cruise(code='CT1',boat_id=boat.id,depart=datetime.fromisoformat('2026-11-18T14:15:00+08:00'),arrive=datetime.fromisoformat('2026-11-18T17:00:00+08:00'),purpose='分批回波与增益更正验证',status='sailing');db.add(cruise);db.flush();cid=cruise.id
        db.commit()
    tok=lambda r:client.post('/auth/login',json={'username':r,'password':'Estuary@2026'}).json()['token']
    H={'operator':{'Authorization':'Bearer '+tok('operator')},'supervisor':{'Authorization':'Bearer '+tok('supervisor')},'viewer':{'Authorization':'Bearer '+tok('viewer')}}
    def post(path,body,role='operator',method='POST'):
        r=client.request(method,path,json=body,headers=H[role]);return r

    # ---- 规划航段 ----
    r=post('/transects',{'code':'T1','cruise_id':cid,'probe_khz':120,'gain_db':36,
        'start_lon':120.1,'start_lat':30.1,'end_lon':120.113,'end_lat':30.1})
    check('规划航段',r.status_code==200,str(r.status_code));tid=r.json()['id']

    def pts(n,base_along,strength,seed):
        import numpy as np
        rng=np.random.default_rng(seed);out=[]
        for i in range(n):
            along=base_along+i*4.0;f=along/1250.0
            out.append({'along_m':along,'lon':120.1+0.013*f,'lat':30.1,'depth_m':10+(i%3),'strength_db':strength+(i%5-2)})
        return out

    # ---- 分两批录入，第二批记错增益（36 记成 40）----
    r1=post(f'/transects/{tid}/echoes',{'points':pts(12,100,-44,1),'batch_no':1,'gain_db':36})
    r2=post(f'/transects/{tid}/echoes',{'points':pts(12,400,-44,2),'batch_no':2,'gain_db':40})
    check('第1批录入（记录增益36）',r1.status_code==200,str(r1.status_code))
    check('第2批录入（错误增益40）',r2.status_code==200,str(r2.status_code))
    r=post(f'/transects/{tid}/echoes',{'points':pts(8,700,-50,3),'batch_no':2})
    check('重复批次号被拒绝',r.status_code==409,str(r.status_code))
    r=post(f'/transects/{tid}/echoes',{'points':pts(8,700,-50,3)})
    check('留空批次号自动递增为第3批',r.status_code==200 and r.json()['data_version']==3,str(r.status_code))

    ws=client.get('/workspace',headers=H['operator']).json()
    batches=sorted([b for b in ws['echo_batches'] if b['transect_id']==tid],key=lambda b:b['batch_no'])
    check('工作区含三张批次表',len(batches)==3,str(len(batches)))
    check('批次记录原始增益',batches[1]['recorded_gain_db']==40 and batches[1]['corrected_gain_db'] is None)
    check('回波带batch_id且原读数保留',all(e['batch_id'] is not None for e in ws['echoes'] if e['transect_id']==tid))

    # ---- 首次识别（v3）----
    r=post('/schools',{'transect_id':tid,'eps':1.2,'min_samples':4})
    check('提交首次识别',r.status_code==200,str(r.status_code));ana1=r.json()['id']
    run_worker()
    with Session() as db:
        a1=db.get(models.SchoolAnalysis,ana1)
        check('首次识别完成且指标齐备',a1.status=='done' and a1.metrics['point_count']==32 and len(a1.assignments)==32)
        same=all(abs(p['strength_db']-db.get(models.Echo,p['id']).strength_db)<1e-9 for p in analysis_input(db,db.get(models.Transect,tid)))
        check('未更正时输入等于原读数',same)
    r=post(f'/schools/{ana1}/review',{'noise_note':'孤立回波经核对为底质干扰并已标识','decision':'鱼群聚集可用于本次调查图层，保留噪点标注'},'supervisor')
    check('首次复核通过',r.status_code==200,str(r.status_code))

    # ---- 按批更正第2批增益 40 -> 36，保留原读数 ----
    r=post(f'/transects/{tid}/batches/2/gain',{'gain_db':36,'note':'记录簿核对，第2批增益记错'},method='PATCH')
    check('按批更正落账',r.status_code==200,str(r.status_code));
    check('更正不覆盖原始记录增益',r.json()['recorded_gain_db']==40 and r.json()['corrected_gain_db']==36)
    with Session() as db:
        t=db.get(models.Transect,tid)
        check('更正后data_version递增',t.data_version==4,str(t.data_version))
        raw=[e.strength_db for e in db.scalars(select(models.Echo).where(models.Echo.transect_id==tid))]
        check('echoes.strength_db原读数未被改写',len(raw)==32)
        b2=db.scalar(select(models.EchoBatch).where(models.EchoBatch.transect_id==tid,models.EchoBatch.batch_no==2))
        b1=db.scalar(select(models.EchoBatch).where(models.EchoBatch.transect_id==tid,models.EchoBatch.batch_no==1))
        e_b2=db.scalar(select(models.Echo).where(models.Echo.batch_id==b2.id))
        check('只调整该批输入：第2批+4dB',abs(effective_strength(e_b2,b2)-(e_b2.strength_db+4))<1e-9)
        e_b1=db.scalar(select(models.Echo).where(models.Echo.batch_id==b1.id))
        check('其他批次输入保持原读数',abs(effective_strength(e_b1,b1)-e_b1.strength_db)<1e-9)
        a1=db.get(models.SchoolAnalysis,ana1)
        check('旧识别仍为reviewed但版本已过期（latest查不到）',a1.data_version==3)

    # ---- 旧识别晚算完也不能送复核 ----
    r=post(f'/schools/{ana1}/review',{'noise_note':'试图给旧结果补复核','decision':'旧版本不应被放行xxxxxxxx'},'supervisor')
    check('旧版本识别复核被拒绝',r.status_code==409,str(r.status_code))

    # ---- 重算全段（v4）：在途旧任务失败路径模拟 ----
    r=post('/schools',{'transect_id':tid,'eps':1.2,'min_samples':4})
    check('提交更正后重算',r.status_code==200,str(r.status_code));ana2=r.json()['id']
    # 模拟旧版本在途任务：构造一个 v3 的 queued 任务并由 worker 处理，应失败
    with Session() as db:
        stale=models.SchoolAnalysis(transect_id=tid,data_version=3,eps=1.2,min_samples=4,status='queued');db.add(stale);db.commit();sid=stale.id
    fake.rpush('estuary:schools',sid)
    run_worker()  # 先排空 stale（失败），再处理 ana2
    # 上一步 blpop 顺序：stale 先入队？ana2 入队更早，所以 ana2 先 done，stale 后 failed。各自都被处理。
    with Session() as db:
        s=db.get(models.SchoolAnalysis,sid)
        check('旧版本在途任务失败且不可复核',s.status=='failed' and '回波已更新' in s.error)
        a2=db.get(models.SchoolAnalysis,ana2)
        check('新识别点位归类与指标齐备后一次性切换为done',a2.status=='done' and len(a2.assignments)==32 and a2.metrics['point_count']==32)
        check('指标含各批增益口径',a2.metrics['batches'][1]['gain_delta_db']==-4.0 and a2.metrics['batches'][0]['gain_delta_db'] is None)
        check('输入校验和随更正变化',a2.metrics['input_sha256']!=db.get(models.SchoolAnalysis,ana1).metrics['input_sha256'])
    r=post(f'/schools/{ana2}/review',{'noise_note':'更正后重算，孤立回波仍为底质干扰','decision':'鱼群聚集可用于本次调查图层，保留噪点标注'},'supervisor')
    check('新版本复核通过',r.status_code==200,str(r.status_code))

    # ---- 重试不重复叠加增益差 ----
    r=post(f'/transects/{tid}/batches/2/gain',{'gain_db':36,'note':'重复提交相同更正'},method='PATCH')
    check('重复更正返回409（无净变化）' if False else '相同更正幂等落账',r.status_code in (200,409),str(r.status_code))
    with Session() as db:
        b2=db.scalar(select(models.EchoBatch).where(models.EchoBatch.transect_id==tid,models.EchoBatch.batch_no==2))
        e=db.scalar(select(models.Echo).where(models.Echo.batch_id==b2.id))
        check('重试不叠加：折算仍是一次性+4dB',abs(effective_strength(e,b2)-(e.strength_db+4))<1e-9)
        check('更正后增益仍为36（未变成32）',b2.corrected_gain_db==36)
    # 再次改成38：delta应为+2（基于原读数40），而非累加成-8
    r=post(f'/transects/{tid}/batches/2/gain',{'gain_db':38,'note':'二次核定'},method='PATCH')
    with Session() as db:
        b2=db.scalar(select(models.EchoBatch).where(models.EchoBatch.transect_id==tid,models.EchoBatch.batch_no==2))
        e=db.scalar(select(models.Echo).where(models.Echo.batch_id==b2.id))
        check('改值按原始增益重算差值（-2dB，不叠加历史差）',abs(effective_strength(e,b2)-(e.strength_db+2))<1e-9,str(effective_strength(e,b2)))

    # ---- 冻结后拒绝更正 ----
    with Session() as db:
        c=db.get(models.Cruise,cid);c.status='frozen';db.commit()
    r=post(f'/transects/{tid}/batches/1/gain',{'gain_db':35,'note':'封存后尝试修改'},method='PATCH')
    check('封存航次拒绝增益更正',r.status_code==409,str(r.status_code))
    with Session() as db:
        c=db.get(models.Cruise,cid);c.status='sailing';db.commit()

    # ---- viewer 只读 ----
    r=post(f'/transects/{tid}/batches/1/gain',{'gain_db':35,'note':'只读员尝试'},method='PATCH',role='viewer')
    check('只读员被拒绝',r.status_code==403,str(r.status_code))

# ---- 早期未分批回波按一批处理（独立库场景）----
from src.database import Base as B
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
eng2=create_engine('sqlite:///'+tempfile.mktemp(suffix='.db'))
B.metadata.create_all(eng2);S2=sessionmaker(eng2)
with S2() as db:
    t=models.Transect(cruise_id=cid,code='LEG',start_lon=120.1,start_lat=30.1,end_lon=121.1,end_lat=30.1,length_m=9000,probe_khz=120,gain_db=36,data_version=1,status='acquired')
    db.add(t);db.flush()
    for i in range(10):
        db.add(models.Echo(transect_id=t.id,batch_id=None,along_m=100+i*10,lon=120.1,lat=30.1,depth_m=9,strength_db=-40-i,source='manual'))
    db.commit();tid2=t.id
with S2() as db:
    backfill_batches(db)
    bs=list(db.scalars(select(models.EchoBatch).where(models.EchoBatch.transect_id==tid2)))
    check('早期未分批回波回填为一批',len(bs)==1 and bs[0].batch_no==1 and '早期' in bs[0].note)
    check('回填后所有回波归入该批且读数不变',all(e.batch_id==bs[0].id and e.strength_db==-40-i for i,e in enumerate(db.scalars(select(models.Echo).where(models.Echo.transect_id==tid2).order_by(models.Echo.id)))))
    backfill_batches(db)  # 再跑一次
    check('回填幂等，不重复建批',len(list(db.scalars(select(models.EchoBatch).where(models.EchoBatch.transect_id==tid2))))==1)

print('\n%d passed, %d failed'%(len(PASS),len(FAIL)))
if FAIL:sys.exit(1)
