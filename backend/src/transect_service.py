import math
import numpy as np
from sqlalchemy import select,func
from .models import Transect,Echo,EchoBatch
from .common import get,reject,finish,cruise_lock,transect_lock

def length(lon1,lat1,lon2,lat2):return math.hypot(math.radians(lon2-lon1)*6371000*math.cos(math.radians((lat1+lat2)/2)),math.radians(lat2-lat1)*6371000)
def create(db,data,user):
    c=cruise_lock(db,data.cruise_id);reject(c.status not in ('planned','sailing'),'现场调查已结束，不能增加航段')
    reject(data.probe_khz not in (38,120,200),'探头频率只支持38/120/200kHz')
    distance=length(data.start_lon,data.start_lat,data.end_lon,data.end_lat)
    reject(distance<80 or distance>20000,'航段长度需80米至20公里')
    reject(db.scalar(select(Transect).where(Transect.cruise_id==c.id,Transect.code==data.code)) is not None,'航次内航段编号重复')
    row=Transect(**data.model_dump(),length_m=distance);db.add(row);db.flush();return finish(db,user,'规划声学航段',row)
def next_batch_no(db,transect_id):
    value=db.scalar(select(func.coalesce(func.max(EchoBatch.batch_no),0)).where(EchoBatch.transect_id==transect_id))
    return value+1
def _create_batch(db,t,batch_no,gain_db,source,note=''):
    batch=EchoBatch(transect_id=t.id,batch_no=batch_no,recorded_gain_db=gain_db,source=source,note=note)
    db.add(batch);db.flush();return batch
def record(db,id,points,user,source='manual',batch_no=None,gain_db=None):
    c,t=transect_lock(db,id,True)
    # 早期未分批的回波按一批处理；新录入每次自成一批，同航段批次号唯一。
    if batch_no is None:batch_no=next_batch_no(db,id)
    else:reject(db.scalar(select(EchoBatch).where(EchoBatch.transect_id==id,EchoBatch.batch_no==batch_no)) is not None,'该批次号已存在，请使用新批次号或留空自动编号')
    batch=_create_batch(db,t,batch_no,t.gain_db if gain_db is None else gain_db,source)
    for p in points:
        reject(p['along_m']>t.length_m,'回波距离超出航段范围')
        f=p['along_m']/t.length_m;expected_lon=t.start_lon+(t.end_lon-t.start_lon)*f;expected_lat=t.start_lat+(t.end_lat-t.start_lat)*f
        reject(length(expected_lon,expected_lat,p['lon'],p['lat'])>100,'回波坐标偏离航段超过100米')
        db.add(Echo(transect_id=id,batch_id=batch.id,**p,source=source))
    t.data_version+=1;t.status='acquired';return finish(db,user,'记录第%d批探头回波'%batch_no,t)
def correct_gain(db,id,batch_no,data,user):
    c,t=transect_lock(db,id,True)
    batch=db.scalar(select(EchoBatch).where(EchoBatch.transect_id==id,EchoBatch.batch_no==batch_no))
    reject(batch is None,'该航段不存在此回波批次')
    reject(not db.scalar(select(Echo.id).where(Echo.batch_id==batch.id).limit(1)),'该批次没有回波，不能更正增益')
    # 原读数（recorded_gain_db 与 echoes.strength_db）保持不变，只落账更正后的增益。
    batch.corrected_gain_db=data.gain_db;batch.note=(batch.note+' ' if batch.note else '')+data.note.strip()
    batch.note=batch.note.strip()[:200]
    # 更正落账即新版本：旧识别全部过期，重算只调整该批回波的输入。
    t.data_version+=1
    db.flush();return finish(db,user,'按批更正探头增益（第%d批）'%batch_no,batch)
def effective_strength(echo,batch):
    # 以更正后增益与原始记录增益之差，一次性折算该批回波强度；重复重算不叠加。
    if batch is None or batch.corrected_gain_db is None:return echo.strength_db
    return echo.strength_db-(batch.corrected_gain_db-batch.recorded_gain_db)
def analysis_input(db,t):
    # 全段重算输入：仅被更正批次的回波强度按增益差调整，其余批次保持原读数。
    batches={b.id:b for b in db.scalars(select(EchoBatch).where(EchoBatch.transect_id==t.id))}
    points=[]
    for p in db.scalars(select(Echo).where(Echo.transect_id==t.id).order_by(Echo.id)):
        row={c.name:getattr(p,c.name) for c in p.__table__.columns}
        row['strength_db']=effective_strength(p,batches.get(p.batch_id))
        points.append(row)
    return points
def backfill_batches(db):
    # 早期未分批的回波统一按一批处理，幂等执行，不改动任何原始读数。
    for t in db.scalars(select(Transect)):
        legacy=list(db.scalars(select(Echo).where(Echo.transect_id==t.id,Echo.batch_id.is_(None)).order_by(Echo.id)))
        if not legacy:continue
        no=next_batch_no(db,t.id)
        batch=EchoBatch(transect_id=t.id,batch_no=no,recorded_gain_db=t.gain_db,source=legacy[0].source,note='早期未分批回波按一批处理')
        db.add(batch);db.flush()
        for e in legacy:e.batch_id=batch.id
        db.flush()
    db.commit()
def synthetic_points(t):
    rng=np.random.default_rng(37+t.id);points=[]
    for center,depth,strength in [(.18,7,-46),(.51,14,-39),(.83,21,-52)]:
        for _ in range(24):
            along=float(np.clip(rng.normal(center*t.length_m,min(12,t.length_m*.018)),0,t.length_m));f=along/t.length_m
            points.append({'along_m':along,'lon':t.start_lon+(t.end_lon-t.start_lon)*f,'lat':t.start_lat+(t.end_lat-t.start_lat)*f,'depth_m':float(rng.normal(depth,.45)),'strength_db':float(rng.normal(strength,1.1))})
    for _ in range(8):
        along=float(rng.uniform(0,t.length_m));f=along/t.length_m
        points.append({'along_m':along,'lon':t.start_lon+(t.end_lon-t.start_lon)*f,'lat':t.start_lat+(t.end_lat-t.start_lat)*f,'depth_m':float(rng.uniform(3,28)),'strength_db':float(rng.uniform(-85,-25))})
    return points
def simulate(db,id,user):
    c,t=transect_lock(db,id,True)
    reject(db.scalar(select(Echo).where(Echo.transect_id==id)) is not None,'该航段已有回波；模拟导入只适用于空航段')
    return record(db,id,synthetic_points(t),user,'synthetic-probe-seed37')
