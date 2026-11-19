import logging,time,json
from pathlib import Path
from sqlalchemy import select,func
from .database import Session
from .models import SchoolAnalysis,Transect,Echo,EchoBatch
from .school_service import queue
from .transect_service import analysis_input
from .algorithm import analyze
logging.basicConfig(level=logging.INFO)
def process_once(item):
    with Session() as db:
        row=db.get(SchoolAnalysis,int(item[1]))
        if not row or row.status!='queued':return
        row.status='running';db.commit()
        try:
            t=db.get(Transect,row.transect_id)
            if row.data_version!=t.data_version:raise ValueError('回波已更新，请重新提交最新版本')
            # 全段重算：只调整被更正批次的回波输入，旧版本任务直接失败且不可复核。
            points=analysis_input(db,t)
            if len(points)<8:raise ValueError('至少8个回波点才可识别群落')
            assignments,metrics=analyze(points,row.eps,row.min_samples)
            batches=list(db.scalars(select(EchoBatch).where(EchoBatch.transect_id==t.id).order_by(EchoBatch.batch_no)))
            metrics['batches']=[{'batch_no':b.batch_no,'recorded_gain_db':b.recorded_gain_db,'corrected_gain_db':b.corrected_gain_db,'gain_delta_db':None if b.corrected_gain_db is None else round(b.corrected_gain_db-b.recorded_gain_db,3),'echo_count':db.scalar(select(func.count()).select_from(Echo).where(Echo.batch_id==b.id))} for b in batches]
            # 点位归类与指标在同一事务内一次性齐备后才切换为 done，避免半成品被复核。
            if not assignments or not metrics or {x['echo_id'] for x in assignments}!={p['id'] for p in points}:raise ValueError('识别结果不完整，点位归类或指标缺失')
            row.assignments,row.metrics=assignments,metrics;row.status='done';row.error=''
            try:
                Path('/artifacts').mkdir(exist_ok=True);Path(f'/artifacts/school-{row.id}.json').write_text(json.dumps(row.metrics,ensure_ascii=False))
            except OSError:logging.warning('artifact write skipped for analysis %s',row.id)
        except Exception as e:row.status='failed';row.error=str(e)[:200]
        db.commit()
def main():
    while True:
        Path('/tmp/worker-heartbeat').touch()
        try:
            item=queue.blpop('estuary:schools',timeout=2)
            if not item:continue
            process_once(item)
        except Exception:logging.exception('echo worker');time.sleep(1)
if __name__=='__main__':main()
