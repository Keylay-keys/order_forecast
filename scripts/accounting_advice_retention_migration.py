"""Apply the checksummed matched-advice function; never grant originals UPDATE."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import psycopg2
from psycopg2 import sql

ROOT=Path(__file__).resolve().parents[1]
MIGRATION=ROOT/'migrations/accounting/0003_payment_advice_retention.sql'
SIGNATURE='public.accounting_discard_matched_advice(uuid,text,text,uuid,bytea,jsonb)'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true')
    apply=parser.parse_args().apply
    digest=hashlib.sha256(MIGRATION.read_bytes()).hexdigest()
    if MIGRATION.with_suffix('.sha256').read_text().strip()!=digest:
        raise RuntimeError('Retention migration checksum mismatch')
    target={key:os.environ.get('POSTGRES_'+key) for key in ('HOST','PORT','DB')}
    if not all(target.values()) or target!={key:os.environ.get('WEB_API_POSTGRES_'+key) for key in target}:
        raise RuntimeError('Migration target differs from the declared API database')
    runtime=os.environ['ACCOUNTING_RUNTIME_ROLE']
    if not runtime or len(runtime)>63 or os.environ.get('PGSSLMODE')!='verify-full':
        raise RuntimeError('Runtime role and verified database TLS are required')
    conn=psycopg2.connect(host=target['HOST'],port=target['PORT'],dbname=target['DB'],
        user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'],
        sslmode='verify-full',sslrootcert=os.environ['PGSSLROOTCERT'],connect_timeout=10)
    marker='RouteSpark matched advice retention '+digest
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout='5s'")
            cur.execute("SET LOCAL statement_timeout='15s'")
            cur.execute('SELECT to_regprocedure(%s)',(SIGNATURE,))
            existing=cur.fetchone()[0]
            if existing:
                cur.execute("SELECT obj_description(%s::regprocedure,'pg_proc')",(SIGNATURE,))
                if cur.fetchone()[0]!=marker:
                    raise RuntimeError('Existing retention function is not this reviewed migration')
            if apply:
                cur.execute(MIGRATION.read_text())
                cur.execute(sql.SQL('GRANT EXECUTE ON FUNCTION '+SIGNATURE+' TO {}').format(sql.Identifier(runtime)))
                cur.execute('COMMENT ON FUNCTION '+SIGNATURE+' IS %s',(marker,))
            cur.execute("SELECT has_column_privilege(%s,'public.accounting_email_originals','content','UPDATE'), has_table_privilege(%s,'public.accounting_email_originals','DELETE')",(runtime,runtime))
            if cur.fetchone()!=(False,False):
                raise RuntimeError('Runtime originals privileges are broader than expected')
            cur.execute("SELECT p.prosecdef,pg_get_userbyid(p.proowner)<>%s,p.proconfig,has_function_privilege(%s,p.oid,'EXECUTE'),NOT EXISTS(SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE') FROM pg_proc p WHERE p.oid=to_regprocedure(%s)",(runtime,runtime,SIGNATURE))
            row=cur.fetchone()
            if row!=(True,True,['search_path=pg_catalog'],True,True):
                raise RuntimeError('Retention function ownership or permissions failed verification')
        conn.commit() if apply else conn.rollback()
        print(json.dumps(dict(ready=True,applied=apply,checksum=digest,originalsUpdateGranted=False,originalsDeleteGranted=False)))
    except BaseException:
        conn.rollback();raise
    finally:conn.close()

if __name__=='__main__':main()
