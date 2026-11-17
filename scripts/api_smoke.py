"""CI-only live HTTP smoke test against the already seeded disposable database."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import httpx
from matrix_db.api_auth import Settings
from matrix_db.api_jobs import run_one
from matrix_db.client import MatrixDBClient
from matrix_db.storage import connect

settings=Settings.from_environment()
token=json.loads(os.environ['MATRIX_DB_API_KEYS'])[0]['token']
server=subprocess.Popen([sys.executable,'-c','from matrix_db.api import main; main()'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
try:
    for attempt in range(50):
        try:
            if httpx.get('http://127.0.0.1:8000/health/live').status_code==200:break
        except httpx.TransportError:pass
        time.sleep(.1)
    else:raise RuntimeError('API did not become live')
    with MatrixDBClient('http://127.0.0.1:8000',token) as client:
        assert client.datasets()['items']
        page=client.query({'dataset_id':'demo-weather','limit':1})
        assert page['items'] and 'revision' in page['items'][0]
        job=client.submit_job('observations',{'dataset_id':'demo-weather','limit':1})
        assert run_one(settings,connect)
        assert client.job(job['id'])['status']=='completed'
    environment={**os.environ,'MATRIX_DB_TEST_API_URL':'http://127.0.0.1:8000','MATRIX_DB_TEST_API_TOKEN':token}
    subprocess.run(['node','clients/typescript/tests/live.mjs'],check=True,env=environment)
    print('Live API, Python client, TypeScript client and worker smoke checks passed')
finally:
    server.terminate()
    try:server.wait(timeout=10)
    except subprocess.TimeoutExpired:server.kill();server.wait()
