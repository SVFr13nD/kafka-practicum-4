"""Real integration check. Run after docker compose up -d --build."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request
import urllib.parse

OUT = Path('evidence')
OUT.mkdir(exist_ok=True)


def get(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def wait_for(check, seconds=180):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except Exception as exc:
            last = exc
        time.sleep(3)
    raise RuntimeError(f'Timed out waiting for {check.__name__}: {last}')


def connect_ready():
    get('http://localhost:8083/connectors')
    return True


def running():
    status = get('http://localhost:8083/connectors/postgres-connector/status')
    (OUT / 'connector-status.json').write_text(json.dumps(status, indent=2))
    tasks = status.get('tasks', [])
    return status['connector']['state'] == 'RUNNING' and tasks and all(t['state'] == 'RUNNING' for t in tasks)


def query(expr):
    return get('http://localhost:9090/api/v1/query?' + urllib.parse.urlencode({'query': expr}))


wait_for(connect_ready)
config = json.loads(Path('connector/postgres-connector.json').read_text())['config']
request = urllib.request.Request('http://localhost:8083/connectors/postgres-connector/config',
    data=json.dumps(config).encode(), headers={'Content-Type': 'application/json'}, method='PUT')
with urllib.request.urlopen(request, timeout=30) as response:
    (OUT / 'connector-config-response.json').write_bytes(response.read())
wait_for(running)

# Wait for initial snapshot to finish before testing live changes.
def snapshot_done():
    response = query('debezium_metrics_snapshotcompleted{context="snapshot"}')
    return any(float(item['value'][1]) == 1 for item in response['data']['result'])

wait_for(snapshot_done)
with (OUT / 'test-data-output.txt').open('w') as log, Path('postgres/test-data.sql').open() as sql:
    subprocess.run(['docker', 'compose', 'exec', '-T', 'postgres', 'psql', '-v', 'ON_ERROR_STOP=1',
                    '-U', 'postgres', '-d', 'appdb'], stdin=sql, stdout=log, stderr=subprocess.STDOUT, check=True)

with (OUT / 'consumer-output.txt').open('w') as log:
    process = subprocess.Popen([sys.executable, '-u', 'consumer/consumer.py'], stdout=log, stderr=subprocess.STDOUT)
    try:
        def events_received():
            if process.poll() is not None:
                raise RuntimeError('Consumer exited before verification')
            text = (OUT / 'consumer-output.txt').read_text()
            return all(word in text for word in ['SNAPSHOT', 'INSERT', 'UPDATE', 'DELETE', 'Tombstone',
                'postgres.public.users', 'postgres.public.orders', 'john.doe@example.com', 'Keyboard'])
        wait_for(events_received, 120)
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

wait_for(running)

def metrics_ready():
    expressions = [
        'kafka_connect_connector_status{connector="postgres-connector",status="running"}',
        'kafka_connect_task_status{connector="postgres-connector",status="running"}',
        'debezium_metrics_totalnumberofeventsseen{context="streaming"}',
    ]
    results = {expr: query(expr) for expr in expressions}
    (OUT / 'prometheus-queries.json').write_text(json.dumps(results, indent=2))
    return all(any(float(item['value'][1]) > 0 for item in r['data']['result']) for r in results.values())

wait_for(metrics_ready)
# A screenshot of the actual Grafana UI; no generated or mocked panel.
from playwright.sync_api import sync_playwright
with sync_playwright() as pw:
    browser = pw.chromium.launch()
    page = browser.new_page(viewport={'width': 1600, 'height': 1100})
    page.goto('http://localhost:3000/login')
    page.locator('input[name="user"]').fill('admin')
    page.locator('input[name="password"]').fill('admin')
    page.get_by_role('button', name='Log in', exact=True).click()
    page.wait_for_url(lambda url: '/login' not in url, timeout=30000)
    if page.get_by_text('Skip', exact=True).is_visible():
        page.get_by_text('Skip', exact=True).click()
    page.goto('http://localhost:3000/d/kafka-connect-debezium?from=now-15m&to=now&kiosk')
    page.get_by_text('Статус коннектора', exact=True).wait_for(timeout=60000)
    page.get_by_text('RUNNING', exact=True).wait_for(timeout=60000)
    page.wait_for_timeout(5000)
    page.screenshot(path=str(OUT / 'grafana.png'), full_page=True)
    browser.close()

sha = os.environ.get('GITHUB_SHA', subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip())
run = os.environ.get('GITHUB_RUN_ID', '')
repo = os.environ.get('GITHUB_REPOSITORY', 'SVFr13nD/kafka-practicum-4')
(OUT / 'README.md').write_text(f'''# Подтверждение реального запуска\n\nПроверен коммит `{sha}`.\n\nЗапуск: https://github.com/{repo}/actions/runs/{run}\n\n- Коннектор и все задачи: RUNNING — [JSON](connector-status.json).\n- Snapshot, INSERT, UPDATE, DELETE, tombstone из двух топиков — [вывод consumer](consumer-output.txt).\n- Выполнение test-data.sql — [вывод PostgreSQL](test-data-output.txt).\n- Непустые метрики — [ответы Prometheus](prometheus-queries.json).\n\n![Grafana после выполнения SQL](grafana.png)\n''')
print('PASS: running connector/task, both topics, live CDC operations, Prometheus, Grafana screenshot')
