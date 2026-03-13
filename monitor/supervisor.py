#!/usr/bin/env python3
import json, time, datetime, subprocess, re, signal
from pathlib import Path

MON = Path('/root/.openclaw/workspace/monitor')
STATUS = MON / 'alpha-status.json'
CONTROL = MON / 'alpha-control.json'
STATE = MON / 'supervisor-state.json'
AUTOLOG = MON / 'alpha_autoloop.log'
AUTOPID = MON / 'alpha_autoloop.pid'
AUTO = MON / 'alpha_autoloop.py'
THRESHOLD = 600


def now():
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')


def read_json(p, d=None):
    try:
        return json.loads(p.read_text())
    except Exception:
        return {} if d is None else d


def write_json(p, obj):
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2))


def parse_time(s):
    if not s:
        return None
    for fmt in ['%Y-%m-%d %H:%M:%S UTC', '%Y-%m-%d %H:%M UTC']:
        try:
            return datetime.datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def pid_alive(pid):
    try:
        Path(f'/proc/{pid}').exists()
        return Path(f'/proc/{pid}').exists()
    except Exception:
        return False


def last_log_time():
    if not AUTOLOG.exists():
        return None
    try:
        return datetime.datetime.utcfromtimestamp(AUTOLOG.stat().st_mtime)
    except Exception:
        return None


def ensure_loop_running(state):
    pid = None
    try:
        pid = int(AUTOPID.read_text().strip())
    except Exception:
        pid = None
    if pid and pid_alive(pid):
        state['loopPid'] = pid
        return state, False
    proc = subprocess.Popen(['nohup', 'python3', str(AUTO)], cwd=str(MON.parent), stdout=open(AUTOLOG, 'a'), stderr=subprocess.STDOUT, preexec_fn=None)
    AUTOPID.write_text(str(proc.pid))
    state['loopPid'] = proc.pid
    state['lastAction'] = 'restarted_autoloop'
    return state, True


def main():
    while True:
        st = read_json(STATE, {})
        a = read_json(STATUS, {})
        c = read_json(CONTROL, {})
        st['updatedAt'] = now()
        st, restarted = ensure_loop_running(st)
        if restarted:
            st['status'] = 'restarted'
        alpha_t = parse_time(a.get('updatedAt'))
        log_t = last_log_time()
        latest = None
        if alpha_t and log_t:
            latest = max(alpha_t, log_t)
        else:
            latest = alpha_t or log_t
        st['lastAlphaStatusAt'] = a.get('updatedAt')
        st['lastApiActivityAt'] = latest.strftime('%Y-%m-%d %H:%M:%S UTC') if latest else None
        fault = False
        if latest:
            idle = (datetime.datetime.utcnow() - latest).total_seconds()
            if idle > THRESHOLD:
                fault = True
                st['faultCount'] = int(st.get('faultCount', 0)) + 1
                st['lastFault'] = f'idle_for_{int(idle)}s'
                c['enabled'] = False
                write_json(CONTROL, c)
                pid = st.get('loopPid')
                if pid and pid_alive(pid):
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except Exception:
                        pass
                st['status'] = 'fault'
                st['lastAction'] = 'stopped_autoloop_due_to_idle'
        if not fault and not restarted:
            st['status'] = 'healthy'
            st['lastAction'] = 'checked_ok'
        write_json(STATE, st)
        time.sleep(30)


if __name__ == '__main__':
    import os
    main()
