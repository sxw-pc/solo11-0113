"""Staged publication with weighted recipient consent and immutable wave scope."""
import hashlib
import json
from .store import connect
from .custody import decide


def create(path, rollout_id, version, waves):
    if not rollout_id or not waves:
        raise ValueError('empty_rollout')
    normalized = []
    for wave in waves:
        members = wave.get('members', [])
        if not members or len({m['recipient'] for m in members}) != len(members):
            raise ValueError('bad_wave_members')
        if any(not isinstance(m.get('weight'), int) or m['weight'] <= 0 for m in members):
            raise ValueError('bad_member_weight')
        quorum = wave.get('quorum')
        if not isinstance(quorum, int) or not 0 < quorum <= sum(m['weight'] for m in members):
            raise ValueError('bad_quorum')
        normalized.append({'name': wave['name'], 'quorum': quorum,
                           'members': sorted([{'recipient': m['recipient'], 'weight': m['weight'],
                                               'mandatory': bool(m.get('mandatory', False))}
                                              for m in members], key=lambda m: m['recipient'])})
    body = json.dumps([version, normalized], sort_keys=True, ensure_ascii=False)
    digest = hashlib.sha256(body.encode()).hexdigest()
    db = connect(path)
    try:
        with db:
            if not db.execute('SELECT 1 FROM manifest WHERE version=?', (version,)).fetchone():
                raise ValueError('unknown_version')
            old = db.execute('SELECT * FROM rollout WHERE id=?', (rollout_id,)).fetchone()
            if old:
                if old['policy_hash'] != digest:
                    raise ValueError('rollout_identity_conflict')
                return digest
            for wave in normalized:
                for member in wave['members']:
                    if not db.execute('SELECT 1 FROM recipient WHERE id=?', (member['recipient'],)).fetchone():
                        raise ValueError('unknown_recipient')
            db.execute("INSERT INTO rollout VALUES (?,?,?,'待启动')", (rollout_id, version, digest))
            for ordinal, wave in enumerate(normalized, 1):
                db.execute("INSERT INTO rollout_wave VALUES (?,?,?,?,'未开放')",
                           (rollout_id, ordinal, wave['name'], wave['quorum']))
                for member in wave['members']:
                    db.execute('INSERT INTO rollout_member VALUES (?,?,?,?,?)',
                               (rollout_id, ordinal, member['recipient'], member['weight'], int(member['mandatory'])))
        return digest
    finally:
        db.close()


def evaluate(path, rollout_id):
    db = connect(path)
    try:
        rollout = db.execute('SELECT * FROM rollout WHERE id=?', (rollout_id,)).fetchone()
        if not rollout:
            raise ValueError('unknown_rollout')
        recipients = [r['recipient_id'] for r in db.execute(
            'SELECT DISTINCT recipient_id FROM rollout_member WHERE rollout_id=?', (rollout_id,)).fetchall()]
        db.close()
        verdicts = {recipient: decide(path, recipient, rollout['version']) for recipient in recipients}
        db = connect(path)
        waves = []
        blocked = False
        with db:
            for wave in db.execute('SELECT * FROM rollout_wave WHERE rollout_id=? ORDER BY ordinal', (rollout_id,)):
                if blocked:
                    status = '未开放'
                    evidence = []
                    db.execute('DELETE FROM rollout_decision WHERE rollout_id=? AND ordinal=?',
                               (rollout_id, wave['ordinal']))
                else:
                    members = list(db.execute('SELECT * FROM rollout_member WHERE rollout_id=? AND ordinal=?',
                                              (rollout_id, wave['ordinal'])))
                    evidence = []
                    for member in members:
                        verdict = verdicts[member['recipient_id']]
                        evidence.append((member, verdict))
                        db.execute('''INSERT INTO rollout_decision VALUES (?,?,?,?,?)
                            ON CONFLICT(rollout_id,ordinal,recipient_id) DO UPDATE SET
                            result=excluded.result,reason=excluded.reason''',
                            (rollout_id, wave['ordinal'], member['recipient_id'],
                             verdict['result'], verdict['reason']))
                    accepted = sum(m['weight'] for m, v in evidence if v['result'] == '已交付')
                    possible = accepted + sum(m['weight'] for m, v in evidence
                                              if v['result'] in ('待核', '待回执'))
                    mandatory_failed = any(m['mandatory'] and v['result'] in ('争议', '拒收', '逾期')
                                           for m, v in evidence)
                    mandatory_pending = any(m['mandatory'] and v['result'] != '已交付'
                                            for m, v in evidence)
                    if mandatory_failed or possible < wave['quorum']:
                        status = '中止'
                    elif accepted >= wave['quorum'] and not mandatory_pending:
                        status = '完成'
                    else:
                        status = '等待'
                db.execute('UPDATE rollout_wave SET status=? WHERE rollout_id=? AND ordinal=?',
                           (status, rollout_id, wave['ordinal']))
                waves.append({'name': wave['name'], 'status': status,
                              'evidence': [{'recipient': m['recipient_id'], 'weight': m['weight'],
                                            'result': v['result']} for m, v in evidence]})
                blocked = status != '完成'
            overall = '中止' if any(w['status'] == '中止' for w in waves) else (
                '已发布' if all(w['status'] == '完成' for w in waves) else '推进中')
            db.execute('UPDATE rollout SET status=? WHERE id=?', (overall, rollout_id))
        return {'status': overall, 'waves': waves}
    finally:
        db.close()
