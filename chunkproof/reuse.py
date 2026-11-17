"""跨交付批次的本地块复用计划。

两种相互独立、由调用方显式选择的业务合同：

* ``strict_provenance`` —— 只接纳与新交付同介质保管链、同收件范围，且在新版本
  下仍有回执的块。本质上不产生跨版本借用，缺块结论保持“仍缺块”，其证据范围仅
  包含新版本回执与当前保管链。
* ``content_reuse`` —— 允许借用旧交付中逐字节同长同哈希的块补齐内容，但旧版本
  介质来源与保管状态作为独立证据记录，绝不生成新版本回执；内容等同不被当作
  保管链等同。

计划按新清单的文件路径、半开区间、长度、摘要查找旧块候选；旧块来源字节已变、
旧版封验不可发布、来源介质在交接后被隔离、或属于另一收件范围的候选一律排除。
dry-run 计划只读台账与来源文件，不写入任何数据；``freeze_plan`` 才把计划连同
新旧 manifest 版本、候选回执与介质链证据冻结入库。
"""
import json
from pathlib import Path

from .engine import canonical, sha
from .store import connect

STRICT = 'strict_provenance'
CONTENT = 'content_reuse'
CONTRACTS = (STRICT, CONTENT)


def recipient_scope(db, version):
    """版本的收件范围：该版本各发布波次中登记的收件人并集。"""
    rows = db.execute(
        'SELECT DISTINCT m.recipient_id FROM rollout_member m JOIN rollout r ON r.id=m.rollout_id '
        'WHERE r.version=? ORDER BY m.recipient_id', (version,))
    return [r['recipient_id'] for r in rows]


def medium_snapshot(db, medium):
    return db.execute(
        'SELECT medium,seq,from_holder,to_holder,at,version,status FROM custody_event '
        'WHERE medium=? ORDER BY seq DESC LIMIT 1', (medium,)).fetchone()


def reuse_plan(db_path, version, contract, recipients, old_versions=None):
    """生成只读复用计划。不写库、不改写任何旧 manifest、回执或原始块文件。"""
    if contract not in CONTRACTS:
        raise ValueError('unknown_contract')
    recipients = sorted(set(recipients))
    if not recipients:
        raise ValueError('empty_recipient_scope')
    db = connect(db_path)
    try:
        current = db.execute('SELECT version,delivery_id,body_hash FROM manifest WHERE version=?',
                             (version,)).fetchone()
        if not current:
            raise ValueError('unknown_version')
        known = {r['id'] for r in db.execute('SELECT id FROM recipient')}
        unknown = [r for r in recipients if r not in known]
        if unknown:
            raise ValueError('unknown_recipient')
        all_versions = [r['version'] for r in db.execute('SELECT version FROM manifest ORDER BY created,version')]
        if old_versions is None:
            old_versions = [v for v in all_versions if v != version]
        else:
            old_versions = list(dict.fromkeys(old_versions))
            bad = [v for v in old_versions
                   if v == version or not db.execute(
                       'SELECT 1 FROM manifest WHERE version=?', (v,)).fetchone()]
            if bad:
                raise ValueError('unknown_old_version')
        new_scope = set(recipients)
        new_chunks = [dict(c) for c in db.execute(
            'SELECT c.id AS chunk_id,f.path,c.idx,c.start,c.end,c.sha256,c.end-c.start AS size '
            'FROM chunk c JOIN file_entry f ON f.id=c.file_id WHERE f.version=? ORDER BY f.path,c.idx',
            (version,))]
        items = []
        reusable_count = 0
        for c in new_chunks:
            key = f"{c['path']}:{c['idx']}"
            new_receipts = [dict(r) for r in db.execute(
                "SELECT r.id,r.medium,r.path,r.status,a.root FROM receipt r JOIN attempt a ON a.id=r.attempt_id "
                "JOIN chunk ch ON ch.id=r.chunk_id WHERE ch.id=? AND r.status='verified' ORDER BY r.id",
                (c['chunk_id'],))]
            entry = {'path': c['path'], 'idx': c['idx'], 'start': c['start'], 'end': c['end'],
                     'length': c['size'], 'sha256': c['sha256'], 'key': key,
                     'new_version_receipts': [r['id'] for r in new_receipts],
                     'candidates': [], 'eligible': None, 'excluded': [],
                     'conclusion': 'reusable' if new_receipts else 'still_missing'}
            if new_receipts:
                entry['basis'] = 'new_version_receipt'
            else:
                entry['basis'] = None
            # 严格保管链合同：只看新版本自身的验真回执与当前保管链。
            if contract == STRICT:
                if not new_receipts:
                    entry['excluded'].append({'code': 'no_new_version_receipt',
                                              'detail': 'strict_provenance 不接纳跨版本借用'})
                items.append(entry)
                if entry['conclusion'] == 'reusable':
                    reusable_count += 1
                continue
            # 内容复用合同：跨版本逐字节同长同哈希查找候选。
            for ov in old_versions:
                old = db.execute(
                    'SELECT c.id AS old_chunk_id,c.sha256,c.end-c.start AS size,f.path AS old_path,'
                    'c.start AS old_start,c.end AS old_end FROM chunk c JOIN file_entry f ON f.id=c.file_id '
                    'WHERE f.version=? AND f.path=? AND c.start=? AND c.end=? AND c.sha256=?',
                    (ov, c['path'], c['start'], c['end'], c['sha256'])).fetchone()
                if not old:
                    entry['excluded'].append({'code': 'no_matching_old_chunk', 'version': ov})
                    continue
                old_receipt = db.execute(
                    "SELECT r.id,r.medium,r.path,a.root FROM receipt r JOIN attempt a ON a.id=r.attempt_id "
                    "WHERE r.chunk_id=? AND r.status='verified' ORDER BY r.id LIMIT 1",
                    (old['old_chunk_id'],)).fetchone()
                if not old_receipt:
                    entry['excluded'].append({'code': 'no_verified_old_receipt', 'version': ov})
                    continue
                reasons = []
                old_scope = set(recipient_scope(db, ov))
                if not old_scope:
                    reasons.append({'code': 'old_scope_unknown', 'detail': '旧版本没有冻结的收件范围'})
                elif old_scope != new_scope:
                    reasons.append({'code': 'recipient_scope_mismatch',
                                    'detail': {'old': sorted(old_scope), 'new': sorted(new_scope)}})
                old_decision = db.execute('SELECT result FROM decision WHERE version=?', (ov,)).fetchone()
                if not old_decision or old_decision['result'] != '可发布':
                    reasons.append({'code': 'old_seal_not_publishable',
                                    'detail': old_decision['result'] if old_decision else 'unsealed'})
                custody_row = medium_snapshot(db, old_receipt['medium'])
                if not custody_row:
                    reasons.append({'code': 'custody_chain_missing', 'medium': old_receipt['medium']})
                elif custody_row['status'] != 'trusted':
                    reasons.append({'code': 'medium_quarantined', 'medium': old_receipt['medium'],
                                    'detail': dict(custody_row)})
                source = (Path(old_receipt['root']) / old_receipt['path']).resolve()
                if not source.is_relative_to(Path(old_receipt['root']).resolve()):
                    raise ValueError('unsafe_path')
                data = source.read_bytes() if source.is_file() else None
                if data is None:
                    reasons.append({'code': 'source_missing', 'detail': str(source)})
                elif sha(data) != c['sha256'] or len(data) != c['size']:
                    reasons.append({'code': 'source_bytes_changed',
                                    'detail': {'actual_sha256': sha(data) if data is not None else None,
                                               'actual_size': len(data) if data is not None else None}})
                candidate = {'old_version': ov, 'old_chunk_id': old['old_chunk_id'],
                             'old_receipt_id': old_receipt['id'], 'medium': old_receipt['medium'],
                             'source_root': old_receipt['root'], 'source_path': old_receipt['path'],
                             'sha256': c['sha256'], 'size': c['size'],
                             'custody': dict(custody_row) if custody_row else None,
                             'recipient_scope': sorted(old_scope),
                             'old_seal': old_decision['result'] if old_decision else None,
                             'eligible': not reasons, 'excluded': reasons}
                entry['candidates'].append(candidate)
                if reasons:
                    entry['excluded'].extend([{**r, 'version': ov, 'medium': old_receipt['medium']}
                                              for r in reasons])
            good = next((x for x in entry['candidates'] if x['eligible']), None)
            if good:
                entry['eligible'] = good
                entry['conclusion'] = 'reusable'
                entry['basis'] = 'content_reuse_old_delivery'
                reusable_count += 1
            else:
                entry['conclusion'] = 'still_missing'
            items.append(entry)
        still_missing = [x['key'] for x in items if x['conclusion'] == 'still_missing']
        frozen = {
            'new_manifest': {'version': current['version'], 'delivery_id': current['delivery_id'],
                             'body_hash': current['body_hash']},
            'old_manifests': [dict(r) for r in db.execute(
                'SELECT version,delivery_id,body_hash FROM manifest WHERE version IN (%s)'
                % ','.join('?' * len(old_versions)), old_versions)] if old_versions else [],
            'candidate_receipts': sorted({cand['old_receipt_id'] for it in items for cand in it['candidates']}),
            'custody_chain': [dict(r) for r in db.execute(
                'SELECT medium,seq,from_holder,to_holder,at,version,status FROM custody_event '
                'WHERE medium IN (SELECT DISTINCT medium FROM receipt WHERE id IN (%s)) ORDER BY medium,seq'
                % ','.join('?' * len({cand['old_receipt_id'] for it in items for cand in it['candidates']})),
                tuple(sorted({cand['old_receipt_id'] for it in items for cand in it['candidates']})))
            ] if {cand['old_receipt_id'] for it in items for cand in it['candidates']} else [],
        }
        plan_id = sha(canonical({'version': version, 'contract': contract, 'recipients': recipients,
                                 'old_versions': old_versions, 'frozen': frozen,
                                 'items': [{'key': x['key'], 'conclusion': x['conclusion'],
                                            'eligible': ({'old_version': x['eligible']['old_version'],
                                                          'old_receipt_id': x['eligible']['old_receipt_id'],
                                                          'medium': x['eligible']['medium']}
                                                         if x['eligible'] else None),
                                            'excluded': x['excluded']} for x in items]}))
        return {
            'plan_id': plan_id, 'version': version, 'contract': contract,
            'recipients': recipients, 'old_versions': old_versions,
            'missing': [x['key'] for x in items if not x['new_version_receipts']],
            'still_missing': still_missing, 'reusable': reusable_count,
            'evidence_scope': {
                STRICT: '仅新版本已验真回执与对应介质当前保管链；不引用任何旧交付证据',
                CONTENT: '新版本已验真回执，加上旧版本封验结论、旧回执、来源字节复验与旧介质保管链；'
                         '内容等同不构成保管链等同',
            }[contract],
            'items': items, 'frozen': frozen,
        }
    finally:
        db.close()


def freeze_plan(db_path, plan):
    """把 dry-run 计划冻结入库（幂等）。返回 plan_id。"""
    db = connect(db_path)
    try:
        with db:
            if not db.execute('SELECT 1 FROM manifest WHERE version=?', (plan['version'],)).fetchone():
                raise ValueError('unknown_version')
            db.execute('INSERT OR IGNORE INTO reuse_plan(id,version,contract,recipients,old_versions,frozen) '
                       'VALUES (?,?,?,?,?,?)',
                       (plan['plan_id'], plan['version'], plan['contract'],
                        json.dumps(plan['recipients'], sort_keys=True),
                        json.dumps(plan['old_versions']),
                        json.dumps(plan['frozen'], sort_keys=True, ensure_ascii=False)))
        return plan['plan_id']
    finally:
        db.close()


def load_plan(db_path, plan_id):
    db = connect(db_path)
    try:
        row = db.execute('SELECT * FROM reuse_plan WHERE id=?', (plan_id,)).fetchone()
        if not row:
            raise ValueError('unknown_reuse_plan')
        return dict(row)
    finally:
        db.close()


def read_source(root, rel):
    root = Path(root).resolve()
    source = (root / rel).resolve()
    if not source.is_relative_to(root):
        raise ValueError('unsafe_path')
    if not source.is_file():
        return None
    return source.read_bytes()


def proof_current(db, proof):
    """复用证明在当前时刻是否仍然成立：来源字节未变且旧介质未被隔离。"""
    data = read_source(proof['source_root'], proof['source_path'])
    if data is None or sha(data) != proof['sha256'] or len(data) != proof['size']:
        return False, 'source_bytes_changed'
    snap = medium_snapshot(db, proof['source_medium'])
    if not snap or snap['status'] != 'trusted':
        return False, 'medium_quarantined'
    return True, None
