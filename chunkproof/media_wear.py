"""Freeze synthetic media telemetry under a custody identity."""
import hashlib
import json
from pathlib import Path

from .custody import instant
from .store import connect


def register(path, survey_id, medium, input_file):
    if not survey_id or not medium:
        raise ValueError('missing_wear_identity')
    input_file = Path(input_file).resolve()
    raw = input_file.read_bytes()
    doc = json.loads(raw)
    samples = doc.get('samples', [])
    if (len(samples) < 2 or doc.get('wear_limit_hours', 0) <= 0 or
            doc.get('counter_modulus', 0) < 2 or doc.get('q10', 0) <= 1 or
            doc.get('error_penalty_hours', -1) < 0):
        raise ValueError('bad_wear_contract')
    previous = None
    for sample in samples:
        moment = instant(sample['at'])
        if (previous and moment <= previous or
                not -40 <= sample['temperature_c'] <= 100 or
                not 0 <= sample['error_counter'] < doc['counter_modulus'] or
                not sample['boot_id']):
            raise ValueError('bad_media_sample')
        previous = moment
    db = connect(path)
    try:
        with db:
            if not db.execute('SELECT 1 FROM custody_event WHERE medium=?',
                              (medium,)).fetchone():
                raise ValueError('unknown_custody_medium')
            digest = hashlib.sha256(raw).hexdigest()
            payload = json.dumps({'medium':medium,'file':str(input_file),
                                  'digest':digest,'doc':doc},
                                 sort_keys=True,separators=(',',':'))
            old = db.execute('SELECT payload FROM media_wear_survey WHERE id=?',
                             (survey_id,)).fetchone()
            if old and old['payload'] != payload:
                raise ValueError('wear_survey_conflict')
            db.execute('INSERT OR IGNORE INTO media_wear_survey VALUES (?,?)',
                       (survey_id,payload))
    finally:
        db.close()
    return {'survey_id':survey_id,'medium':medium,'digest':digest,
            'sample_count':len(samples)}
