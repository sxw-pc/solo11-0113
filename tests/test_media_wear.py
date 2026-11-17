from pathlib import Path

import pytest

from chunkproof import engine, custody, media_wear
from chunkproof.store import connect


def known_medium(tmp_path):
    src = tmp_path/'src'; src.mkdir(); (src/'f').write_bytes(b'abcd')
    db = tmp_path/'db.sqlite'
    version = engine.manifest(db, 'delivery', src, 4)
    custody.transfer(db, version, 'medium-A', 1, 'origin', 'vault',
                     '2026-01-01T00:00:00Z')
    return db


def test_register_synthetic_media_wear(tmp_path):
    db = known_medium(tmp_path)
    fixture = Path(__file__).resolve().parents[1]/'fixtures'/'media-wear.json'
    result = media_wear.register(db,'survey-A','medium-A',fixture)
    assert result['sample_count'] == 3
    assert media_wear.register(db,'survey-A','medium-A',fixture) == result
    with connect(db) as con:
        assert con.execute('SELECT count(*) FROM media_wear_survey').fetchone()[0] == 1


def test_reject_unknown_medium_and_changed_survey(tmp_path):
    db = known_medium(tmp_path)
    fixture = Path(__file__).resolve().parents[1]/'fixtures'/'media-wear.json'
    with pytest.raises(ValueError, match='unknown_custody_medium'):
        media_wear.register(db,'survey-B','unknown',fixture)
    media_wear.register(db,'survey-A','medium-A',fixture)
    changed = tmp_path/'changed.json'
    changed.write_text(fixture.read_text().replace('"q10":2','"q10":3'))
    with pytest.raises(ValueError, match='wear_survey_conflict'):
        media_wear.register(db,'survey-A','medium-A',changed)
