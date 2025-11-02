#!/usr/bin/env python3
"""
add_wikidata_qids.py

Usage:
  python add_wikidata_qids.py --in taxonomy_scored_full.csv --out_csv taxonomy_scored_full_wikidata.csv --out_json taxonomy_scored_full_wikidata.json --threshold 0.80

This script:
 - reads CSV rows
 - for each row with wiki_qid == 'TBD' or empty, attempts to find Wikidata candidates
 - computes a simple confidence score for candidates using label match, geo/time match and description
 - writes final CSV and JSON files with wiki_qid, wiki_qid_confidence, wiki_qid_candidates, wiki_description, wikidata_aliases
"""

import argparse
import csv
import json
import requests
import time
import urllib.parse

WIKIDATA_SEARCH = "https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json&language=en&limit=10&search={}"
WIKIDATA_ENTITY = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"
WIKIPEDIA_OPENSEARCH = "https://en.wikipedia.org/w/api.php?action=opensearch&format=json&search={}&limit=5"

def safe_get(url, params=None, retries=3, backoff=0.5):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20, headers={"User-Agent":"TaxonomyBot/1.0 (contact: you@example.com)"})
            if r.status_code == 200:
                return r.json()
            else:
                time.sleep(backoff * (attempt+1))
        except Exception as e:
            time.sleep(backoff * (attempt+1))
    return None

def search_wikidata(label):
    q = urllib.parse.quote(label)
    url = WIKIDATA_SEARCH.format(q)
    return safe_get(url)

def get_entity_data(qid):
    url = WIKIDATA_ENTITY.format(qid)
    return safe_get(url)

def score_candidate(candidate, row):
    """
    candidate: dict from wbsearchentities (contains 'id', 'label', 'description', 'match' etc.)
    row: csv row dict with 'geo_path','time_path','category','word'
    Returns: float confidence 0..1
    """
    score = 0.0
    # label exactness
    label = (candidate.get('label') or "").lower()
    term = (row.get('wiki_label') or row.get('word') or "").lower()
    if label == term:
        score += 0.45
    elif term in label or label in term:
        score += 0.30
    else:
        # fuzzy: partial tokens
        term_tokens = set(term.split())
        label_tokens = set(label.split())
        if len(term_tokens & label_tokens) > 0:
            score += 0.15

    # description match: if Wikidata description contains geo/time/category tokens
    desc = (candidate.get('description') or "").lower()
    if desc:
        for token in (row.get('geo_path','') + " " + row.get('time_path','') + " " + row.get('category','') ).lower().split():
            if token and token in desc:
                score += 0.05
                # cap small additions

    # presence of English Wikipedia sitelink (favors canonical things)
    ent = get_entity_data(candidate['id'])
    if ent:
        entities = ent.get('entities',{})
        e = entities.get(candidate['id'],{})
        sitelinks = e.get('sitelinks',{})
        if 'enwiki' in sitelinks:
            score += 0.20

    # normalize
    if score > 1.0:
        score = 1.0
    return score

def find_candidates_for_row(row, threshold=0.8):
    """Return list of candidates with confidence >= threshold and best candidate"""
    label = row.get('wiki_label') or row.get('word')
    if not label:
        return []
    res = search_wikidata(label)
    if not res:
        return []
    search_hits = res.get('search', [])
    scored = []
    for c in search_hits:
        conf = score_candidate(c, row)
        scored.append((c['id'], c.get('label'), c.get('description'), conf))
    # sort by confidence desc
    scored.sort(key=lambda x: x[3], reverse=True)
    candidates = [ {"qid": qid, "label":lbl, "description":desc, "confidence":conf} for (qid,lbl,desc,conf) in scored if conf >= threshold ]
    return candidates, scored

def main(infile, out_csv, out_json, threshold=0.8):
    rows = []
    with open(infile, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    results = []
    for idx, r in enumerate(rows, start=1):
        print(f"[{idx}/{len(rows)}] Processing: {r.get('word')}")
        if (r.get('wiki_qid') and r.get('wiki_qid') != 'TBD') or (r.get('wiki_label') is None and r.get('word')==''):
            # skip if already filled or nothing to search
            results.append(r)
            continue
        candidates, scored = find_candidates_for_row(r, threshold=threshold)
        # attach results
        r_out = dict(r)  # copy
        if candidates:
            r_out['wiki_qid_candidates'] = json.dumps(candidates, ensure_ascii=False)
            r_out['wiki_qid'] = candidates[0]['qid']
            r_out['wiki_qid_confidence'] = candidates[0]['confidence']
            # also include second best if > threshold
        else:
            # fallback: record top three scored candidates regardless of threshold
            top = scored[:3]
            r_out['wiki_qid_candidates'] = json.dumps([{"qid":qid,"label":lbl,"desc":desc,"raw_conf":conf} for (qid,lbl,desc,conf) in top], ensure_ascii=False)
            r_out['wiki_qid'] = ''
            r_out['wiki_qid_confidence'] = ''
        results.append(r_out)
        time.sleep(0.2)  # polite pacing
    # write outputs
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        fieldnames = list(results[0].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("Done. Files written:", out_csv, out_json)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--in', dest='infile', required=True)
    ap.add_argument('--out_csv', default='taxonomy_scored_full_wikidata.csv')
    ap.add_argument('--out_json', default='taxonomy_scored_full_wikidata.json')
    ap.add_argument('--threshold', type=float, default=0.8)
    args = ap.parse_args()
    main(args.infile, args.out_csv, args.out_json, threshold=args.threshold)
