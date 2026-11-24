"""M3 artist-name matching for the temporary priority scan mode."""
from __future__ import annotations

import json
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
M3_ARTISTS_FILE = ROOT / "m3_artist_names.json"

# These groups were explicitly confirmed by the user from the 2026-10-26
# matching review. Keep punctuation-only candidates out of this list.
CONFIRMED_M3_GROUPS = [
    {"id": "ariiol", "name": "ariiol", "netease_ids": {"122335233", "35882993"}, "labels": {"ariiol"}},
    {"id": "eleanor-forte", "name": "Eleanor Forte", "netease_ids": {"29825674", "49156846"}, "labels": {"Eleanor Forte"}},
    {"id": "eno", "name": "Eno", "netease_ids": {"49435379", "96219908"}, "labels": {"Eno"}},
    {"id": "kou", "name": "Kou", "netease_ids": {"203477", "36769981"}, "labels": {"Kou", "kou"}},
    {"id": "mono", "name": "Mono.", "netease_ids": {"20452", "51011856"}, "labels": {"MONO", "Mono."}},
    {"id": "moyu", "name": "moyu", "netease_ids": {"31594335", "59096301"}, "labels": {"Moyu", "moyu"}},
    {"id": "rejection", "name": "rejection", "netease_ids": {"49589906", "50126366"}, "labels": {"Rejection", "rejection"}},
    {"id": "sonance", "name": "Sonance", "netease_ids": {"350154", "95747734"}, "labels": {"Sonance"}},
    {"id": "xeno", "name": "Xeno", "netease_ids": {"13838571", "51536362"}, "labels": {"Xeno"}},
    {"id": "xh", "name": "XH", "netease_ids": {"100476334", "49158992"}, "labels": {"XH"}},
    {"id": "zekk", "name": "Zekk", "netease_ids": {"12023478", "59961880"}, "labels": {"Zekk", "ZEKK"}},
    {"id": "maqμin", "name": "Maqμin", "netease_ids": {"56845690"}, "labels": {"Maqμin", "Maqµin"}},
    {"id": "maezawa", "name": "前澤", "netease_ids": {"95554696", "61491540"}, "labels": {"前澤", "maezawa"}},
]


def _name_keys(value: Any) -> tuple[str, str]:
    """Return conservative and separator-insensitive comparison keys.

    NFKC and casefolding handle compatibility forms and capitalization.  The
    loose key is deliberately only a fallback: it ignores whitespace, dashes,
    periods, and underscores. Other punctuation and symbols remain significant
    because characters such as +, #, !, and / can distinguish artist names.
    Invisible format marks are discarded in both keys.
    """
    if not isinstance(value, str):
        return "", ""
    value = "".join(ch for ch in unicodedata.normalize("NFKC", value).casefold()
                    if unicodedata.category(ch) != "Cf").strip()
    strict = " ".join(value.split())
    loose = "".join(ch for ch in value if not ch.isspace() and
                    unicodedata.category(ch) != "Pd" and ch not in "._")
    return strict, loose


def _display_key(value: str) -> str:
    """NFKC-normalized display spelling with normalized internal spacing."""
    return " ".join(unicodedata.normalize("NFKC", value).split())


def load_m3_artist_names(path: Path | str = M3_ARTISTS_FILE) -> list[dict[str, Any]]:
    path = Path(path)
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    records = payload.get("artists") if isinstance(payload, dict) else None
    if not isinstance(records, list) or not records:
        raise ValueError("M3 艺人名单缺失或为空")
    result = []
    for record in records:
        if not isinstance(record, dict):
            continue
        name = record.get("name")
        aliases = record.get("aliases", [])
        if not isinstance(name, str) or not name.strip():
            continue
        if not isinstance(aliases, list):
            aliases = []
        labels = [name, *(alias for alias in aliases if isinstance(alias, str) and alias.strip())]
        m3_artist_id = record.get("m3_artist_id", record.get("id", ""))
        if not isinstance(m3_artist_id, (str, int)):
            m3_artist_id = ""
        result.append({"m3_artist_id": str(m3_artist_id), "name": name.strip(),
                       "aliases": [a.strip() for a in aliases if isinstance(a, str) and a.strip()],
                       "keys": {_name_keys(label) for label in labels if _name_keys(label)[0]}})
    if not result:
        raise ValueError("M3 艺人名单没有有效名称")
    return result


def partition_m3_followed_artists(followed: list[dict[str, Any]],
                                  m3_artists: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    """Split followed artists into M3 matches and the remaining account list.

    Matching is case-insensitive and Unicode-normalized. A conservative
    separator-insensitive key is a fallback only; a key that maps to multiple
    M3 records or followed IDs is treated as ambiguous and left in the second
    round. Different labels for one M3 record that resolve to multiple followed
    IDs are also ambiguous. Fuzzy spelling matches are never auto-applied.
    """
    # Keep the function safe for both records produced by the loader and raw
    # records supplied by tests or callers.  The latter do not yet have keys.
    keyed_records = []
    for record in m3_artists:
        if not isinstance(record, dict):
            continue
        name = record.get("name")
        aliases = record.get("aliases", [])
        if not isinstance(name, str) or not name.strip():
            continue
        if not isinstance(aliases, list):
            aliases = []
        labels = [name, *(alias for alias in aliases if isinstance(alias, str) and alias.strip())]
        keys = record.get("keys")
        if not isinstance(keys, (set, list, tuple)) or not keys:
            keys = {_name_keys(label) for label in labels if _name_keys(label)[0]}
        keyed_records.append({**record, "name": name.strip(), "keys": keys})

    strict_ids: dict[str, set[int]] = defaultdict(set)
    loose_ids: dict[str, set[int]] = defaultdict(set)
    for index, record in enumerate(keyed_records):
        for strict, loose in record.get("keys", ()):
            if strict:
                strict_ids[strict].add(index)
            if loose:
                loose_ids[loose].add(index)

    followed_strict: dict[str, set[str]] = defaultdict(set)
    followed_loose: dict[str, set[str]] = defaultdict(set)
    for artist in followed:
        strict, loose = _name_keys(artist.get("name"))
        artist_id = str(artist.get("id", ""))
        if strict and artist_id:
            followed_strict[strict].add(artist_id)
        if loose and artist_id:
            followed_loose[loose].add(artist_id)

    # Different labels (for example a Japanese display name and its romanized
    # alias) can each resolve cleanly but still point to multiple NetEase
    # accounts for the same M3 record. Treat that cross-alias relationship as
    # ambiguous too; otherwise both accounts would be promoted independently.
    m3_followed_ids: dict[int, set[str]] = defaultdict(set)
    followed_m3_ids: dict[str, set[int]] = defaultdict(set)
    for artist in followed:
        strict, loose = _name_keys(artist.get("name"))
        artist_id = str(artist.get("id", ""))
        if not artist_id:
            continue
        m3_ids = strict_ids.get(strict, set()) if strict else set()
        if not m3_ids and loose:
            m3_ids = loose_ids.get(loose, set())
        for m3_id in m3_ids:
            m3_followed_ids[m3_id].add(artist_id)
            followed_m3_ids[artist_id].add(m3_id)
    shared_m3_conflict_ids = {
        artist_id
        for artist_id, m3_ids in followed_m3_ids.items()
        if any(len(m3_followed_ids[m3_id]) > 1 for m3_id in m3_ids)
    }

    manual_group_by_id = {
        artist_id: group
        for group in CONFIRMED_M3_GROUPS
        for artist_id in group["netease_ids"]
    }
    manual_group_record_indices = {}
    for group in CONFIRMED_M3_GROUPS:
        expected_keys = {_name_keys(label) for label in group["labels"]}
        manual_group_record_indices[group["id"]] = {
            index for index, record in enumerate(keyed_records)
            if any(key in expected_keys for key in record["keys"])
        }

    prioritized = []
    remaining = []
    used_ids = set()
    ambiguous_ids = set()
    strict_count = exact_count = loose_count = 0
    match_kinds: Counter[str] = Counter()
    alias_matches = 0
    matches = []
    ambiguous_matches = []
    unmatched_matches = []
    for artist in followed:
        artist_id = str(artist.get("id", ""))
        strict, loose = _name_keys(artist.get("name"))
        m3_ids = strict_ids.get(strict, set()) if strict else set()
        match_kind = "strict"
        if not m3_ids and loose:
            m3_ids = loose_ids.get(loose, set())
            match_kind = "loose"
        follower_ids = followed_strict.get(strict, set()) if match_kind == "strict" else followed_loose.get(loose, set())
        shared_candidates = set().union(*(m3_followed_ids.get(index, set()) for index in m3_ids)) if m3_ids else set()
        global_m3_conflict = bool(artist_id in shared_m3_conflict_ids)
        confirmed_group = manual_group_by_id.get(artist_id)
        confirmed_indices = (manual_group_record_indices.get(confirmed_group["id"], set())
                            if confirmed_group else set())
        label_keys = {_name_keys(label) for label in confirmed_group["labels"]} if confirmed_group else set()
        compatible_name = any(key and (strict == key[0] or loose == key[1]) for key in label_keys)
        if confirmed_group and confirmed_indices.intersection(m3_ids) and compatible_name:
            prioritized.append({**artist, "m3_logical_artist": confirmed_group["name"],
                                "m3_logical_artist_id": confirmed_group["id"]})
            used_ids.add(artist_id)
            candidate_records = sorted(
                ({"m3_artist_id": str(keyed_records[index].get("m3_artist_id", "")),
                  "name": keyed_records[index]["name"]}
                 for index in confirmed_indices.intersection(m3_ids)),
                key=lambda candidate: (candidate["name"].casefold(), candidate["m3_artist_id"]),
            )
            matches.append({
                "netease_id": artist_id,
                "netease_name": str(artist.get("name", "")),
                "m3_artist_id": ",".join(item["m3_artist_id"] for item in candidate_records),
                "m3_name": confirmed_group["name"],
                "matched_label": str(artist.get("name", "")),
                "match_source": "user_confirmed_group",
                "match_kind": "user_confirmed_group",
                "logical_artist_id": confirmed_group["id"],
                "logical_artist_name": confirmed_group["name"],
                "m3_candidates": candidate_records,
            })
            match_kinds["user_confirmed_group"] += 1
            continue
        if m3_ids:
            if (len(m3_ids) == 1 and len(follower_ids) == 1
                    and artist_id not in used_ids and not global_m3_conflict):
                prioritized.append(artist)
                used_ids.add(artist_id)
                record = keyed_records[next(iter(m3_ids))]
                labels = [(record["name"], "primary"),
                          *((label, "alias") for label in record.get("aliases", []))]
                matching_labels = [
                    (label, source) for label, source in labels
                    if _name_keys(label)[0 if match_kind == "strict" else 1] ==
                    (strict if match_kind == "strict" else loose)
                ]
                # Prefer a spelling that is literally the followed name. This
                # makes alias use and case-only matches auditable in the report.
                matched_label, matched_source = next(
                    ((label, source) for label, source in matching_labels
                     if label == str(artist.get("name", ""))),
                    matching_labels[0] if matching_labels else (record["name"], "primary"),
                )
                if match_kind == "loose":
                    resolved_kind = "punctuation_normalized"
                    loose_count += 1
                else:
                    strict_count += 1
                    if matched_label == str(artist.get("name", "")):
                        resolved_kind = "exact"
                        exact_count += 1
                    else:
                        followed_display = _display_key(str(artist.get("name", "")))
                        matched_display = _display_key(matched_label)
                        if followed_display.casefold() == matched_display.casefold() and followed_display != matched_display:
                            resolved_kind = "case_insensitive"
                        else:
                            resolved_kind = "normalized"
                match_kinds[resolved_kind] += 1
                alias_matches += matched_source == "alias"
                matches.append({"netease_id": artist_id, "netease_name": str(artist.get("name", "")),
                                "m3_artist_id": str(record.get("m3_artist_id", "")),
                                "m3_name": record["name"], "matched_label": matched_label,
                                "match_source": matched_source, "match_kind": resolved_kind})
                continue
            ambiguous_ids.add(artist_id)
            candidate_ids = set(follower_ids) | shared_candidates
            followed_candidates = sorted(
                ({"id": str(candidate.get("id", "")),
                  "name": str(candidate.get("name", ""))}
                 for candidate in followed
                 if str(candidate.get("id", "")) in candidate_ids),
                key=lambda candidate: (candidate["name"].casefold(), candidate["id"]),
            )
            reasons = []
            if len(m3_ids) != 1:
                reasons.append("M3 名称或别名存在多个候选")
            if len(follower_ids) != 1:
                reasons.append("网易云关注列表中多个艺人 ID 共享该名称")
            if global_m3_conflict:
                reasons.append("同一 M3 艺人主名或别名对应多个网易云关注艺人 ID")
            if artist_id in used_ids:
                reasons.append("同一网易云艺人 ID 已在前一条记录中出现")
            candidate_records = sorted(
                ({"m3_artist_id": str(keyed_records[index].get("m3_artist_id", "")),
                  "name": keyed_records[index]["name"]} for index in m3_ids),
                key=lambda candidate: (candidate["name"].casefold(), candidate["m3_artist_id"]),
            )
            ambiguous_matches.append({
                "netease_id": artist_id,
                "netease_name": str(artist.get("name", "")),
                "m3_candidates": [candidate["name"] for candidate in candidate_records],
                "m3_candidate_records": candidate_records,
                "netease_candidates": followed_candidates,
                "reason": "；".join(reasons),
            })
        else:
            unmatched_matches.append({
                "netease_id": artist_id,
                "netease_name": str(artist.get("name", "")),
                "reason": "M3 名称及别名中没有对应项",
            })
        remaining.append(artist)

    logical_matches = {match.get("logical_artist_id") or match.get("m3_artist_id")
                       for match in matches}
    return prioritized, remaining, {
        "followed": len(followed),
        "matched": len(logical_matches),
        "matched_accounts": len(prioritized),
        "confirmed_group_accounts": sum(match.get("match_kind") == "user_confirmed_group"
                                         for match in matches),
        "confirmed_groups": len({match["logical_artist_id"] for match in matches
                                  if match.get("logical_artist_id")}),
        "strict_matches": strict_count,
        "normalized_matches": loose_count,
        "exact_matches": match_kinds["exact"],
        "case_insensitive_matches": match_kinds["case_insensitive"],
        "other_normalized_matches": match_kinds["normalized"],
        "punctuation_matches": match_kinds["punctuation_normalized"],
        "alias_matches": alias_matches,
        "remaining": len(remaining),
        "ambiguous": len(ambiguous_ids),
        "shared_m3_ambiguities": len(shared_m3_conflict_ids),
        "unmatched": len(unmatched_matches),
        "m3_artist_records": len(keyed_records),
        "matches": matches,
        "ambiguous_matches": ambiguous_matches,
        "unmatched_matches": unmatched_matches,
    }
