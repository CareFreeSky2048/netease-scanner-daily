import json

from m3_mode import CONFIRMED_M3_GROUPS, _name_keys, load_m3_artist_names, partition_m3_followed_artists


def test_artist_keys_normalize_case_unicode_width_spacing_and_punctuation():
    m3 = [{"m3_artist_id": "full-name", "name": "Ａlice Smith", "aliases": []},
          {"m3_artist_id": "alias-name", "name": "Other Artist", "aliases": ["ALICE"]}]
    followed = [{"id": 1, "name": " alice   smith "}, {"id": 2, "name": "a-l-i-c-e"}]
    first, second, summary = partition_m3_followed_artists(followed, m3)
    assert [artist["id"] for artist in first] == [1, 2]
    assert not second
    assert summary["strict_matches"] == 1
    assert summary["normalized_matches"] == 1
    assert summary["case_insensitive_matches"] == 1
    assert summary["other_normalized_matches"] == 0
    assert summary["punctuation_matches"] == 1
    assert summary["matches"][0]["match_kind"] == "case_insensitive"
    assert summary["matches"][1]["match_kind"] == "punctuation_normalized"
    assert summary["unmatched"] == 0
    assert _name_keys("Straße") == _name_keys("STRASSE")
    assert _name_keys("Game-i")[1] == _name_keys("gamei")[1]
    assert _name_keys("C++")[1] != _name_keys("C")[1]
    assert _name_keys("AC/DC")[1] != _name_keys("ACDC")[1]


def test_case_only_differences_are_uniquely_matched_and_reported():
    m3 = [{"m3_artist_id": "artist_123", "name": "TeddyLoid", "aliases": []}]
    followed = [{"id": "100", "name": "TEDDYLOID"}]
    first, second, summary = partition_m3_followed_artists(followed, m3)
    assert first == followed and second == []
    assert summary["case_insensitive_matches"] == 1
    assert summary["exact_matches"] == 0
    assert summary["matches"][0]["match_kind"] == "case_insensitive"
    assert summary["matches"][0]["matched_label"] == "TeddyLoid"
    assert summary["matches"][0]["m3_artist_id"] == "artist_123"


def test_ambiguous_m3_and_followed_names_are_left_in_second_round():
    m3 = [
        {"m3_artist_id": "mu", "name": "Maqμin", "aliases": []},
        {"m3_artist_id": "micro", "name": "Maqµin", "aliases": []},
        {"m3_artist_id": "echo", "name": "Echo", "aliases": []},
    ]
    followed = [
        {"id": 10, "name": "Maqμin"},
        {"id": 11, "name": "Echo"},
        {"id": 12, "name": "ECHO"},
        {"id": 13, "name": "Other"},
    ]
    first, second, summary = partition_m3_followed_artists(followed, m3)
    assert first == []
    assert [artist["id"] for artist in second] == [10, 11, 12, 13]
    assert summary["ambiguous"] == 3
    assert summary["unmatched"] == 1
    assert len(summary["ambiguous_matches"]) == 3
    echo = next(item for item in summary["ambiguous_matches"] if item["netease_name"] == "Echo")
    assert echo["netease_candidates"] == [
        {"id": "11", "name": "Echo"}, {"id": "12", "name": "ECHO"}
    ]
    maqin = next(item for item in summary["ambiguous_matches"] if item["netease_id"] == "10")
    assert maqin["m3_candidates"] == ["Maqµin", "Maqμin"]
    assert {item["m3_artist_id"] for item in maqin["m3_candidate_records"]} == {"mu", "micro"}
    assert summary["unmatched_matches"][0]["netease_name"] == "Other"


def test_primary_name_and_alias_matching_different_netease_ids_is_ambiguous():
    m3 = [{"m3_artist_id": "artist_maezawa", "name": "前澤",
           "aliases": ["maezawa"]}]
    followed = [{"id": "test-maezawa-1", "name": "maezawa"},
                {"id": "test-maezawa-2", "name": "前澤"}]
    first, second, summary = partition_m3_followed_artists(followed, m3)

    assert first == []
    assert second == followed
    assert summary["matched"] == 0
    assert summary["ambiguous"] == 2
    assert summary["shared_m3_ambiguities"] == 2
    assert len(summary["ambiguous_matches"]) == 2
    for match in summary["ambiguous_matches"]:
        assert match["m3_candidate_records"] == [
            {"m3_artist_id": "artist_maezawa", "name": "前澤"}
        ]
        assert {candidate["id"] for candidate in match["netease_candidates"]} == {
            "test-maezawa-1", "test-maezawa-2"
        }
        assert "同一 M3 艺人主名或别名对应多个网易云关注艺人 ID" in match["reason"]


def test_user_confirmed_m3_groups_merge_reviewed_ids_but_keep_punctuation_candidates_separate():
    m3 = []
    followed = []
    labels_by_id = {
        "122335233": "ariiol", "35882993": "ariiol",
        "29825674": "Eleanor Forte", "49156846": "Eleanor Forte",
        "49435379": "Eno", "96219908": "Eno",
        "203477": "Kou", "36769981": "kou",
        "20452": "MONO", "51011856": "Mono.",
        "31594335": "Moyu", "59096301": "moyu",
        "49589906": "Rejection", "50126366": "rejection",
        "350154": "Sonance", "95747734": "Sonance",
        "13838571": "Xeno", "51536362": "Xeno",
        "100476334": "XH", "49158992": "XH",
        "12023478": "Zekk", "59961880": "ZEKK",
        "56845690": "Maqμin",
        "95554696": "maezawa", "61491540": "前澤",
    }
    for group in CONFIRMED_M3_GROUPS:
        for label in group["labels"]:
            m3.append({"m3_artist_id": f"{group['id']}-{len(m3)}", "name": label, "aliases": []})
    followed = [{"id": artist_id, "name": name} for artist_id, name in labels_by_id.items()]
    followed.extend([{"id": "29946227", "name": "WATER"},
                     {"id": "53262793", "name": "Kou!"}])

    first, second, summary = partition_m3_followed_artists(followed, m3)

    assert len(first) == 25
    assert len(second) == 2
    assert {artist["name"] for artist in second} == {"WATER", "Kou!"}
    assert len({artist["m3_logical_artist_id"] for artist in first}) == 13
    assert summary["matched"] == 13
    assert summary["matched_accounts"] == 25
    assert summary["confirmed_groups"] == 13
    assert summary["confirmed_group_accounts"] == 25
    assert summary["unmatched"] == 2


def test_m3_name_file_supports_alias_lists(tmp_path):
    path = tmp_path / "artists.json"
    path.write_text(json.dumps({"artists": [{"m3_artist_id": "artist_abc", "name": "Primary", "aliases": ["Alt"]}]},
                               ensure_ascii=False), encoding="utf-8")
    records = load_m3_artist_names(path)
    assert len(records) == 1
    assert records[0]["m3_artist_id"] == "artist_abc"
    assert _name_keys("alt")[0] in {key[0] for key in records[0]["keys"]}
