from db.draftdb import (
    create_draft_test_session_from_pack_pool,
    ensure_draft_testing_schema,
    get_draft_test_detail,
    move_human_draft_test_pick_zone,
    normalize_draft_test_packs_per_player,
    normalize_draft_test_pod_size,
    record_human_draft_test_pick,
)


def create_draft_test_from_pack_pool(
    campaign_id=None,
    tracked_pack_ids=None,
    pod_size=8,
    packs_per_player=3,
    human_player_name="You",
    human_player_id=None,
    human_portrait_image_path="",
):
    return create_draft_test_session_from_pack_pool(
        campaign_id=campaign_id,
        tracked_pack_ids=tracked_pack_ids or [],
        pod_size=pod_size,
        packs_per_player=packs_per_player,
        human_player_name=human_player_name,
        human_player_id=human_player_id,
        human_portrait_image_path=human_portrait_image_path,
    )


def get_draft_test_detail_state(draft_test_id):
    ensure_draft_testing_schema()
    return get_draft_test_detail(draft_test_id)


def record_human_draft_test_pick_state(draft_test_id, draft_test_pack_card_id, deck_zone="deck"):
    ensure_draft_testing_schema()
    return record_human_draft_test_pick(
        draft_test_id=draft_test_id,
        draft_test_pack_card_id=draft_test_pack_card_id,
        deck_zone=deck_zone,
    )



def move_human_draft_test_pick_zone_state(draft_test_id, draft_test_pick_id, deck_zone):
    ensure_draft_testing_schema()
    return move_human_draft_test_pick_zone(
        draft_test_id=draft_test_id,
        draft_test_pick_id=draft_test_pick_id,
        deck_zone=deck_zone,
    )


def normalize_draft_test_start_payload(form_data):
    return {
        "pod_size": normalize_draft_test_pod_size(form_data.get("pod_size")),
        "packs_per_player": normalize_draft_test_packs_per_player(form_data.get("packs_per_player")),
        "human_player_name": (form_data.get("human_player_name") or "You").strip() or "You",
    }