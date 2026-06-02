import random
from datetime import datetime, timezone

from db.database import get_db_connection
from modes.bot_selection import choose_bot_draft_pick


DRAFT_TEST_COLOR_PAIRS = [
    ("W", "U"),
    ("U", "B"),
    ("B", "R"),
    ("R", "G"),
    ("G", "W"),
    ("W", "B"),
    ("U", "R"),
    ("B", "G"),
    ("R", "W"),
    ("G", "U"),
]

DRAFT_TEST_BASIC_LANDS = [
    "Plains",
    "Island",
    "Swamp",
    "Mountain",
    "Forest",
    "Wastes",
]


def normalize_draft_test_basic_land_name(value):
    clean_value = str(value or "").strip()

    for land_name in DRAFT_TEST_BASIC_LANDS:
        if clean_value.lower() == land_name.lower():
            return land_name

    return ""


def draft_test_utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def draft_test_normalize_optional_int(value):
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def ensure_draft_testing_schema():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS draft_test_sessions (
            draft_test_id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER NULL,
            draft_name TEXT,
            pod_size INTEGER NOT NULL DEFAULT 8,
            packs_per_player INTEGER NOT NULL DEFAULT 3,
            status TEXT NOT NULL DEFAULT 'setup',
            current_pack_number INTEGER NOT NULL DEFAULT 1,
            current_pick_number INTEGER NOT NULL DEFAULT 1,
            current_human_pack_id INTEGER NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS draft_test_players (
            draft_test_player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_test_id INTEGER NOT NULL,
            seat_index INTEGER NOT NULL,
            display_name TEXT NOT NULL,
            is_human INTEGER NOT NULL DEFAULT 0,
            campaign_player_id INTEGER,
            portrait_image_path TEXT,
            color_preference_1 TEXT,
            color_preference_2 TEXT,
            color_tracking_white INTEGER NOT NULL DEFAULT 0,
            color_tracking_blue INTEGER NOT NULL DEFAULT 0,
            color_tracking_black INTEGER NOT NULL DEFAULT 0,
            color_tracking_red INTEGER NOT NULL DEFAULT 0,
            color_tracking_green INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            UNIQUE(draft_test_id, seat_index),
            FOREIGN KEY (draft_test_id) REFERENCES draft_test_sessions (draft_test_id)
        )
        """
    )

    cursor.execute("PRAGMA table_info(draft_test_players)")
    draft_test_player_columns = {
        row["name"]
        for row in cursor.fetchall()
    }

    if "campaign_player_id" not in draft_test_player_columns:
        cursor.execute(
            """
            ALTER TABLE draft_test_players
            ADD COLUMN campaign_player_id INTEGER
            """
        )

    if "portrait_image_path" not in draft_test_player_columns:
        cursor.execute(
            """
            ALTER TABLE draft_test_players
            ADD COLUMN portrait_image_path TEXT
            """
        )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS draft_test_pack_pool (
            draft_test_pack_pool_id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_test_id INTEGER NOT NULL,
            tracked_pack_id INTEGER NOT NULL,
            pool_order INTEGER NOT NULL,
            is_used INTEGER NOT NULL DEFAULT 0,
            assigned_pack_number INTEGER,
            assigned_seat_index INTEGER,
            created_at_utc TEXT NOT NULL,
            FOREIGN KEY (draft_test_id) REFERENCES draft_test_sessions (draft_test_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS draft_test_packs (
            draft_test_pack_id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_test_id INTEGER NOT NULL,
            pack_number INTEGER NOT NULL,
            original_seat_index INTEGER NOT NULL,
            current_seat_index INTEGER NOT NULL,
            pass_number INTEGER NOT NULL DEFAULT 1,
            tracked_pack_id INTEGER,
            pack_tracking_code TEXT,
            pack_display_name TEXT,
            set_code TEXT,
            booster_name TEXT,
            booster_index INTEGER,
            is_complete INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT,
            FOREIGN KEY (draft_test_id) REFERENCES draft_test_sessions (draft_test_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS draft_test_pack_cards (
            draft_test_pack_card_id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_test_pack_id INTEGER NOT NULL,
            draft_test_id INTEGER NOT NULL,
            card_order INTEGER NOT NULL,
            card_uuid TEXT NOT NULL,
            card_name TEXT NOT NULL,
            set_code TEXT,
            collector_number TEXT,
            rarity TEXT,
            type_line TEXT,
            mana_value REAL,
            mana_cost TEXT,
            colors_json TEXT,
            color_identity_json TEXT,
            edhrec_rank INTEGER,
            edhrec_saltiness REAL,
            image_url TEXT,
            is_foil INTEGER NOT NULL DEFAULT 0,
            is_picked INTEGER NOT NULL DEFAULT 0,
            picked_by_seat_index INTEGER,
            picked_at_pick_number INTEGER,
            picked_at_utc TEXT,
            FOREIGN KEY (draft_test_pack_id) REFERENCES draft_test_packs (draft_test_pack_id),
            FOREIGN KEY (draft_test_id) REFERENCES draft_test_sessions (draft_test_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS draft_test_picks (
            draft_test_pick_id INTEGER PRIMARY KEY AUTOINCREMENT,
            draft_test_id INTEGER NOT NULL,
            draft_test_player_id INTEGER NOT NULL,
            draft_test_pack_id INTEGER NOT NULL,
            draft_test_pack_card_id INTEGER NOT NULL,
            seat_index INTEGER NOT NULL,
            pack_number INTEGER NOT NULL,
            pick_number INTEGER NOT NULL,
            card_uuid TEXT NOT NULL,
            card_name TEXT NOT NULL,
            deck_zone TEXT NOT NULL DEFAULT 'deck',
            picked_at_utc TEXT NOT NULL,
            pick_score REAL,
            pick_reason TEXT,
            FOREIGN KEY (draft_test_id) REFERENCES draft_test_sessions (draft_test_id),
            FOREIGN KEY (draft_test_player_id) REFERENCES draft_test_players (draft_test_player_id),
            FOREIGN KEY (draft_test_pack_id) REFERENCES draft_test_packs (draft_test_pack_id),
            FOREIGN KEY (draft_test_pack_card_id) REFERENCES draft_test_pack_cards (draft_test_pack_card_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_draft_test_sessions_campaign_status
        ON draft_test_sessions (campaign_id, status, created_at_utc)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_draft_test_players_session_seat
        ON draft_test_players (draft_test_id, seat_index)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_draft_test_packs_session_holder
        ON draft_test_packs (draft_test_id, pack_number, current_seat_index, is_complete)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_draft_test_pack_cards_pack_available
        ON draft_test_pack_cards (draft_test_pack_id, is_picked, card_order)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_draft_test_picks_session_player
        ON draft_test_picks (draft_test_id, seat_index, pick_number)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_draft_test_pack_pool_session_order
        ON draft_test_pack_pool (draft_test_id, pool_order, is_used)
        """
    )

    conn.commit()
    conn.close()


def normalize_draft_test_pod_size(value):
    try:
        pod_size = int(value)
    except (TypeError, ValueError):
        pod_size = 8

    if pod_size < 2:
        pod_size = 2

    if pod_size > 12:
        pod_size = 12

    return pod_size


def normalize_draft_test_packs_per_player(value):
    try:
        packs_per_player = int(value)
    except (TypeError, ValueError):
        packs_per_player = 3

    if packs_per_player < 1:
        packs_per_player = 1

    if packs_per_player > 9:
        packs_per_player = 9

    return packs_per_player


def deactivate_existing_draft_tests(campaign_id=None):
    ensure_draft_testing_schema()

    parsed_campaign_id = draft_test_normalize_optional_int(campaign_id)
    now_utc = draft_test_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    if parsed_campaign_id is None:
        cursor.execute(
            """
            UPDATE draft_test_sessions
            SET status = 'abandoned',
                updated_at_utc = ?
            WHERE campaign_id IS NULL
              AND status IN ('setup', 'drafting')
            """,
            (now_utc,),
        )
    else:
        cursor.execute(
            """
            UPDATE draft_test_sessions
            SET status = 'abandoned',
                updated_at_utc = ?
            WHERE campaign_id = ?
              AND status IN ('setup', 'drafting')
            """,
            (
                now_utc,
                parsed_campaign_id,
            ),
        )

    conn.commit()
    conn.close()


def create_draft_test_session(campaign_id=None, pod_size=8, packs_per_player=3, human_player_name="You"):
    ensure_draft_testing_schema()

    parsed_campaign_id = draft_test_normalize_optional_int(campaign_id)
    parsed_pod_size = normalize_draft_test_pod_size(pod_size)
    parsed_packs_per_player = normalize_draft_test_packs_per_player(packs_per_player)
    clean_human_player_name = str(human_player_name or "You").strip() or "You"
    now_utc = draft_test_utc_now()

    deactivate_existing_draft_tests(parsed_campaign_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO draft_test_sessions (
            campaign_id,
            draft_name,
            pod_size,
            packs_per_player,
            status,
            current_pack_number,
            current_pick_number,
            current_human_pack_id,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            parsed_campaign_id,
            None,
            parsed_pod_size,
            parsed_packs_per_player,
            "setup",
            1,
            1,
            None,
            now_utc,
            now_utc,
        ),
    )

    draft_test_id = int(cursor.lastrowid)

    for seat_index in range(parsed_pod_size):
        is_human = 1 if seat_index == 0 else 0
        display_name = clean_human_player_name if is_human else f"AI Drafter {seat_index}"

        color_pair = random.choice(DRAFT_TEST_COLOR_PAIRS)
        color_1 = color_pair[0]
        color_2 = color_pair[1]

        cursor.execute(
            """
            INSERT INTO draft_test_players (
                draft_test_id,
                seat_index,
                display_name,
                is_human,
                color_preference_1,
                color_preference_2,
                created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                draft_test_id,
                seat_index,
                display_name,
                is_human,
                color_1,
                color_2,
                now_utc,
            ),
        )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": "Draft Testing session created.",
        "draft_test_id": draft_test_id,
        "campaign_id": parsed_campaign_id,
        "pod_size": parsed_pod_size,
        "packs_per_player": parsed_packs_per_player,
    }


def get_active_draft_test_session(campaign_id=None):
    ensure_draft_testing_schema()

    parsed_campaign_id = draft_test_normalize_optional_int(campaign_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    if parsed_campaign_id is None:
        cursor.execute(
            """
            SELECT *
            FROM draft_test_sessions
            WHERE campaign_id IS NULL
              AND status IN ('setup', 'drafting')
            ORDER BY created_at_utc DESC, draft_test_id DESC
            LIMIT 1
            """
        )
    else:
        cursor.execute(
            """
            SELECT *
            FROM draft_test_sessions
            WHERE campaign_id = ?
              AND status IN ('setup', 'drafting')
            ORDER BY created_at_utc DESC, draft_test_id DESC
            LIMIT 1
            """,
            (parsed_campaign_id,),
        )

    row = cursor.fetchone()
    conn.close()

    return row


def get_draft_test_players(draft_test_id):
    ensure_draft_testing_schema()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM draft_test_players
        WHERE draft_test_id = ?
        ORDER BY seat_index ASC
        """,
        (int(draft_test_id),),
    )

    rows = cursor.fetchall()
    conn.close()
    return rows


def get_draft_test_pick_counts(draft_test_id):
    ensure_draft_testing_schema()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            seat_index,
            COUNT(*) AS pick_count
        FROM draft_test_picks
        WHERE draft_test_id = ?
        GROUP BY seat_index
        """,
        (int(draft_test_id),),
    )

    rows = cursor.fetchall()
    conn.close()

    return {
        int(row["seat_index"]): int(row["pick_count"] or 0)
        for row in rows
    }



def normalize_draft_test_pack_ids(values):
    normalized_ids = []
    seen_ids = set()

    for value in values or []:
        try:
            parsed_id = int(value)
        except (TypeError, ValueError):
            continue

        if parsed_id <= 0:
            continue

        if parsed_id in seen_ids:
            continue

        normalized_ids.append(parsed_id)
        seen_ids.add(parsed_id)

    return normalized_ids


def get_tracked_pack_rows_for_draft_pool(tracked_pack_ids, campaign_id=None):
    pack_ids = normalize_draft_test_pack_ids(tracked_pack_ids)

    if not pack_ids:
        return []

    parsed_campaign_id = draft_test_normalize_optional_int(campaign_id)
    placeholders = ",".join(["?"] * len(pack_ids))

    sql = f"""
        SELECT
            tracked_pack_id,
            pack_tracking_code,
            pack_display_name,
            set_code,
            booster_name,
            booster_index,
            total_cards,
            campaign_enabled
        FROM tracked_chaos_packs
        WHERE tracked_pack_id IN ({placeholders})
    """

    params = list(pack_ids)

    if parsed_campaign_id is None:
        sql += " AND campaign_id IS NULL"
    else:
        sql += " AND campaign_id = ?"
        params.append(parsed_campaign_id)

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    conn.close()

    row_lookup = {
        int(row["tracked_pack_id"]): row
        for row in rows
    }

    return [
        row_lookup[pack_id]
        for pack_id in pack_ids
        if pack_id in row_lookup
    ]


def get_tracked_pack_card_rows_for_draft_pool(tracked_pack_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            tcpc.card_order,
            tcpc.card_uuid,
            tcpc.card_name,
            tcpc.set_code,
            tcpc.collector_number,
            tcpc.rarity,
            tcpc.type_line,
            tcpc.sheet_is_foil,
            cc.mana_value,
            cc.mana_cost,
            cc.colors_json,
            cc.color_identity_json,
            cc.edhrec_rank,
            cc.edhrec_saltiness,
            cc.image_url
        FROM tracked_chaos_pack_cards tcpc
        LEFT JOIN chaos_cards cc
            ON cc.card_uuid = tcpc.card_uuid
        WHERE tcpc.tracked_pack_id = ?
        ORDER BY
            tcpc.card_order ASC,
            tcpc.tracked_pack_card_id ASC
        """,
        (int(tracked_pack_id),),
    )

    rows = cursor.fetchall()
    conn.close()
    return rows



def create_draft_test_session_from_pack_pool(
    campaign_id=None,
    tracked_pack_ids=None,
    pod_size=8,
    packs_per_player=3,
    human_player_name="You",
    human_player_id=None,
    human_portrait_image_path="",
):
    ensure_draft_testing_schema()

    parsed_campaign_id = draft_test_normalize_optional_int(campaign_id)
    parsed_pod_size = normalize_draft_test_pod_size(pod_size)
    parsed_packs_per_player = normalize_draft_test_packs_per_player(packs_per_player)
    required_pack_count = parsed_pod_size * parsed_packs_per_player
    clean_human_player_name = str(human_player_name or "You").strip() or "You"
    parsed_human_player_id = draft_test_normalize_optional_int(human_player_id)
    clean_human_portrait_image_path = str(human_portrait_image_path or "").strip()

    pack_ids = normalize_draft_test_pack_ids(tracked_pack_ids)

    if len(pack_ids) < required_pack_count:
        return {
            "ok": False,
            "message": f"Not enough packs selected. This draft requires {required_pack_count} pack(s), but only {len(pack_ids)} were selected.",
            "required_pack_count": required_pack_count,
            "selected_pack_count": len(pack_ids),
        }

    pack_rows = get_tracked_pack_rows_for_draft_pool(
        pack_ids,
        campaign_id=parsed_campaign_id,
    )

    if len(pack_rows) < required_pack_count:
        return {
            "ok": False,
            "message": f"Not enough valid packs were found for this campaign. This draft requires {required_pack_count} pack(s), but only {len(pack_rows)} valid selected pack(s) were found.",
            "required_pack_count": required_pack_count,
            "selected_pack_count": len(pack_rows),
        }

    selected_pack_rows = list(pack_rows)
    random.shuffle(selected_pack_rows)
    selected_pack_rows = selected_pack_rows[:required_pack_count]

    now_utc = draft_test_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO draft_test_sessions (
            campaign_id,
            draft_name,
            pod_size,
            packs_per_player,
            status,
            current_pack_number,
            current_pick_number,
            current_human_pack_id,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            parsed_campaign_id,
            "Test Draft",
            parsed_pod_size,
            parsed_packs_per_player,
            "drafting",
            1,
            1,
            None,
            now_utc,
            now_utc,
        ),
    )

    draft_test_id = int(cursor.lastrowid)

    for seat_index in range(parsed_pod_size):
        is_human = 1 if seat_index == 0 else 0
        display_name = clean_human_player_name if is_human else f"AI Drafter {seat_index}"

        color_pair = random.choice(DRAFT_TEST_COLOR_PAIRS)

        cursor.execute(
            """
            INSERT INTO draft_test_players (
                draft_test_id,
                seat_index,
                display_name,
                is_human,
                campaign_player_id,
                portrait_image_path,
                color_preference_1,
                color_preference_2,
                created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                draft_test_id,
                seat_index,
                display_name,
                is_human,
                parsed_human_player_id if is_human else None,
                clean_human_portrait_image_path if is_human else "",
                color_pair[0],
                color_pair[1],
                now_utc,
            ),
        )

    pack_index = 0
    first_human_pack_id = None

    for pack_number in range(1, parsed_packs_per_player + 1):
        for seat_index in range(parsed_pod_size):
            pack_row = selected_pack_rows[pack_index]
            tracked_pack_id = int(pack_row["tracked_pack_id"])

            cursor.execute(
                """
                INSERT INTO draft_test_pack_pool (
                    draft_test_id,
                    tracked_pack_id,
                    pool_order,
                    is_used,
                    assigned_pack_number,
                    assigned_seat_index,
                    created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    draft_test_id,
                    tracked_pack_id,
                    pack_index + 1,
                    1,
                    pack_number,
                    seat_index,
                    now_utc,
                ),
            )

            cursor.execute(
                """
                INSERT INTO draft_test_packs (
                    draft_test_id,
                    pack_number,
                    original_seat_index,
                    current_seat_index,
                    pass_number,
                    tracked_pack_id,
                    pack_tracking_code,
                    pack_display_name,
                    set_code,
                    booster_name,
                    booster_index,
                    is_complete,
                    created_at_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    draft_test_id,
                    pack_number,
                    seat_index,
                    seat_index,
                    1,
                    tracked_pack_id,
                    pack_row["pack_tracking_code"] or "",
                    pack_row["pack_display_name"] or "",
                    pack_row["set_code"] or "",
                    pack_row["booster_name"] or "",
                    int(pack_row["booster_index"] or 0),
                    0,
                    now_utc,
                    now_utc,
                ),
            )

            draft_test_pack_id = int(cursor.lastrowid)

            if pack_number == 1 and seat_index == 0:
                first_human_pack_id = draft_test_pack_id

            card_rows = get_tracked_pack_card_rows_for_draft_pool(tracked_pack_id)

            for card_index, card_row in enumerate(card_rows, start=1):
                cursor.execute(
                    """
                    INSERT INTO draft_test_pack_cards (
                        draft_test_pack_id,
                        draft_test_id,
                        card_order,
                        card_uuid,
                        card_name,
                        set_code,
                        collector_number,
                        rarity,
                        type_line,
                        mana_value,
                        mana_cost,
                        colors_json,
                        color_identity_json,
                        edhrec_rank,
                        edhrec_saltiness,
                        image_url,
                        is_foil,
                        is_picked,
                        picked_by_seat_index,
                        picked_at_pick_number,
                        picked_at_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        draft_test_pack_id,
                        draft_test_id,
                        int(card_row["card_order"] or card_index),
                        card_row["card_uuid"] or "",
                        card_row["card_name"] or "",
                        card_row["set_code"] or "",
                        card_row["collector_number"] or "",
                        card_row["rarity"] or "",
                        card_row["type_line"] or "",
                        card_row["mana_value"],
                        card_row["mana_cost"] or "",
                        card_row["colors_json"] or "[]",
                        card_row["color_identity_json"] or "[]",
                        card_row["edhrec_rank"],
                        card_row["edhrec_saltiness"],
                        card_row["image_url"] or "",
                        int(card_row["sheet_is_foil"] or 0),
                        0,
                        None,
                        None,
                        None,
                    ),
                )

            pack_index += 1

    if first_human_pack_id:
        cursor.execute(
            """
            UPDATE draft_test_sessions
            SET current_human_pack_id = ?,
                updated_at_utc = ?
            WHERE draft_test_id = ?
            """,
            (
                first_human_pack_id,
                now_utc,
                draft_test_id,
            ),
        )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": "Test Draft created.",
        "draft_test_id": draft_test_id,
        "required_pack_count": required_pack_count,
        "selected_pack_count": len(selected_pack_rows),
    }



def get_draft_test_detail(draft_test_id):
    ensure_draft_testing_schema()

    try:
        parsed_draft_test_id = int(draft_test_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid Test Draft ID.",
        }

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            dts.*,
            COALESCE(cc.campaign_name, 'No Campaign') AS campaign_name
        FROM draft_test_sessions dts
        LEFT JOIN chaos_campaigns cc
            ON cc.campaign_id = dts.campaign_id
        WHERE dts.draft_test_id = ?
        """,
        (parsed_draft_test_id,),
    )

    session_row = cursor.fetchone()

    if not session_row:
        conn.close()
        return {
            "ok": False,
            "message": "Test Draft was not found.",
        }

    cursor.execute(
        """
        SELECT
            dtplayer.*,
            COALESCE(
                NULLIF(dtplayer.portrait_image_path, ''),
                cp_by_id.portrait_image_path,
                cp_by_name.portrait_image_path,
                ''
            ) AS effective_portrait_image_path
        FROM draft_test_players dtplayer
        INNER JOIN draft_test_sessions dts
            ON dts.draft_test_id = dtplayer.draft_test_id
        LEFT JOIN chaos_players cp_by_id
            ON cp_by_id.player_id = dtplayer.campaign_player_id
        LEFT JOIN chaos_players cp_by_name
            ON cp_by_name.campaign_id = dts.campaign_id
           AND LOWER(cp_by_name.player_name) = LOWER(dtplayer.display_name)
        WHERE dtplayer.draft_test_id = ?
        ORDER BY dtplayer.seat_index ASC
        """,
        (parsed_draft_test_id,),
    )
    player_rows = cursor.fetchall()

    cursor.execute(
        """
        SELECT *
        FROM draft_test_packs
        WHERE draft_test_pack_id = ?
          AND draft_test_id = ?
        """,
        (
            int(session_row["current_human_pack_id"] or 0),
            parsed_draft_test_id,
        ),
    )
    current_human_pack_row = cursor.fetchone()

    cursor.execute(
        """
        SELECT
            dtp.*,
            COUNT(dtpc.draft_test_pack_card_id) AS remaining_cards
        FROM draft_test_packs dtp
        LEFT JOIN draft_test_pack_cards dtpc
            ON dtpc.draft_test_pack_id = dtp.draft_test_pack_id
           AND dtpc.is_picked = 0
        WHERE dtp.draft_test_id = ?
        GROUP BY dtp.draft_test_pack_id
        ORDER BY
            dtp.pack_number ASC,
            dtp.original_seat_index ASC
        """,
        (parsed_draft_test_id,),
    )
    pack_rows = cursor.fetchall()

    cursor.execute(
        """
        SELECT *
        FROM draft_test_pack_cards
        WHERE draft_test_pack_id = ?
          AND is_picked = 0
        ORDER BY card_order ASC, draft_test_pack_card_id ASC
        """,
        (int(session_row["current_human_pack_id"] or 0),),
    )
    current_pack_cards = cursor.fetchall()

    cursor.execute(
        """
        SELECT
            dtpick.*,
            COALESCE(dtpc.set_code, cc.set_code, '') AS set_code,
            COALESCE(dtpc.collector_number, cc.collector_number, '') AS collector_number,
            COALESCE(dtpc.rarity, cc.rarity, 'common') AS rarity,
            COALESCE(dtpc.type_line, cc.type_line, 'Basic Land') AS type_line,
            COALESCE(dtpc.mana_value, cc.mana_value, 0) AS mana_value,
            COALESCE(dtpc.mana_cost, cc.mana_cost, '') AS mana_cost,
            COALESCE(dtpc.colors_json, cc.colors_json, '[]') AS colors_json,
            COALESCE(dtpc.color_identity_json, cc.color_identity_json, '[]') AS color_identity_json,
            COALESCE(dtpc.image_url, cc.image_url, '') AS image_url
        FROM draft_test_picks dtpick
        INNER JOIN draft_test_players dtplayer
            ON dtplayer.draft_test_player_id = dtpick.draft_test_player_id
        LEFT JOIN draft_test_pack_cards dtpc
            ON dtpc.draft_test_pack_card_id = dtpick.draft_test_pack_card_id
        LEFT JOIN chaos_cards cc
            ON cc.card_uuid = dtpick.card_uuid
        WHERE dtpick.draft_test_id = ?
          AND dtplayer.is_human = 1
          AND dtpick.deck_zone = 'deck'
        ORDER BY
            dtpick.pick_number ASC,
            dtpick.draft_test_pick_id ASC
        """,
        (parsed_draft_test_id,),
    )
    human_deck_cards = cursor.fetchall()

    cursor.execute(
        """
        SELECT
            dtpick.*,
            COALESCE(dtpc.set_code, cc.set_code, '') AS set_code,
            COALESCE(dtpc.collector_number, cc.collector_number, '') AS collector_number,
            COALESCE(dtpc.rarity, cc.rarity, 'common') AS rarity,
            COALESCE(dtpc.type_line, cc.type_line, '') AS type_line,
            COALESCE(dtpc.mana_value, cc.mana_value, 0) AS mana_value,
            COALESCE(dtpc.mana_cost, cc.mana_cost, '') AS mana_cost,
            COALESCE(dtpc.colors_json, cc.colors_json, '[]') AS colors_json,
            COALESCE(dtpc.color_identity_json, cc.color_identity_json, '[]') AS color_identity_json,
            COALESCE(dtpc.image_url, cc.image_url, '') AS image_url
        FROM draft_test_picks dtpick
        INNER JOIN draft_test_players dtplayer
            ON dtplayer.draft_test_player_id = dtpick.draft_test_player_id
        LEFT JOIN draft_test_pack_cards dtpc
            ON dtpc.draft_test_pack_card_id = dtpick.draft_test_pack_card_id
        LEFT JOIN chaos_cards cc
            ON cc.card_uuid = dtpick.card_uuid
        WHERE dtpick.draft_test_id = ?
          AND dtplayer.is_human = 1
          AND dtpick.deck_zone = 'sideboard'
        ORDER BY
            dtpick.pick_number ASC,
            dtpick.draft_test_pick_id ASC
        """,
        (parsed_draft_test_id,),
    )
    human_sideboard_cards = cursor.fetchall()

    basic_land_counts = get_basic_land_counts_for_draft_test(
        cursor=cursor,
        draft_test_id=parsed_draft_test_id,
    )

    conn.close()

    return {
        "ok": True,
        "session": session_row,
        "players": player_rows,
        "packs": pack_rows,
        "current_human_pack": current_human_pack_row,
        "current_pack_cards": current_pack_cards,
        "human_deck_cards": human_deck_cards,
        "human_sideboard_cards": human_sideboard_cards,
        "basic_land_counts": basic_land_counts,
        "basic_land_names": DRAFT_TEST_BASIC_LANDS,
    }

def get_draft_test_virtual_player_detail(draft_test_id, draft_test_player_id=None):
    ensure_draft_testing_schema()

    try:
        parsed_draft_test_id = int(draft_test_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid Test Draft ID.",
        }

    parsed_player_id = draft_test_normalize_optional_int(draft_test_player_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM draft_test_sessions
        WHERE draft_test_id = ?
        """,
        (parsed_draft_test_id,),
    )
    session_row = cursor.fetchone()

    if not session_row:
        conn.close()
        return {
            "ok": False,
            "message": "Test Draft was not found.",
        }

    cursor.execute(
        """
        SELECT *
        FROM draft_test_players
        WHERE draft_test_id = ?
          AND is_human = 0
        ORDER BY seat_index ASC
        """,
        (parsed_draft_test_id,),
    )
    virtual_player_rows = cursor.fetchall()

    selected_player_row = None

    if parsed_player_id is not None:
        for player_row in virtual_player_rows:
            if int(player_row["draft_test_player_id"]) == parsed_player_id:
                selected_player_row = player_row
                break

    if selected_player_row is None and virtual_player_rows:
        selected_player_row = virtual_player_rows[0]

    selected_player_id = int(selected_player_row["draft_test_player_id"]) if selected_player_row else 0

    picked_cards = []

    if selected_player_id > 0:
        cursor.execute(
            """
            SELECT
                dtpick.*,
                COALESCE(dtpc.set_code, cc.set_code, '') AS set_code,
                COALESCE(dtpc.collector_number, cc.collector_number, '') AS collector_number,
                COALESCE(dtpc.rarity, cc.rarity, '') AS rarity,
                COALESCE(dtpc.type_line, cc.type_line, '') AS type_line,
                COALESCE(dtpc.mana_value, cc.mana_value, 0) AS mana_value,
                COALESCE(dtpc.mana_cost, cc.mana_cost, '') AS mana_cost,
                COALESCE(dtpc.colors_json, cc.colors_json, '[]') AS colors_json,
                COALESCE(dtpc.color_identity_json, cc.color_identity_json, '[]') AS color_identity_json,
                COALESCE(dtpc.image_url, cc.image_url, '') AS image_url
            FROM draft_test_picks dtpick
            LEFT JOIN draft_test_pack_cards dtpc
                ON dtpc.draft_test_pack_card_id = dtpick.draft_test_pack_card_id
            LEFT JOIN chaos_cards cc
                ON cc.card_uuid = dtpick.card_uuid
            WHERE dtpick.draft_test_id = ?
              AND dtpick.draft_test_player_id = ?
            ORDER BY
                dtpick.pack_number ASC,
                dtpick.pick_number ASC,
                dtpick.draft_test_pick_id ASC
            """,
            (
                parsed_draft_test_id,
                selected_player_id,
            ),
        )
        picked_cards = cursor.fetchall()

    conn.close()

    return {
        "ok": True,
        "session": session_row,
        "virtual_players": virtual_player_rows,
        "selected_player": selected_player_row,
        "picked_cards": picked_cards,
    }

def get_draft_test_pass_direction(pack_number):
    try:
        parsed_pack_number = int(pack_number)
    except (TypeError, ValueError):
        parsed_pack_number = 1

    if parsed_pack_number % 2 == 1:
        return 1

    return -1

def get_draft_test_player_picked_cards_for_bot_selection(cursor, draft_test_id, draft_test_player_id):
    cursor.execute(
        """
        SELECT
            dtpick.*,
            COALESCE(dtpc.set_code, cc.set_code, '') AS set_code,
            COALESCE(dtpc.collector_number, cc.collector_number, '') AS collector_number,
            COALESCE(dtpc.rarity, cc.rarity, '') AS rarity,
            COALESCE(dtpc.type_line, cc.type_line, '') AS type_line,
            COALESCE(dtpc.mana_value, cc.mana_value, 0) AS mana_value,
            COALESCE(dtpc.mana_cost, cc.mana_cost, '') AS mana_cost,
            COALESCE(dtpc.colors_json, cc.colors_json, '[]') AS colors_json,
            COALESCE(dtpc.color_identity_json, cc.color_identity_json, '[]') AS color_identity_json,
            COALESCE(dtpc.edhrec_rank, cc.edhrec_rank) AS edhrec_rank,
            COALESCE(dtpc.edhrec_saltiness, cc.edhrec_saltiness) AS edhrec_saltiness,
            COALESCE(dtpc.image_url, cc.image_url, '') AS image_url
        FROM draft_test_picks dtpick
        LEFT JOIN draft_test_pack_cards dtpc
            ON dtpc.draft_test_pack_card_id = dtpick.draft_test_pack_card_id
        LEFT JOIN chaos_cards cc
            ON cc.card_uuid = dtpick.card_uuid
        WHERE dtpick.draft_test_id = ?
          AND dtpick.draft_test_player_id = ?
        ORDER BY
            dtpick.pack_number ASC,
            dtpick.pick_number ASC,
            dtpick.draft_test_pick_id ASC
        """,
        (
            int(draft_test_id),
            int(draft_test_player_id),
        ),
    )

    return cursor.fetchall()

def choose_ai_draft_test_card(cursor, session_row, draft_test_pack_id, player_row):
    cursor.execute(
        """
        SELECT *
        FROM draft_test_pack_cards
        WHERE draft_test_pack_id = ?
          AND is_picked = 0
        ORDER BY card_order ASC, draft_test_pack_card_id ASC
        """,
        (int(draft_test_pack_id),),
    )

    available_cards = cursor.fetchall()

    if not available_cards:
        return None

    drafted_cards = get_draft_test_player_picked_cards_for_bot_selection(
        cursor=cursor,
        draft_test_id=int(session_row["draft_test_id"]),
        draft_test_player_id=int(player_row["draft_test_player_id"]),
    )

    draft_context = {
        "draft_test_id": int(session_row["draft_test_id"]),
        "pack_number": int(session_row["current_pack_number"] or 1),
        "pick_number": int(session_row["current_pick_number"] or 1),
        "packs_per_player": int(session_row["packs_per_player"] or 3),
        "pod_size": int(session_row["pod_size"] or 8),
    }

    return choose_bot_draft_pick(
        available_cards=available_cards,
        drafted_cards=drafted_cards,
        player_row=player_row,
        draft_context=draft_context,
    )


def record_ai_draft_test_pick(cursor, session_row, player_row, pack_row, current_pick_number, now_utc):
    ai_choice = choose_ai_draft_test_card(
        cursor=cursor,
        session_row=session_row,
        draft_test_pack_id=int(pack_row["draft_test_pack_id"]),
        player_row=player_row,
    )

    if not ai_choice or not ai_choice.get("card"):
        return False

    selected_card_row = ai_choice["card"]
    selected_score = ai_choice.get("score")
    selected_reason = ai_choice.get("reason") or "AI pick"

    cursor.execute(
        """
        UPDATE draft_test_pack_cards
        SET is_picked = 1,
            picked_by_seat_index = ?,
            picked_at_pick_number = ?,
            picked_at_utc = ?
        WHERE draft_test_pack_card_id = ?
          AND draft_test_id = ?
          AND is_picked = 0
        """,
        (
            int(player_row["seat_index"]),
            current_pick_number,
            now_utc,
            int(selected_card_row["draft_test_pack_card_id"]),
            int(session_row["draft_test_id"]),
        ),
    )

    if cursor.rowcount <= 0:
        return False

    cursor.execute(
        """
        INSERT INTO draft_test_picks (
            draft_test_id,
            draft_test_player_id,
            draft_test_pack_id,
            draft_test_pack_card_id,
            seat_index,
            pack_number,
            pick_number,
            card_uuid,
            card_name,
            deck_zone,
            picked_at_utc,
            pick_score,
            pick_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(session_row["draft_test_id"]),
            int(player_row["draft_test_player_id"]),
            int(pack_row["draft_test_pack_id"]),
            int(selected_card_row["draft_test_pack_card_id"]),
            int(player_row["seat_index"]),
            int(pack_row["pack_number"]),
            current_pick_number,
            selected_card_row["card_uuid"] or "",
            selected_card_row["card_name"] or "",
            "deck",
            now_utc,
            selected_score,
            selected_reason,
        ),
    )

    return True


def record_ai_draft_test_picks_for_round(cursor, session_row, current_pick_number, now_utc):
    cursor.execute(
        """
        SELECT *
        FROM draft_test_players
        WHERE draft_test_id = ?
          AND is_human = 0
        ORDER BY seat_index ASC
        """,
        (int(session_row["draft_test_id"]),),
    )
    ai_player_rows = cursor.fetchall()

    for player_row in ai_player_rows:
        cursor.execute(
            """
            SELECT *
            FROM draft_test_packs
            WHERE draft_test_id = ?
              AND pack_number = ?
              AND current_seat_index = ?
              AND is_complete = 0
            LIMIT 1
            """,
            (
                int(session_row["draft_test_id"]),
                int(session_row["current_pack_number"]),
                int(player_row["seat_index"]),
            ),
        )
        pack_row = cursor.fetchone()

        if not pack_row:
            continue

        record_ai_draft_test_pick(
            cursor=cursor,
            session_row=session_row,
            player_row=player_row,
            pack_row=pack_row,
            current_pick_number=current_pick_number,
            now_utc=now_utc,
        )


def get_remaining_cards_for_draft_test_pack(cursor, draft_test_pack_id):
    cursor.execute(
        """
        SELECT COUNT(*) AS remaining_count
        FROM draft_test_pack_cards
        WHERE draft_test_pack_id = ?
          AND is_picked = 0
        """,
        (int(draft_test_pack_id),),
    )

    row = cursor.fetchone()

    if not row:
        return 0

    return int(row["remaining_count"] or 0)


def advance_draft_test_after_round(cursor, session_row, now_utc):
    draft_test_id = int(session_row["draft_test_id"])
    pod_size = int(session_row["pod_size"] or 8)
    current_pack_number = int(session_row["current_pack_number"] or 1)
    current_pick_number = int(session_row["current_pick_number"] or 1)
    packs_per_player = int(session_row["packs_per_player"] or 3)

    cursor.execute(
        """
        SELECT *
        FROM draft_test_packs
        WHERE draft_test_id = ?
          AND pack_number = ?
          AND is_complete = 0
        ORDER BY draft_test_pack_id ASC
        """,
        (
            draft_test_id,
            current_pack_number,
        ),
    )
    active_pack_rows = cursor.fetchall()

    active_pack_ids_with_cards = []

    for pack_row in active_pack_rows:
        remaining_count = get_remaining_cards_for_draft_test_pack(
            cursor=cursor,
            draft_test_pack_id=int(pack_row["draft_test_pack_id"]),
        )

        if remaining_count <= 0:
            cursor.execute(
                """
                UPDATE draft_test_packs
                SET is_complete = 1,
                    updated_at_utc = ?
                WHERE draft_test_pack_id = ?
                """,
                (
                    now_utc,
                    int(pack_row["draft_test_pack_id"]),
                ),
            )
        else:
            active_pack_ids_with_cards.append(int(pack_row["draft_test_pack_id"]))

    if active_pack_ids_with_cards:
        pass_direction = get_draft_test_pass_direction(current_pack_number)

        for pack_row in active_pack_rows:
            draft_test_pack_id = int(pack_row["draft_test_pack_id"])

            if draft_test_pack_id not in active_pack_ids_with_cards:
                continue

            next_seat_index = (int(pack_row["current_seat_index"]) + pass_direction) % pod_size

            cursor.execute(
                """
                UPDATE draft_test_packs
                SET current_seat_index = ?,
                    pass_number = pass_number + 1,
                    updated_at_utc = ?
                WHERE draft_test_pack_id = ?
                """,
                (
                    next_seat_index,
                    now_utc,
                    draft_test_pack_id,
                ),
            )

        cursor.execute(
            """
            SELECT draft_test_pack_id
            FROM draft_test_packs
            WHERE draft_test_id = ?
              AND pack_number = ?
              AND current_seat_index = 0
              AND is_complete = 0
            LIMIT 1
            """,
            (
                draft_test_id,
                current_pack_number,
            ),
        )
        next_human_pack_row = cursor.fetchone()

        cursor.execute(
            """
            UPDATE draft_test_sessions
            SET current_pick_number = ?,
                current_human_pack_id = ?,
                updated_at_utc = ?
            WHERE draft_test_id = ?
            """,
            (
                current_pick_number + 1,
                int(next_human_pack_row["draft_test_pack_id"]) if next_human_pack_row else None,
                now_utc,
                draft_test_id,
            ),
        )

        return

    next_pack_number = current_pack_number + 1

    if next_pack_number > packs_per_player:
        cursor.execute(
            """
            UPDATE draft_test_sessions
            SET status = 'complete',
                current_human_pack_id = NULL,
                updated_at_utc = ?
            WHERE draft_test_id = ?
            """,
            (
                now_utc,
                draft_test_id,
            ),
        )

        return

    cursor.execute(
        """
        SELECT draft_test_pack_id
        FROM draft_test_packs
        WHERE draft_test_id = ?
          AND pack_number = ?
          AND current_seat_index = 0
          AND is_complete = 0
        LIMIT 1
        """,
        (
            draft_test_id,
            next_pack_number,
        ),
    )
    next_pack_row = cursor.fetchone()

    cursor.execute(
        """
        UPDATE draft_test_sessions
        SET current_pack_number = ?,
            current_pick_number = 1,
            current_human_pack_id = ?,
            updated_at_utc = ?
        WHERE draft_test_id = ?
        """,
        (
            next_pack_number,
            int(next_pack_row["draft_test_pack_id"]) if next_pack_row else None,
            now_utc,
            draft_test_id,
        ),
    )


def get_basic_land_card_for_draft_test(cursor, land_name):
    clean_land_name = normalize_draft_test_basic_land_name(land_name)

    if not clean_land_name:
        return None

    cursor.execute(
        """
        SELECT
            card_uuid,
            set_code,
            card_name,
            collector_number,
            rarity,
            type_line,
            mana_value,
            mana_cost,
            colors_json,
            color_identity_json,
            image_url
        FROM chaos_cards
        WHERE LOWER(card_name) = LOWER(?)
          AND (
                LOWER(type_line) LIKE '%basic land%'
                OR LOWER(type_line) LIKE '%land%'
              )
        ORDER BY
            CASE
                WHEN LOWER(set_code) IN ('fdn', 'dmu', 'neo', 'znr') THEN 0
                ELSE 1
            END,
            set_code DESC,
            collector_number ASC
        LIMIT 1
        """,
        (clean_land_name,),
    )

    return cursor.fetchone()


def get_basic_land_counts_for_draft_test(cursor, draft_test_id):
    cursor.execute(
        """
        SELECT
            card_name,
            COUNT(*) AS land_count
        FROM draft_test_picks
        WHERE draft_test_id = ?
          AND deck_zone = 'deck'
          AND pick_reason = 'Basic land'
        GROUP BY card_name
        """,
        (int(draft_test_id),),
    )

    rows = cursor.fetchall()

    counts = {
        land_name: 0
        for land_name in DRAFT_TEST_BASIC_LANDS
    }

    for row in rows:
        land_name = normalize_draft_test_basic_land_name(row["card_name"])

        if land_name:
            counts[land_name] = int(row["land_count"] or 0)

    return counts


def record_human_draft_test_basic_land(draft_test_id, land_name):
    ensure_draft_testing_schema()

    try:
        parsed_draft_test_id = int(draft_test_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid Test Draft ID.",
        }

    clean_land_name = normalize_draft_test_basic_land_name(land_name)

    if not clean_land_name:
        return {
            "ok": False,
            "message": "Invalid basic land.",
        }

    now_utc = draft_test_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM draft_test_sessions
        WHERE draft_test_id = ?
          AND status = 'complete'
        """,
        (parsed_draft_test_id,),
    )
    session_row = cursor.fetchone()

    if not session_row:
        conn.close()
        return {
            "ok": False,
            "message": "Basic lands can only be changed after the draft is complete.",
        }

    cursor.execute(
        """
        SELECT *
        FROM draft_test_players
        WHERE draft_test_id = ?
          AND is_human = 1
        ORDER BY seat_index ASC
        LIMIT 1
        """,
        (parsed_draft_test_id,),
    )
    human_player_row = cursor.fetchone()

    if not human_player_row:
        conn.close()
        return {
            "ok": False,
            "message": "Human drafter was not found.",
        }

    land_card_row = get_basic_land_card_for_draft_test(
        cursor=cursor,
        land_name=clean_land_name,
    )

    if not land_card_row:
        conn.close()
        return {
            "ok": False,
            "message": f"{clean_land_name} was not found in the card database.",
        }

    cursor.execute(
        """
        INSERT INTO draft_test_picks (
            draft_test_id,
            draft_test_player_id,
            draft_test_pack_id,
            draft_test_pack_card_id,
            seat_index,
            pack_number,
            pick_number,
            card_uuid,
            card_name,
            deck_zone,
            picked_at_utc,
            pick_score,
            pick_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            parsed_draft_test_id,
            int(human_player_row["draft_test_player_id"]),
            0,
            0,
            int(human_player_row["seat_index"]),
            0,
            0,
            land_card_row["card_uuid"] or "",
            clean_land_name,
            "deck",
            now_utc,
            None,
            "Basic land",
        ),
    )

    cursor.execute(
        """
        UPDATE draft_test_sessions
        SET updated_at_utc = ?
        WHERE draft_test_id = ?
        """,
        (
            now_utc,
            parsed_draft_test_id,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": f"Added {clean_land_name}.",
        "land_name": clean_land_name,
    }


def remove_human_draft_test_basic_land(draft_test_id, land_name):
    ensure_draft_testing_schema()

    try:
        parsed_draft_test_id = int(draft_test_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid Test Draft ID.",
        }

    clean_land_name = normalize_draft_test_basic_land_name(land_name)

    if not clean_land_name:
        return {
            "ok": False,
            "message": "Invalid basic land.",
        }

    now_utc = draft_test_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM draft_test_sessions
        WHERE draft_test_id = ?
          AND status = 'complete'
        """,
        (parsed_draft_test_id,),
    )
    session_row = cursor.fetchone()

    if not session_row:
        conn.close()
        return {
            "ok": False,
            "message": "Basic lands can only be changed after the draft is complete.",
        }

    cursor.execute(
        """
        SELECT draft_test_pick_id
        FROM draft_test_picks
        WHERE draft_test_id = ?
          AND deck_zone = 'deck'
          AND pick_reason = 'Basic land'
          AND LOWER(card_name) = LOWER(?)
        ORDER BY draft_test_pick_id DESC
        LIMIT 1
        """,
        (
            parsed_draft_test_id,
            clean_land_name,
        ),
    )
    land_pick_row = cursor.fetchone()

    if not land_pick_row:
        conn.close()
        return {
            "ok": False,
            "message": f"No {clean_land_name} is currently in the deck.",
        }

    cursor.execute(
        """
        DELETE FROM draft_test_picks
        WHERE draft_test_pick_id = ?
          AND draft_test_id = ?
          AND pick_reason = 'Basic land'
        """,
        (
            int(land_pick_row["draft_test_pick_id"]),
            parsed_draft_test_id,
        ),
    )

    cursor.execute(
        """
        UPDATE draft_test_sessions
        SET updated_at_utc = ?
        WHERE draft_test_id = ?
        """,
        (
            now_utc,
            parsed_draft_test_id,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": f"Removed {clean_land_name}.",
        "land_name": clean_land_name,
    }

def move_human_draft_test_pick_zone(draft_test_id, draft_test_pick_id, deck_zone):
    ensure_draft_testing_schema()

    try:
        parsed_draft_test_id = int(draft_test_id)
        parsed_pick_id = int(draft_test_pick_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid draft pick.",
        }

    clean_deck_zone = (deck_zone or "deck").strip().lower()

    if clean_deck_zone not in {"deck", "sideboard"}:
        return {
            "ok": False,
            "message": "Invalid deck zone.",
        }

    now_utc = draft_test_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT dtpick.*
        FROM draft_test_picks dtpick
        INNER JOIN draft_test_players dtplayer
            ON dtplayer.draft_test_player_id = dtpick.draft_test_player_id
        WHERE dtpick.draft_test_id = ?
          AND dtpick.draft_test_pick_id = ?
          AND dtplayer.is_human = 1
        """,
        (
            parsed_draft_test_id,
            parsed_pick_id,
        ),
    )

    pick_row = cursor.fetchone()

    if not pick_row:
        conn.close()
        return {
            "ok": False,
            "message": "Human draft pick was not found.",
        }

    cursor.execute(
        """
        UPDATE draft_test_picks
        SET deck_zone = ?
        WHERE draft_test_id = ?
          AND draft_test_pick_id = ?
        """,
        (
            clean_deck_zone,
            parsed_draft_test_id,
            parsed_pick_id,
        ),
    )

    cursor.execute(
        """
        UPDATE draft_test_sessions
        SET updated_at_utc = ?
        WHERE draft_test_id = ?
        """,
        (
            now_utc,
            parsed_draft_test_id,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": "Pick moved.",
        "draft_test_id": parsed_draft_test_id,
        "draft_test_pick_id": parsed_pick_id,
        "deck_zone": clean_deck_zone,
    }


def record_human_draft_test_pick(draft_test_id, draft_test_pack_card_id, deck_zone="deck"):
    ensure_draft_testing_schema()

    try:
        parsed_draft_test_id = int(draft_test_id)
        parsed_pack_card_id = int(draft_test_pack_card_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid draft or card selection.",
        }

    clean_deck_zone = (deck_zone or "deck").strip().lower()

    if clean_deck_zone not in {"deck", "sideboard"}:
        clean_deck_zone = "deck"

    now_utc = draft_test_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM draft_test_sessions
        WHERE draft_test_id = ?
          AND status = 'drafting'
        """,
        (parsed_draft_test_id,),
    )
    session_row = cursor.fetchone()

    if not session_row:
        conn.close()
        return {
            "ok": False,
            "message": "Active Test Draft was not found.",
        }

    current_human_pack_id = int(session_row["current_human_pack_id"] or 0)

    if current_human_pack_id <= 0:
        conn.close()
        return {
            "ok": False,
            "message": "No current pack is assigned to the human drafter.",
        }

    cursor.execute(
        """
        SELECT *
        FROM draft_test_players
        WHERE draft_test_id = ?
          AND is_human = 1
        ORDER BY seat_index ASC
        LIMIT 1
        """,
        (parsed_draft_test_id,),
    )
    human_player_row = cursor.fetchone()

    if not human_player_row:
        conn.close()
        return {
            "ok": False,
            "message": "Human drafter was not found.",
        }

    cursor.execute(
        """
        SELECT *
        FROM draft_test_packs
        WHERE draft_test_pack_id = ?
          AND draft_test_id = ?
        """,
        (
            current_human_pack_id,
            parsed_draft_test_id,
        ),
    )
    current_pack_row = cursor.fetchone()

    if not current_pack_row:
        conn.close()
        return {
            "ok": False,
            "message": "Current draft pack was not found.",
        }

    cursor.execute(
        """
        SELECT *
        FROM draft_test_pack_cards
        WHERE draft_test_pack_card_id = ?
          AND draft_test_id = ?
          AND draft_test_pack_id = ?
          AND is_picked = 0
        """,
        (
            parsed_pack_card_id,
            parsed_draft_test_id,
            current_human_pack_id,
        ),
    )
    selected_card_row = cursor.fetchone()

    if not selected_card_row:
        conn.close()
        return {
            "ok": False,
            "message": "Selected card is not available in the current pack.",
        }

    current_pick_number = int(session_row["current_pick_number"] or 1)

    cursor.execute(
        """
        UPDATE draft_test_pack_cards
        SET is_picked = 1,
            picked_by_seat_index = ?,
            picked_at_pick_number = ?,
            picked_at_utc = ?
        WHERE draft_test_pack_card_id = ?
          AND draft_test_id = ?
          AND is_picked = 0
        """,
        (
            int(human_player_row["seat_index"]),
            current_pick_number,
            now_utc,
            parsed_pack_card_id,
            parsed_draft_test_id,
        ),
    )

    if cursor.rowcount <= 0:
        conn.rollback()
        conn.close()
        return {
            "ok": False,
            "message": "Selected card could not be picked.",
        }

    cursor.execute(
        """
        INSERT INTO draft_test_picks (
            draft_test_id,
            draft_test_player_id,
            draft_test_pack_id,
            draft_test_pack_card_id,
            seat_index,
            pack_number,
            pick_number,
            card_uuid,
            card_name,
            deck_zone,
            picked_at_utc,
            pick_score,
            pick_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            parsed_draft_test_id,
            int(human_player_row["draft_test_player_id"]),
            current_human_pack_id,
            parsed_pack_card_id,
            int(human_player_row["seat_index"]),
            int(current_pack_row["pack_number"]),
            current_pick_number,
            selected_card_row["card_uuid"] or "",
            selected_card_row["card_name"] or "",
            clean_deck_zone,
            now_utc,
            None,
            "Human pick",
        ),
    )

    record_ai_draft_test_picks_for_round(
        cursor=cursor,
        session_row=session_row,
        current_pick_number=current_pick_number,
        now_utc=now_utc,
    )

    advance_draft_test_after_round(
        cursor=cursor,
        session_row=session_row,
        now_utc=now_utc,
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": "Pick recorded.",
        "draft_test_id": parsed_draft_test_id,
        "draft_test_pack_card_id": parsed_pack_card_id,
        "card_name": selected_card_row["card_name"] or "",
        "deck_zone": clean_deck_zone,
    }