import json
import random


BOT_COLOR_CODES = ["W", "U", "B", "R", "G"]

BOT_COLOR_TRACKING_COLUMN_MAP = {
    "W": "color_tracking_white",
    "U": "color_tracking_blue",
    "B": "color_tracking_black",
    "R": "color_tracking_red",
    "G": "color_tracking_green",
}

BOT_BASIC_LAND_NAMES = {
    "plains",
    "island",
    "swamp",
    "mountain",
    "forest",
    "wastes",
}


def bot_row_get(row, key, default=None):
    if row is None:
        return default

    try:
        if hasattr(row, "keys") and key in row.keys():
            return row[key]
    except Exception:
        pass

    try:
        if isinstance(row, dict):
            return row.get(key, default)
    except Exception:
        pass

    return default


def bot_parse_json_list(value):
    if isinstance(value, list):
        return value

    raw_value = str(value or "").strip()

    if not raw_value:
        return []

    try:
        parsed_value = json.loads(raw_value)
        return parsed_value if isinstance(parsed_value, list) else []
    except Exception:
        return []


def bot_normalize_color_list(value):
    colors = []

    for color_value in bot_parse_json_list(value):
        clean_color = str(color_value or "").strip().upper()

        if clean_color in BOT_COLOR_CODES and clean_color not in colors:
            colors.append(clean_color)

    return colors


def bot_get_card_color_identity(card_row):
    return bot_normalize_color_list(bot_row_get(card_row, "color_identity_json", "[]"))


def bot_get_card_mana_cost(card_row):
    return str(bot_row_get(card_row, "mana_cost", "") or "").strip().upper()


def bot_get_card_name(card_row):
    return str(bot_row_get(card_row, "card_name", "") or "").strip()


def bot_get_card_type_line(card_row):
    return str(bot_row_get(card_row, "type_line", "") or "").strip()


def bot_get_card_mana_value(card_row):
    try:
        mana_value = bot_row_get(card_row, "mana_value", None)

        if mana_value is None or mana_value == "":
            return None

        return float(mana_value)
    except (TypeError, ValueError):
        return None


def bot_is_land(card_row):
    return "land" in bot_get_card_type_line(card_row).lower()


def bot_is_basic_land(card_row):
    card_name = bot_get_card_name(card_row).lower()
    type_line = bot_get_card_type_line(card_row).lower()

    return card_name in BOT_BASIC_LAND_NAMES or "basic land" in type_line


def bot_is_creature(card_row):
    return "creature" in bot_get_card_type_line(card_row).lower()


def bot_is_artifact(card_row):
    return "artifact" in bot_get_card_type_line(card_row).lower()


def bot_is_enchantment(card_row):
    return "enchantment" in bot_get_card_type_line(card_row).lower()


def bot_is_instant_or_sorcery(card_row):
    type_line = bot_get_card_type_line(card_row).lower()
    return "instant" in type_line or "sorcery" in type_line


def bot_get_rarity_score(card_row):
    rarity = str(bot_row_get(card_row, "rarity", "") or "").strip().lower()

    if rarity == "mythic":
        return 7.0

    if rarity == "rare":
        return 5.0

    if rarity == "uncommon":
        return 3.0

    return 0.0


def bot_get_edhrec_score(card_row):
    edhrec_rank = bot_row_get(card_row, "edhrec_rank", None)

    if edhrec_rank is None:
        return 0.0

    try:
        parsed_rank = int(edhrec_rank)

        if parsed_rank <= 0:
            return 0.0

        return max(0.0, 6.0 - min(parsed_rank, 6000) / 1000.0)
    except (TypeError, ValueError):
        return 0.0


def bot_get_base_quality_score(card_row):
    if bot_is_land(card_row):
        return 0.0

    score = 0.0

    score += bot_get_rarity_score(card_row)
    score += bot_get_edhrec_score(card_row)

    mana_value = bot_get_card_mana_value(card_row)

    if mana_value is not None:
        if 2 <= mana_value <= 4:
            score += 0.85
        elif mana_value >= 7:
            score -= 0.85

    if bot_is_creature(card_row):
        score += 0.60

    if bot_is_instant_or_sorcery(card_row):
        score += 0.25

    return score


def bot_get_draft_stage(draft_context):
    pack_number = int((draft_context or {}).get("pack_number") or 1)
    pick_number = int((draft_context or {}).get("pick_number") or 1)
    packs_per_player = int((draft_context or {}).get("packs_per_player") or 3)

    total_estimated_picks = max(1, packs_per_player * 15)
    absolute_pick = ((pack_number - 1) * 15) + pick_number
    progress = absolute_pick / total_estimated_picks

    if pack_number <= 1 and pick_number <= 4:
        return "early"

    if progress < 0.33:
        return "forming"

    if progress < 0.62:
        return "middle"

    return "late"


def bot_get_stage_weights(stage):
    if stage == "early":
        return {
            "off_color_penalty": -1.25,
            "on_color_bonus": 1.50,
            "seeded_color_bonus": 1.00,
            "splash_penalty": -0.75,
            "curve_weight": 0.40,
            "deck_shape_weight": 0.35,
            "noise": 1.15,
        }

    if stage == "forming":
        return {
            "off_color_penalty": -2.75,
            "on_color_bonus": 2.50,
            "seeded_color_bonus": 0.65,
            "splash_penalty": -1.50,
            "curve_weight": 0.75,
            "deck_shape_weight": 0.75,
            "noise": 0.85,
        }

    if stage == "middle":
        return {
            "off_color_penalty": -5.50,
            "on_color_bonus": 4.00,
            "seeded_color_bonus": 0.25,
            "splash_penalty": -2.75,
            "curve_weight": 1.10,
            "deck_shape_weight": 1.10,
            "noise": 0.55,
        }

    return {
        "off_color_penalty": -8.50,
        "on_color_bonus": 5.50,
        "seeded_color_bonus": 0.00,
        "splash_penalty": -4.00,
        "curve_weight": 1.45,
        "deck_shape_weight": 1.40,
        "noise": 0.30,
    }


def bot_count_colored_pips(card_row):
    mana_cost = bot_get_card_mana_cost(card_row)
    pip_counts = {color_code: 0 for color_code in BOT_COLOR_CODES}

    for color_code in BOT_COLOR_CODES:
        pip_counts[color_code] = mana_cost.count("{" + color_code + "}")

    return pip_counts


def bot_build_draft_profile(drafted_cards, player_row, draft_context):
    drafted_cards = list(drafted_cards or [])

    color_playable_counts = {color_code: 0 for color_code in BOT_COLOR_CODES}
    color_quality_scores = {color_code: 0.0 for color_code in BOT_COLOR_CODES}
    mana_curve = {
        "0-1": 0,
        "2": 0,
        "3": 0,
        "4": 0,
        "5": 0,
        "6+": 0,
    }

    creature_count = 0
    noncreature_count = 0
    fixing_land_count = 0
    nonbasic_land_count = 0
    high_quality_cards = 0
    drafted_nonland_count = 0

    for card_row in drafted_cards:
        if bot_is_basic_land(card_row):
            continue

        if bot_is_land(card_row):
            nonbasic_land_count += 1

            if len(bot_get_card_color_identity(card_row)) >= 2:
                fixing_land_count += 1

            continue

        drafted_nonland_count += 1

        card_quality = bot_get_base_quality_score(card_row)
        card_colors = bot_get_card_color_identity(card_row)

        if card_quality >= 6.5:
            high_quality_cards += 1

        if not card_colors:
            # Colorless cards are playable in any deck, but do not define color identity.
            pass
        else:
            for color_code in card_colors:
                color_playable_counts[color_code] += 1
                color_quality_scores[color_code] += card_quality

        if bot_is_creature(card_row):
            creature_count += 1
        else:
            noncreature_count += 1

        mana_value = bot_get_card_mana_value(card_row)

        if mana_value is None:
            continue

        if mana_value <= 1:
            mana_curve["0-1"] += 1
        elif mana_value == 2:
            mana_curve["2"] += 1
        elif mana_value == 3:
            mana_curve["3"] += 1
        elif mana_value == 4:
            mana_curve["4"] += 1
        elif mana_value == 5:
            mana_curve["5"] += 1
        else:
            mana_curve["6+"] += 1

    color_commitment_scores = {}

    for color_code in BOT_COLOR_CODES:
        tracking_column = BOT_COLOR_TRACKING_COLUMN_MAP[color_code]
        tracking_value = bot_row_get(player_row, tracking_column, 0) or 0

        try:
            tracking_value = float(tracking_value)
        except (TypeError, ValueError):
            tracking_value = 0.0

        color_commitment_scores[color_code] = (
            (color_playable_counts[color_code] * 2.0)
            + color_quality_scores[color_code]
            + (tracking_value * 0.35)
        )

    ranked_colors = sorted(
        BOT_COLOR_CODES,
        key=lambda color_code: color_commitment_scores[color_code],
        reverse=True,
    )

    seeded_colors = [
        str(bot_row_get(player_row, "color_preference_1", "") or "").strip().upper(),
        str(bot_row_get(player_row, "color_preference_2", "") or "").strip().upper(),
    ]
    seeded_colors = [
        color_code
        for color_code in seeded_colors
        if color_code in BOT_COLOR_CODES
    ]

    stage = bot_get_draft_stage(draft_context)

    primary_colors = []
    splash_colors = []
    identity_type = "open"

    best_color = ranked_colors[0] if ranked_colors else ""
    second_color = ranked_colors[1] if len(ranked_colors) > 1 else ""
    third_color = ranked_colors[2] if len(ranked_colors) > 2 else ""

    best_score = color_commitment_scores.get(best_color, 0.0)
    second_score = color_commitment_scores.get(second_color, 0.0)
    third_score = color_commitment_scores.get(third_color, 0.0)

    if stage == "early":
        identity_type = "open"
        primary_colors = seeded_colors[:2] if seeded_colors else ranked_colors[:2]
    else:
        if drafted_nonland_count >= 8 and best_score > 0 and second_score < max(3.0, best_score * 0.32):
            identity_type = "mono"
            primary_colors = [best_color]
        elif fixing_land_count >= 3 and third_score >= max(5.0, second_score * 0.70):
            identity_type = "tri"
            primary_colors = ranked_colors[:3]
        else:
            identity_type = "dual"
            primary_colors = [
                color_code
                for color_code in ranked_colors[:2]
                if color_commitment_scores.get(color_code, 0.0) > 0
            ]

            if len(primary_colors) < 2 and seeded_colors:
                for color_code in seeded_colors:
                    if color_code not in primary_colors:
                        primary_colors.append(color_code)
                    if len(primary_colors) >= 2:
                        break

        for color_code in ranked_colors:
            if color_code in primary_colors:
                continue

            if color_commitment_scores.get(color_code, 0.0) >= max(4.0, second_score * 0.55):
                splash_colors.append(color_code)

            if len(splash_colors) >= 1 and identity_type != "tri":
                break

    return {
        "stage": stage,
        "seeded_colors": seeded_colors,
        "primary_colors": primary_colors,
        "splash_colors": splash_colors,
        "ranked_colors": ranked_colors,
        "identity_type": identity_type,
        "color_playable_counts": color_playable_counts,
        "color_quality_scores": color_quality_scores,
        "color_commitment_scores": color_commitment_scores,
        "mana_curve": mana_curve,
        "creature_count": creature_count,
        "noncreature_count": noncreature_count,
        "fixing_land_count": fixing_land_count,
        "nonbasic_land_count": nonbasic_land_count,
        "high_quality_cards": high_quality_cards,
        "drafted_nonland_count": drafted_nonland_count,
    }


def bot_get_color_fit_score(card_row, profile, draft_context):
    if bot_is_basic_land(card_row):
        return -999.0

    stage = profile.get("stage") or "early"
    weights = bot_get_stage_weights(stage)

    card_colors = bot_get_card_color_identity(card_row)
    primary_colors = profile.get("primary_colors") or []
    splash_colors = profile.get("splash_colors") or []
    seeded_colors = profile.get("seeded_colors") or []

    if not card_colors:
        if bot_is_land(card_row):
            return 0.0

        return 1.0

    off_primary_colors = [
        color_code
        for color_code in card_colors
        if color_code not in primary_colors
    ]

    on_primary_count = len([
        color_code
        for color_code in card_colors
        if color_code in primary_colors
    ])

    score = 0.0

    if on_primary_count > 0:
        score += weights["on_color_bonus"] * on_primary_count

    if stage in {"early", "forming"}:
        seeded_match_count = len([
            color_code
            for color_code in card_colors
            if color_code in seeded_colors
        ])
        score += weights["seeded_color_bonus"] * seeded_match_count

    if not primary_colors or stage == "early":
        return score

    if not off_primary_colors:
        return score + 1.5

    card_quality = bot_get_base_quality_score(card_row)
    pip_counts = bot_count_colored_pips(card_row)
    off_color_pips = sum(
        pip_counts.get(color_code, 0)
        for color_code in off_primary_colors
    )

    is_reasonable_splash = (
        len(off_primary_colors) == 1
        and off_color_pips <= 1
        and card_quality >= 5.5
        and (bot_get_card_mana_value(card_row) or 0) >= 4
    )

    if is_reasonable_splash:
        score += weights["splash_penalty"]

        if off_primary_colors[0] in splash_colors:
            score += 2.0

        if profile.get("fixing_land_count", 0) > 0:
            score += min(3.0, profile.get("fixing_land_count", 0) * 0.85)

        return score

    score += weights["off_color_penalty"] * max(1, len(off_primary_colors))

    if off_color_pips >= 2:
        score -= 3.0

    return score


def bot_get_curve_score(card_row, profile, draft_context):
    if bot_is_land(card_row):
        return 0.0

    mana_value = bot_get_card_mana_value(card_row)

    if mana_value is None:
        return 0.0

    stage = profile.get("stage") or "early"
    weights = bot_get_stage_weights(stage)
    curve_weight = weights["curve_weight"]
    mana_curve = profile.get("mana_curve") or {}

    score = 0.0

    two_drops = int(mana_curve.get("2") or 0)
    three_drops = int(mana_curve.get("3") or 0)
    six_plus = int(mana_curve.get("6+") or 0)

    if mana_value == 2:
        if two_drops < 4:
            score += (4 - two_drops) * 0.85 * curve_weight
        else:
            score += 0.35 * curve_weight

    elif mana_value == 3:
        if three_drops < 4:
            score += (4 - three_drops) * 0.65 * curve_weight
        else:
            score += 0.25 * curve_weight

    elif mana_value >= 6:
        if six_plus >= 2:
            score -= (six_plus - 1) * 1.10 * curve_weight
        else:
            score -= 0.30 * curve_weight

    elif mana_value <= 1:
        score -= 0.25 * curve_weight

    return score


def bot_get_deck_shape_score(card_row, profile, draft_context):
    if bot_is_land(card_row):
        return 0.0

    stage = profile.get("stage") or "early"
    weights = bot_get_stage_weights(stage)
    shape_weight = weights["deck_shape_weight"]

    creature_count = int(profile.get("creature_count") or 0)
    drafted_nonland_count = int(profile.get("drafted_nonland_count") or 0)

    score = 0.0

    if bot_is_creature(card_row):
        if creature_count < 14:
            score += min(3.0, (14 - creature_count) * 0.22) * shape_weight
        elif creature_count >= 18:
            score -= 1.0 * shape_weight
    else:
        if drafted_nonland_count >= 12 and creature_count < 9:
            score -= 1.25 * shape_weight

    return score


def bot_get_fixing_score(card_row, profile, draft_context):
    if bot_is_basic_land(card_row):
        return -999.0

    if not bot_is_land(card_row):
        return 0.0

    stage = profile.get("stage") or "early"
    draft_context = draft_context or {}
    pick_number = int(draft_context.get("pick_number") or 1)

    card_colors = bot_get_card_color_identity(card_row)
    primary_colors = profile.get("primary_colors") or []
    splash_colors = profile.get("splash_colors") or []
    identity_type = profile.get("identity_type") or "open"
    nonbasic_land_count = int(profile.get("nonbasic_land_count") or 0)
    fixing_land_count = int(profile.get("fixing_land_count") or 0)

    if not card_colors:
        # Colorless utility lands are okay if not over-picked, but not a priority.
        if nonbasic_land_count >= 3 and identity_type not in {"tri", "five"}:
            return -4.0

        return 0.25

    matching_primary = [
        color_code
        for color_code in card_colors
        if color_code in primary_colors
    ]

    matching_splash = [
        color_code
        for color_code in card_colors
        if color_code in splash_colors
    ]

    if not matching_primary and not matching_splash:
        return -4.0

    score = 0.0

    if matching_primary:
        score += 1.75 * len(matching_primary)

    if matching_splash:
        score += 2.25 * len(matching_splash)

    if len(card_colors) >= 2:
        score += 1.0

    if identity_type == "tri":
        score += 2.0
    elif identity_type == "dual" and fixing_land_count >= 3:
        score -= 7.0
    elif identity_type == "mono" and fixing_land_count >= 2:
        score -= 6.0

    if nonbasic_land_count >= 3 and identity_type not in {"tri", "five"}:
        score -= 7.0

    # In the first few picks, lands should almost never beat strong spells.
    # They are fixing, not core deck quality.
    if stage == "early" and pick_number <= 4:
        score -= 5.0

    return score


def bot_score_card(card_row, profile, draft_context):
    if bot_is_basic_land(card_row):
        return {
            "score": -999.0,
            "reason": "Basic land ignored; lands come from infinite deckbuilding pool.",
            "components": {
                "basic_land": -999.0,
            },
        }

    if bot_is_land(card_row):
        quality_score = 0.0
        color_fit_score = 0.0
        curve_score = 0.0
        deck_shape_score = 0.0
        fixing_score = bot_get_fixing_score(card_row, profile, draft_context)
    else:
        quality_score = bot_get_base_quality_score(card_row)
        color_fit_score = bot_get_color_fit_score(card_row, profile, draft_context)
        curve_score = bot_get_curve_score(card_row, profile, draft_context)
        deck_shape_score = bot_get_deck_shape_score(card_row, profile, draft_context)
        fixing_score = bot_get_fixing_score(card_row, profile, draft_context)

    stage = profile.get("stage") or "early"
    weights = bot_get_stage_weights(stage)
    noise_score = random.random() * weights["noise"]

    total_score = (
        quality_score
        + color_fit_score
        + curve_score
        + deck_shape_score
        + fixing_score
        + noise_score
    )

    return {
        "score": total_score,
        "reason": (
            f"stage={stage}; identity={profile.get('identity_type')}; "
            f"colors={'/'.join(profile.get('primary_colors') or []) or 'open'}; "
            f"quality={quality_score:.2f}; color={color_fit_score:.2f}; "
            f"curve={curve_score:.2f}; shape={deck_shape_score:.2f}; "
            f"fixing={fixing_score:.2f}"
        ),
        "components": {
            "quality": quality_score,
            "color_fit": color_fit_score,
            "curve": curve_score,
            "deck_shape": deck_shape_score,
            "fixing": fixing_score,
            "noise": noise_score,
        },
    }


def choose_bot_draft_pick(available_cards, drafted_cards, player_row, draft_context):
    available_cards = list(available_cards or [])

    if not available_cards:
        return None

    profile = bot_build_draft_profile(
        drafted_cards=drafted_cards or [],
        player_row=player_row,
        draft_context=draft_context or {},
    )

    best_card = None
    best_score = None
    best_result = None

    for card_row in available_cards:
        score_result = bot_score_card(
            card_row=card_row,
            profile=profile,
            draft_context=draft_context or {},
        )

        score = score_result["score"]

        if best_card is None or score > best_score:
            best_card = card_row
            best_score = score
            best_result = score_result

    if best_card is None:
        return None

    return {
        "card": best_card,
        "score": best_score,
        "reason": best_result.get("reason", "") if best_result else "",
        "components": best_result.get("components", {}) if best_result else {},
        "profile": profile,
    }