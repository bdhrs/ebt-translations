"""Sutta ID Normalization Utility.

Handles all variations:
- DN1, DN 1, dn1, dn-1 -> dn1
- SN56.11, sn56_11, sn56-11 -> sn56.11
- AN3.45, an3_45, an3-45 -> an3.45
"""

import re
from typing import Optional


def normalize_sutta_id(sutta_id: str) -> str:
    """Normalize sutta ID to canonical format.
    
    Args:
        sutta_id: Raw sutta ID (any format)
        
    Returns:
        Normalized sutta ID (e.g., 'dn1', 'sn56.11', 'an3.45')
    """
    if not sutta_id:
        return ""
    
    # Convert to lowercase
    sid = sutta_id.strip().lower()
    
    # Remove extra spaces
    sid = re.sub(r'\s+', '', sid)
    
    # Remove dashes (except the decimal point pattern)
    sid = sid.replace('-', '')
    
    # Replace underscores with nothing
    sid = sid.replace('_', '')
    
    # Now parse by nikaya prefix
    # DN: dn1, dn01, dn001 -> dn1
    dn_match = re.match(r'^dn0*(\d+)$', sid)
    if dn_match:
        return f"dn{dn_match.group(1)}"
    
    # MN: mn1, mn01 -> mn1
    mn_match = re.match(r'^mn0*(\d+)$', sid)
    if mn_match:
        return f"mn{mn_match.group(1)}"
    
    # SN: sn1.1, sn01.01 -> sn1.1 (keep decimal)
    sn_match = re.match(r'^sn0*(\d+)\.?(\d*)$', sid)
    if sn_match:
        num = int(sn_match.group(1))
        sub = sn_match.group(2)
        if sub:
            return f"sn{num}.{sub}"
        return f"sn{num}"
    
    # AN: an1.1, an01.01 -> an1.1
    an_match = re.match(r'^an0*(\d+)\.?(\d*)$', sid)
    if an_match:
        num = int(an_match.group(1))
        sub = an_match.group(2)
        if sub:
            return f"an{num}.{sub}"
        return f"an{num}"
    
    # KN sub-collections
    # Dhammapada: dhp1 -> dhp1
    dhp_match = re.match(r'^dhp0*(\d+)$', sid)
    if dhp_match:
        return f"dhp{dhp_match.group(1)}"
    
    # Itivuttaka: iti1 -> iti1
    iti_match = re.match(r'^iti0*(\d+)$', sid)
    if iti_match:
        return f"iti{iti_match.group(1)}"
    
    # Sutta Nipata: snp1 -> snp1
    snp_match = re.match(r'^snp0*(\d+)$', sid)
    if snp_match:
        return f"snp{snp_match.group(1)}"
    
    # Theragatha: thag1 -> thag1
    thag_match = re.match(r'^thag0*(\d+)$', sid)
    if thag_match:
        return f"thag{thag_match.group(1)}"
    
    # Therigatha: thig1 -> thig1
    thig_match = re.match(r'^thig0*(\d+)$', sid)
    if thig_match:
        return f"thig{thig_match.group(1)}"
    
    # Udana: ud1 -> ud1
    ud_match = re.match(r'^ud0*(\d+)$', sid)
    if ud_match:
        return f"ud{ud_match.group(1)}"
    
    # Khuddakapatha: kp1 -> kp1
    kp_match = re.match(r'^kp0*(\d+)$', sid)
    if kp_match:
        return f"kp{kp_match.group(1)}"
    
    # Return as-is if no pattern matched
    return sid


def normalize_batch(sutta_ids: list[str]) -> list[str]:
    """Normalize multiple sutta IDs.
    
    Args:
        sutta_ids: List of raw sutta IDs
        
    Returns:
        List of normalized IDs (same order)
    """
    return [normalize_sutta_id(sid) for sid in sutta_ids]


def get_nikaya(sutta_id: str) -> str:
    """Determine nikaya from sutta ID.
    
    Args:
        sutta_id: Sutta ID (normalized or raw)
        
    Returns:
        Nikaya code: 'dn', 'mn', 'sn', 'an', 'kn'
    """
    sid = normalize_sutta_id(sutta_id)
    
    if sid.startswith('dn'):
        return 'dn'
    elif sid.startswith('mn'):
        return 'mn'
    elif sid.startswith('sn'):
        return 'sn'
    elif sid.startswith('an'):
        return 'an'
    else:
        return 'kn'


def is_valid_sutta_id(sutta_id: str) -> bool:
    """Check if sutta ID is valid format.
    
    Args:
        sutta_id: Sutta ID to validate
        
    Returns:
        True if valid format
    """
    normalized = normalize_sutta_id(sutta_id)
    
    # Basic pattern check
    pattern = r'^(dn|mn|sn|an|dhp|iti|snp|thag|thig|ud|kp)\d+(\.\d+)?$'
    return bool(re.match(pattern, normalized))


# Test utility when run directly
if __name__ == "__main__":
    test_cases = [
        ("DN1", "dn1"),
        ("DN 1", "dn1"),
        ("dn1", "dn1"),
        ("dn-1", "dn1"),
        ("sn56.11", "sn56.11"),
        ("sn56_11", "sn56.11"),
        ("SN056.011", "sn56.11"),
        ("an3.45", "an3.45"),
        ("an3_45", "an3.45"),
        ("AN003.045", "an3.45"),
        ("dhp1", "dhp1"),
        ("DHP001", "dhp1"),
    ]
    
    print("Sutta ID Normalization Tests:")
    for raw, expected in test_cases:
        result = normalize_sutta_id(raw)
        status = "✓" if result == expected else f"✗ (got {result})"
        print(f"  {raw:15} -> {expected:10} {status}")