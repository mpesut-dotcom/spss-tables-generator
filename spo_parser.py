"""
SPO file parser — extracts SPSS output metadata from .spo (OLE2) files.

Parses:
  - SAV filename used to produce the output
  - Filter conditions (TEMPORARY / SELECT IF / FILTER)
  - CTABLES / CROSSTABS / FREQUENCIES syntax blocks
  - Table structure: type (total/krizanje), row vars, banner var, MR, numeric
"""
import re
import os
import olefile


# ═══════════════════════════════════════════════════════════════════
#  Low-level binary extraction
# ═══════════════════════════════════════════════════════════════════

def _extract_text_runs(data, min_len=6):
    """Extract sequences of printable characters from OLE binary data."""
    text = data.decode('latin-1', errors='replace')
    runs = []
    current = []
    for i, c in enumerate(text):
        if c.isprintable() or c in '\r\n\t':
            current.append(c)
        else:
            if len(current) >= min_len:
                s = ''.join(current).strip()
                if s:
                    runs.append(s)
            current = []
    if current and len(current) >= min_len:
        s = ''.join(current).strip()
        if s:
            runs.append(s)
    return runs


# ═══════════════════════════════════════════════════════════════════
#  SPO file parsing
# ═══════════════════════════════════════════════════════════════════

def parse_spo(filepath):
    """
    Parse an SPSS .spo file, returning structured metadata.

    Returns dict:
        sav_name:   str | None — basename of the .sav file used
        filters:    list[str] — filter expressions (e.g. 'q32=1')
        tables:     list[dict] — parsed table definitions, each with:
            type:           'total' | 'krizanje'
            row_vars:       list[str]   (single-var tables)
            banner_var:     str | None
            is_mr:          bool
            mr_vars:        list[str]   (MR sub-variables)
            is_numeric:     bool
            statistics:     list[str]
            filter_expr:    str | None  (filter applied to THIS table specifically)
    """
    ole = olefile.OleFileIO(filepath)
    data = ole.openstream('Contents').read()
    ole.close()

    runs = _extract_text_runs(data)

    # ── SAV filename ──
    sav_name = None
    for run in runs:
        m = re.search(r'([A-Za-z0-9_\-]+\.sav)', run, re.IGNORECASE)
        if m:
            sav_name = m.group(1)
            break  # first occurrence is enough

    # ── Filter conditions: "expr (FILTER)" ──
    all_filters = []
    for run in runs:
        m = re.match(r'[&]?(.+?)\s*\(FILTER\)\s*$', run, re.IGNORECASE)
        if m:
            filt_expr = m.group(1).strip()
            if filt_expr not in all_filters:
                all_filters.append(filt_expr)

    # ── CTABLES syntax blocks ──
    # These contain '/FORMAT', '/FTOTAL', or '/STATISTICS'
    ctables_blocks = []
    for run in runs:
        if 'TABLES' in run and ('/FORMAT' in run or '/FTOTAL' in run or '/STATISTICS' in run):
            ctables_blocks.append(run)

    # ── CROSSTABS syntax blocks ──
    # Format: ...CROSSTABS\r  /TABLES=var BY var\r  /CELLS=COUNT COLUMN.
    # The run may have a length-prefix byte char at the start (e.g. '9')
    crosstabs_blocks = []
    for run in runs:
        m = re.search(r'CROSSTABS\s+/TABLES=(\w+)\s+BY\s+(\w+)', run, re.IGNORECASE)
        if m:
            crosstabs_blocks.append(run)

    # ── FREQUENCIES syntax blocks ──
    freq_blocks = []
    for run in runs:
        m = re.search(r'FREQUENCIES\s+VARIABLES=([\w\s]+)', run, re.IGNORECASE)
        if m:
            freq_blocks.append(run)

    # ── Parse all blocks into table definitions ──
    tables = []
    seen_keys = set()  # deduplicate (syntax appears twice in SPO)

    # Assign filters to tables.
    # Heuristic: if there are N unique CTABLES blocks and N unique filters,
    # they correspond 1-to-1.  If there's only 1 unique filter, it's global.
    # Otherwise map by order of appearance.
    unique_ctables = _deduplicate_blocks(ctables_blocks)
    unique_crosstabs = _deduplicate_blocks(crosstabs_blocks)
    unique_freq = _deduplicate_blocks(freq_blocks)

    # Determine filter assignment strategy
    filter_map = _build_filter_map(all_filters, unique_ctables, data)

    for i, ct in enumerate(unique_ctables):
        parsed = _parse_ctables_syntax(ct)
        dedup_key = (parsed['type'], tuple(parsed['row_vars']),
                     tuple(parsed.get('mr_vars', [])), parsed['banner_var'],
                     parsed['is_numeric'])
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)
        parsed['filter_expr'] = filter_map.get(i)
        tables.append(parsed)

    for cr_block in unique_crosstabs:
        parsed = _parse_crosstabs_syntax(cr_block)
        if parsed:
            dedup_key = (parsed['type'], tuple(parsed['row_vars']),
                         (), parsed['banner_var'], False)
            if dedup_key not in seen_keys:
                seen_keys.add(dedup_key)
                # CROSSTABS files typically have one global filter
                parsed['filter_expr'] = all_filters[0] if len(all_filters) == 1 else None
                tables.append(parsed)

    for fr_block in unique_freq:
        parsed = _parse_frequencies_syntax(fr_block)
        if parsed:
            dedup_key = ('freq', tuple(parsed['row_vars']), (), None, False)
            if dedup_key not in seen_keys:
                seen_keys.add(dedup_key)
                parsed['filter_expr'] = all_filters[0] if len(all_filters) == 1 else None
                tables.append(parsed)

    return {
        'sav_name': sav_name,
        'filters': all_filters,
        'tables': tables,
        'filename': os.path.basename(filepath),
    }


def _deduplicate_blocks(blocks):
    """Deduplicate syntax blocks (each appears twice in SPO — Notes + NavNote)."""
    seen = set()
    result = []
    for b in blocks:
        key = b[:120]
        if key not in seen:
            seen.add(key)
            result.append(b)
    return result


def _build_filter_map(filters, ctables_blocks, raw_data):
    """
    Map filter expressions to the CTABLES block they precede.
    In SPO binary, filter value appears right before its CTABLES syntax.
    If only 1 unique filter → it applies to ALL tables (global).
    """
    if not filters:
        return {}

    unique_filters = list(dict.fromkeys(filters))  # preserve order, deduplicate
    if len(unique_filters) == 1:
        # Single filter → global, applies to all tables
        return {i: unique_filters[0] for i in range(len(ctables_blocks))}

    # Multiple unique filters — try to pair by position in binary data
    text = raw_data.decode('latin-1', errors='replace')
    filter_positions = []
    for filt in unique_filters:
        pos = text.find(filt)
        if pos >= 0:
            filter_positions.append((pos, filt))
    filter_positions.sort()

    ctables_positions = []
    for i, block in enumerate(ctables_blocks):
        pos = text.find(block[:60])
        if pos >= 0:
            ctables_positions.append((pos, i))
    ctables_positions.sort()

    # Map: for each CTABLES block, find the closest preceding filter
    fmap = {}
    for ct_pos, ct_idx in ctables_positions:
        best = None
        for f_pos, f_expr in filter_positions:
            if f_pos < ct_pos:
                best = f_expr
            else:
                break
        if best:
            fmap[ct_idx] = best
    return fmap


# ═══════════════════════════════════════════════════════════════════
#  Syntax parsers
# ═══════════════════════════════════════════════════════════════════

def _parse_ctables_syntax(syntax):
    """Parse a CTABLES syntax block into a table definition dict."""
    result = {
        'type': None,
        'row_vars': [],
        'banner_var': None,
        'is_mr': False,
        'mr_vars': [],
        'is_numeric': False,
        'observation_var': None,
        'statistics': [],
        'filter_expr': None,
    }

    # /OBSERVATION var → numeric table
    obs_m = re.search(r'/OBSERVATION\s+(\w+)', syntax, re.IGNORECASE)
    if obs_m:
        result['is_numeric'] = True
        result['observation_var'] = obs_m.group(1)

    # /MRGROUP $name '' var1 var2 ... → multi-response
    mr_m = re.search(r"/MRGROUP\s+\$\w+\s+'[^']*'\s+([\w\s]+?)(?=\s*/)", syntax, re.IGNORECASE)
    if mr_m:
        result['is_mr'] = True
        result['mr_vars'] = mr_m.group(1).strip().split()

    # Parse /TABLES or /TABLE or /Tabela clause
    tables_m = re.search(
        r'/(?:TABLES?|Tabela)\s*[=\s]\s*(.+?)(?=\s*/(?:STATISTICS|TITLE))',
        syntax, re.IGNORECASE | re.DOTALL
    )
    if tables_m:
        tables_clause = tables_m.group(1).strip()
        by_m = re.search(r'\bBY\b\s+(.+)', tables_clause, re.IGNORECASE)
        if by_m:
            by_part = by_m.group(1).strip()
            before_by = tables_clause[:by_m.start()].strip()

            # Is it total (BY (STATISTICS)) or cross (BY banner_var...)?
            if re.match(r'[\(\s]*STATISTICS[\)\s]*$', by_part, re.IGNORECASE):
                result['type'] = 'total'
            else:
                result['type'] = 'krizanje'
                # Extract banner vars: all words in BY clause that are
                # not SPSS keywords, $-references, or parenthesized blocks
                clean_by = re.sub(r'[()>+]', ' ', by_part)
                by_words = clean_by.split()
                # Skip SPSS tokens: $t, $t1, $t2, $e1, STATISTICS, etc.
                _skip = {'statistics', 'by'}
                banner_vars = [w for w in by_words
                               if not w.startswith('$')
                               and w.lower() not in _skip]
                if banner_vars:
                    # Last non-keyword var is typically the real banner
                    # (e.g. "total +qzemlja" → qzemlja is banner, total is a row aggregator)
                    result['banner_var'] = banner_vars[-1]

            # Extract row variables from before BY
            if not result['is_mr']:
                clean = re.sub(r'[()$+]', ' ', before_by)
                words = clean.split()
                result['row_vars'] = [w for w in words
                                      if w.lower() not in ('t', 'statistics')
                                      and not w.startswith('$')]

    # Statistics
    stats = re.findall(
        r'\b(count|cpct|mean|stddev|median|minimum|maximum|validn)\b',
        syntax, re.IGNORECASE
    )
    result['statistics'] = list(set(s.lower() for s in stats))

    # Determine type for MR if not yet set
    if result['type'] is None:
        result['type'] = 'total'

    return result


def _parse_crosstabs_syntax(syntax):
    """Parse CROSSTABS /TABLES=var BY var /CELLS=..."""
    m = re.search(r'CROSSTABS\s+/TABLES=(\w+)\s+BY\s+(\w+)', syntax, re.IGNORECASE)
    if m:
        return {
            'type': 'krizanje',
            'row_vars': [m.group(1)],
            'banner_var': m.group(2),
            'is_mr': False,
            'mr_vars': [],
            'is_numeric': False,
            'observation_var': None,
            'statistics': ['count', 'cpct'],
            'filter_expr': None,
        }
    return None


def _parse_frequencies_syntax(syntax):
    """Parse FREQUENCIES VARIABLES=var1 var2 ..."""
    m = re.search(r'FREQUENCIES\s+VARIABLES=([\w\s]+?)(?:\s*/|\s*\.)', syntax, re.IGNORECASE)
    if m:
        var_list = m.group(1).strip().split()
        return {
            'type': 'total',
            'row_vars': var_list,
            'banner_var': None,
            'is_mr': False,
            'mr_vars': [],
            'is_numeric': False,
            'observation_var': None,
            'statistics': ['count', 'cpct'],
            'filter_expr': None,
        }
    return None


# ═══════════════════════════════════════════════════════════════════
#  SAV name matching
# ═══════════════════════════════════════════════════════════════════

def sav_names_match(spo_sav_name, loaded_sav_name):
    """
    Check if the SAV name from SPO is compatible with the loaded SAV.

    Handles version differences:
      Data_online_26_01_009_brand_tracker_skupni_v7_ansi.sav
      matches
      Data_online_26_01_009_brand_tracker_skupni_v6.sav

    Strategy: strip version suffix (_v\\d+[_a-zA-Z]*) and compare the base.
    """
    if not spo_sav_name or not loaded_sav_name:
        return False

    def normalize(name):
        # Remove .sav extension
        base = re.sub(r'\.sav$', '', name, flags=re.IGNORECASE)
        # Remove version suffix: _v7_ansi, _v6, _v1, etc.
        base = re.sub(r'_v\d+[_a-zA-Z]*$', '', base, flags=re.IGNORECASE)
        return base.lower()

    return normalize(spo_sav_name) == normalize(loaded_sav_name)


# ═══════════════════════════════════════════════════════════════════
#  Filter expression parsing
# ═══════════════════════════════════════════════════════════════════

def parse_filter_expression(expr):
    """
    Parse SPSS filter expression into structured filter conditions.

    Examples:
        'q32=1'                         → [{'var': 'q32', 'vals': [1.0]}]
        'q12_3=1 or q12_3=2 or q12_3=3' → [{'var': 'q12_3', 'vals': [1.0, 2.0, 3.0]}]
        'q8_9=9'                        → [{'var': 'q8_9', 'vals': [9.0]}]

    Returns list of dicts with 'var' and 'vals' keys.
    """
    if not expr:
        return []

    # Split on 'or' (case-insensitive)
    parts = re.split(r'\bor\b', expr, flags=re.IGNORECASE)

    # Group by variable
    conditions = {}
    for part in parts:
        part = part.strip()
        m = re.match(r'(\w+)\s*=\s*([0-9.]+)', part)
        if m:
            var = m.group(1)
            val = float(m.group(2))
            if val == int(val):
                val = int(val)
            conditions.setdefault(var, []).append(val)

    return [{'var': var, 'vals': vals} for var, vals in conditions.items()]


# ═══════════════════════════════════════════════════════════════════
#  High-level: match SPO tables to input.txt tables
# ═══════════════════════════════════════════════════════════════════

def match_spo_to_input(spo_result, titles, variables, df_columns):
    """
    Match SPO table definitions to input.txt table indices.

    For each SPO table:
      - If row_vars → find input table(s) whose variables include those vars
      - If mr_vars → find input table(s) whose MR group matches
      - Return matched table indices and any unmatched tables

    Returns list of dicts, one per SPO table:
        {
            'spo_table': dict,          # original parsed table
            'matched_indices': [int],   # input.txt table indices (0-based)
            'match_status': 'ok' | 'partial' | 'no_match',
            'reason': str,              # human-readable status description
        }
    """
    from spss_tables import get_table_type, get_table_title, parse_mr_vars

    col_lc = {c.lower(): c for c in df_columns}
    results = []

    for tbl in spo_result['tables']:
        match_info = {
            'spo_table': tbl,
            'matched_indices': [],
            'match_status': 'no_match',
            'reason': '',
        }

        # Determine which variables to search for
        if tbl['is_mr']:
            search_vars = set(v.lower() for v in tbl['mr_vars'])
        else:
            search_vars = set(v.lower() for v in tbl['row_vars'])

        if not search_vars:
            match_info['reason'] = 'Nema varijabli za pretragu u SPO bloku'
            results.append(match_info)
            continue

        # Check if vars exist in loaded datafile
        missing_vars = [v for v in search_vars if v not in col_lc]
        if missing_vars:
            match_info['reason'] = f'Varijable ne postoje u datasetu: {", ".join(missing_vars[:5])}'
            match_info['match_status'] = 'no_match'
            results.append(match_info)
            continue

        # Search input.txt tables
        matched = []
        for ti, (title_line, var_line) in enumerate(zip(titles, variables)):
            ttype = get_table_type(title_line)
            if ttype not in ('s', 'k', 'd', 'n', 'm'):
                continue

            # Extract variables from this input table
            var_line_s = var_line.strip()
            if var_line_s.startswith('$'):
                # MR: $e1 '' var1 var2 var3
                input_vars = set(v.lower() for v in parse_mr_vars(var_line_s))
            elif '+' in var_line_s:
                # Numeric: var1 var2 var1+var2
                input_vars = set(v.lower() for v in var_line_s.split() if '+' not in v)
            else:
                input_vars = set(v.lower() for v in var_line_s.split() if v)

            # Check overlap
            if tbl['is_mr']:
                # For MR: input vars must be a subset of or match SPO mr_vars
                if input_vars and input_vars.issubset(search_vars):
                    matched.append(ti)
                elif input_vars and search_vars.issubset(input_vars):
                    matched.append(ti)
            elif tbl['is_numeric']:
                # Numeric: match observation var or row_vars
                if input_vars and search_vars.issubset(input_vars):
                    matched.append(ti)
                elif tbl.get('observation_var') and tbl['observation_var'].lower() in input_vars:
                    matched.append(ti)
            else:
                # Single var table
                if input_vars == search_vars:
                    matched.append(ti)
                elif len(search_vars) == 1 and search_vars.issubset(input_vars):
                    matched.append(ti)

        if matched:
            match_info['matched_indices'] = matched
            match_info['match_status'] = 'ok'
            match_info['reason'] = f'Pronađeno {len(matched)} tablica'
        else:
            match_info['match_status'] = 'df_only'
            match_info['reason'] = f'Varijable postoje u datasetu ali ne u input.txt — bit će dodane automatski'

        results.append(match_info)

    return results
