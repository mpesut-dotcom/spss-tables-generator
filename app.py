#!/usr/bin/env python3
"""
SPSS Tables — Web aplikacija
=============================
Korisničko sučelje za generiranje Excel tablica iz SPSS .sav podataka.

Pokretanje:
    streamlit run app.py

Potrebni paketi:
    pip install streamlit pyreadstat pandas openpyxl
"""

import io
import json
import os
import re
import tempfile
import time

import numpy as np
import pandas as pd
import pyreadstat
import streamlit as st
from openpyxl import load_workbook

# Importaj engine funkcije iz spss_tables.py
from spss_tables import (
    build_column_map,
    compute_sig_total_banner,
    get_table_title,
    get_table_type,
    make_crosstab_mr,
    make_crosstab_numeric,
    make_crosstab_simple,
    make_freq_table,
    make_mr_table,
    make_numeric_table,
    make_simple_table,
    merge_crosstabs_banner,
    write_banner_to_sheet,
    write_tables_to_excel,
)


# ═══════════════════════════════════════════════════════════════════
#  PARSIRANJE INPUT-a IZ UPLOADANOG FAJLA (bytes, ne filepath)
# ═══════════════════════════════════════════════════════════════════

def parse_input_bytes(raw_bytes):
    """Parsira uploadani input.txt iz sirovih bajtova."""
    for enc in ('utf-8', 'cp1250', 'latin-1'):
        try:
            content = raw_bytes.decode(enc)
            break
        except (UnicodeDecodeError, AttributeError):
            continue
    else:
        content = raw_bytes.decode('utf-8', errors='replace')

    lines = content.replace('\r\n', '\n').split('\n')

    sections = []
    current = []
    for line in lines:
        if line.strip() == '':
            if current:
                sections.append(current)
                current = []
        else:
            current.append(line)
    if current:
        sections.append(current)

    if len(sections) < 3:
        raise ValueError(
            f"Input fajl mora imati 3 sekcije razdvojene blank linijama, "
            f"pronađeno: {len(sections)}"
        )

    return sections[0], sections[1], sections[2]


# ═══════════════════════════════════════════════════════════════════
#  GRUPIRANJE VARIJABLI IZ INPUT DEFINICIJA
# ═══════════════════════════════════════════════════════════════════

def _extract_group_key(title):
    """Izvuci ključ grupe iz naslova tablice (npr. 'q1' iz 'q1.2. Niže su...')."""
    title = title.strip()
    # Pokušaj matchati pitanje s brojem: q1, q2, r1, q22, dwork, deduc, ...
    m = re.match(r'^([a-zA-Z]+\d+)', title)
    if m:
        return m.group(1).lower()
    # Demografske varijable (Spol:, Dob:, Regija:, ...) → grupa "demo"
    if ':' in title.split('.')[0]:
        return '_demo_'
    # Fallback: prva riječ
    return title.split('.')[0].split(':')[0].split()[0].lower() if title else '_ostalo_'


def _extract_vars_from_line(var_line):
    """Izvuci pojedinačne varijable iz var definicijskog reda."""
    var_line = var_line.strip()
    # MR: $e1 '' var1 var2 var3
    if var_line.startswith('$'):
        return [p for p in var_line.split() if not p.startswith('$') and p != "''"]
    # Numeric composite: q1_1 q1_2 ... q1_1+q1_2+...
    if '+' in var_line:
        return [p for p in var_line.split() if '+' not in p]
    # Simple single var
    return [var_line] if var_line else []


def build_variable_groups(titles, variables, df_columns):
    """
    Gradi hijerarhijsku strukturu varijabli iz input definicija.
    Vraća: OrderedDict { group_key: { 'label': str, 'vars': [str], 'types': set } }
    """
    from collections import OrderedDict
    groups = OrderedDict()
    col_set = {c.lower(): c for c in df_columns}

    for title_line, var_line in zip(titles, variables):
        table_title = get_table_title(title_line)
        table_type = get_table_type(title_line)
        group_key = _extract_group_key(table_title)

        actual_vars = _extract_vars_from_line(var_line)

        if group_key not in groups:
            # Kreiraj label iz prvog naslova u grupi
            if group_key == '_demo_':
                label = '📋 Demografija'
            else:
                # Skrati na zajednički dio pitanja
                base = table_title.split(' - ')[0].strip()
                # Ako ima sub-item (q1.1.), uzmi dio do prvog sub-broja
                m = re.match(r'^([a-zA-Z]+\d+)\.\d+\.\s*(.+)', base)
                if m:
                    label = f"{m.group(1)}. {m.group(2)[:55]}"
                else:
                    label = base[:65]
            groups[group_key] = {'label': label, 'vars': [], 'types': set()}

        groups[group_key]['types'].add(table_type)

        for v in actual_vars:
            resolved = col_set.get(v.lower())
            if resolved and resolved not in groups[group_key]['vars']:
                groups[group_key]['vars'].append(resolved)

    # Dodaj "Ostale" grupu za varijable koje nisu u input.txt
    used = set()
    for g in groups.values():
        used.update(g['vars'])
    other = [c for c in df_columns if c not in used]
    if other:
        groups['_ostalo_'] = {'label': '📦 Ostale varijable', 'vars': other, 'types': set()}

    # Makni prazne grupe
    groups = OrderedDict((k, v) for k, v in groups.items() if v['vars'])

    return groups


# ═══════════════════════════════════════════════════════════════════
#  GENERIRANJE TABLICA (engine wrapper)
# ═══════════════════════════════════════════════════════════════════

def generate_tables(df, meta, titles, variables, weight_col, start_num):
    """Generira sve tablice. Vraća (tables_list, errors_list)."""
    col_map = build_column_map(df)
    count = min(len(titles), len(variables))
    tables = []
    errors = []

    for i in range(count):
        title_line = titles[i]
        var_line = variables[i]
        table_type = get_table_type(title_line)
        table_title = get_table_title(title_line)
        table_num = i + start_num
        title_str = f"{table_title} (Table {table_num}.1)"

        try:
            if table_type == 's':
                result = make_simple_table(df, var_line.strip(), meta, col_map, weight_col)
            elif table_type in ('k', 'd'):
                result = make_mr_table(df, var_line, meta, col_map, table_type, weight_col)
            elif table_type == 'n':
                result = make_numeric_table(df, var_line, meta, col_map, full_stats=True, weight_col=weight_col)
            elif table_type == 'm':
                result = make_numeric_table(df, var_line, meta, col_map, full_stats=False, weight_col=weight_col)
            elif table_type == 'f':
                result = make_freq_table(df, var_line, meta, col_map, weight_col)
            else:
                errors.append(f"Tablica {table_num}: nepoznat tip '{table_type}'")
                continue

            result['title'] = title_str
            tables.append(result)
        except KeyError as e:
            errors.append(f"Tablica {table_num}: varijabla {e} ne postoji u podatcima")
        except Exception as e:
            errors.append(f"Tablica {table_num}: {e}")

    return tables, errors


# ═══════════════════════════════════════════════════════════════════
#  FILTER LOGIC
# ═══════════════════════════════════════════════════════════════════

def apply_filter_groups(df, filter_groups):
    """
    Primijeni filter grupe na DataFrame.
    Svaka grupa ima:
      - 'mode': 'single' (jedna varijabla, filtrira po vrijednostima)
                ili 'multi' (grupa varijabli, filtrira po odabranim sub-varijablama)
      - 'logic': 'AND' ili 'OR' (veznik s prethodnom grupom)
    """
    if not filter_groups:
        return df

    try:
        combined_mask = None

        for grp in filter_groups:
            vals = grp.get('vals', [])
            if not vals:
                continue

            mode = grp.get('mode', 'single')
            logic = grp.get('logic', 'AND')

            if mode == 'multi':
                # vals su imena sub-varijabli; ispitanik prolazi ako ima
                # ne-NaN i != 0 u barem jednoj od odabranih sub-varijabli
                grp_mask = pd.Series(False, index=df.index)
                for sv in vals:
                    if sv in df.columns:
                        grp_mask = grp_mask | (df[sv].notna() & (df[sv] != 0))
            else:
                # single: vals su vrijednosti jedne varijable
                var = grp['var']
                cmp_vals = []
                for v in vals:
                    try:
                        cmp_vals.append(float(v))
                    except (ValueError, TypeError):
                        cmp_vals.append(v)
                grp_mask = df[var].isin(cmp_vals)

            if combined_mask is None:
                combined_mask = grp_mask
            elif logic == 'OR':
                combined_mask = combined_mask | grp_mask
            else:
                combined_mask = combined_mask & grp_mask

        if combined_mask is None:
            return df
        return df[combined_mask].copy()
    except Exception:
        return df


def build_filter_groups_description(filter_groups, labels_dict, val_labels_dict):
    """Napravi citljiv opis filter grupa."""
    parts = []
    for i, grp in enumerate(filter_groups):
        vals = grp.get('vals', [])
        if not vals:
            continue

        mode = grp.get('mode', 'single')
        logic = grp.get('logic', 'AND')
        group_label = grp.get('group_label', '')

        if mode == 'multi':
            val_parts = [labels_dict.get(sv, sv)[:40] for sv in vals]
            val_str = ' ILI '.join(val_parts)
            part = f"`{group_label[:35]}` = {val_str}"
        else:
            var = grp['var']
            var_lbl = labels_dict.get(var) or var
            short_var = var_lbl[:35] if var_lbl != var else var

            vlabels = val_labels_dict.get(var, {})
            val_parts = []
            for v in vals:
                lbl = vlabels.get(v, '')
                if not lbl:
                    try:
                        lbl = vlabels.get(float(v), '')
                    except (ValueError, TypeError):
                        pass
                if not lbl:
                    try:
                        lbl = vlabels.get(int(float(v)), '')
                    except (ValueError, TypeError):
                        pass
                val_parts.append(str(lbl) if lbl else str(v))

            val_str = ' ILI '.join(val_parts)
            part = f"`{short_var}` = {val_str}"

        if i > 0 and parts:
            connector = ' **ILI** ' if logic == 'OR' else ' **I** '
            parts.append(connector)
        parts.append(part)

    return ''.join(parts) if parts else ''


# ═══════════════════════════════════════════════════════════════════
#  PLAN OBRADE : save / load
# ═══════════════════════════════════════════════════════════════════

class _NumpyEncoder(json.JSONEncoder):
    """Handle numpy types when serializing to JSON."""
    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def collect_plan(output_defs, use_weight, weight_col, start_num):
    """Build a JSON-serializable dict from the current configuration."""
    plan = {
        'version': 1,
        'global': {
            'use_weight': bool(use_weight),
            'weight_col': weight_col,
            'start_num': int(start_num),
        },
        'outputs': [],
    }
    for oi, od in enumerate(output_defs):
        out = {
            'type': od['type'],
            'sheet_name': od['sheet_name'],
            'filter_groups': od.get('filter_groups', []),
        }
        if od['type'] == 'krizanje':
            out['banner_vars'] = od.get('banner_vars', [])
            out['show_sig'] = od.get('show_sig', False)
            out['show_sig_total'] = od.get('show_sig_total', False)
            out['table_indices'] = od.get('table_indices', [])
            out['table_mode'] = st.session_state.get(f'out_tblmode_{oi}', 'all')
        plan['outputs'].append(out)
    return plan


def _apply_plan_outputs(plan, cat_var_names, filter_choices,
                        all_tbl_indices, df, val_labels_dict):
    """Set session_state keys for outputs from a loaded plan."""
    outputs = plan.get('outputs', [])
    st.session_state['n_outputs'] = max(len(outputs), 1)

    for oi, out in enumerate(outputs):
        st.session_state[f'out_type_{oi}'] = out.get('type', 'total')
        st.session_state[f'out_name_{oi}'] = out.get('sheet_name', f'Output_{oi+1}')

        # ── Filters ──
        fg = out.get('filter_groups', [])
        st.session_state[f'out_filt_{oi}'] = bool(fg)
        if fg:
            st.session_state[f'n_fg_{oi}'] = len(fg)
            for fi, fgroup in enumerate(fg):
                if fi > 0:
                    logic_val = "ILI (OR)" if fgroup.get('logic') == 'OR' else "I (AND)"
                    st.session_state[f'fg_logic_{oi}_{fi}'] = logic_val

                if fgroup.get('mode') == 'multi':
                    saved_vars = set(fgroup.get('vals', []))
                    fc_idx = 0
                    for j, fc in enumerate(filter_choices):
                        if fc['mode'] == 'multi' and set(fc['vars']) == saved_vars:
                            fc_idx = j
                            break
                    st.session_state[f'fg_var_{oi}_{fi}'] = fc_idx
                    matched_fc = filter_choices[fc_idx] if filter_choices else {'vars': []}
                    val_idx = [matched_fc['vars'].index(v)
                               for v in fgroup['vals'] if v in matched_fc['vars']]
                    st.session_state[f'fg_vals_{oi}_{fi}'] = val_idx
                else:
                    the_var = fgroup.get('var', '')
                    fc_idx = 0
                    for j, fc in enumerate(filter_choices):
                        if fc['mode'] == 'single' and fc['vars'][0] == the_var:
                            fc_idx = j
                            break
                    st.session_state[f'fg_var_{oi}_{fi}'] = fc_idx
                    # Map saved values to indices in unique_vals
                    try:
                        unique_vals = sorted(
                            df[the_var].dropna().unique(),
                            key=lambda x: (0, float(x)) if isinstance(x, (int, float)) else (1, str(x))
                        )
                    except (TypeError, KeyError):
                        unique_vals = []
                    val_idx = []
                    for sv in fgroup.get('vals', []):
                        for k, uv in enumerate(unique_vals):
                            if uv == sv or str(uv) == str(sv):
                                val_idx.append(k)
                                break
                    st.session_state[f'fg_vals_{oi}_{fi}'] = val_idx

        # ── Križanje settings ──
        if out.get('type') == 'krizanje':
            banner_idx = [cat_var_names.index(v)
                          for v in out.get('banner_vars', []) if v in cat_var_names]
            st.session_state[f'out_banner_{oi}'] = banner_idx
            st.session_state[f'out_sig_{oi}'] = out.get('show_sig', True)
            st.session_state[f'out_sigtot_{oi}'] = out.get('show_sig_total', False)

            tbl_mode = out.get('table_mode', 'all')
            st.session_state[f'out_tblmode_{oi}'] = tbl_mode
            saved_set = set(out.get('table_indices', []))
            if tbl_mode == 'exclude':
                st.session_state[f'out_excl_{oi}'] = [
                    j for j, idx in enumerate(all_tbl_indices) if idx not in saved_set]
            elif tbl_mode == 'select':
                st.session_state[f'out_sel_{oi}'] = [
                    j for j, idx in enumerate(all_tbl_indices) if idx in saved_set]


# ═══════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ═══════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(
        page_title="SPSS Tables Generator",
        page_icon="📊",
        layout="wide",
    )

    # ── Header ──
    st.title("📊 SPSS Tables Generator")
    st.markdown(
        "Generirajte profesionalne Excel tablice iz SPSS podataka — "
        "bez potrebe za SPSS-om."
    )

    st.divider()

    # ══════════════════════════════════════════════════
    #  KORAK 1: Upload fajlova
    # ══════════════════════════════════════════════════
    st.header("1. Učitajte podatke")

    col_sav, col_input = st.columns(2)

    with col_sav:
        st.subheader("📁 SPSS podatkovni fajl (.sav)")
        sav_file = st.file_uploader(
            "Odaberite .sav fajl",
            type=["sav"],
            help="SPSS podatkovni fajl s vašim podacima",
        )

    with col_input:
        st.subheader("📝 Definicije tablica (input.txt)")
        input_file = st.file_uploader(
            "Odaberite input.txt",
            type=["txt"],
            help="Tekstualni fajl s definicijama tablica (3 sekcije odvojene praznim redom)",
        )

    # ── Učitaj .sav ako je uploadano ──
    if sav_file is not None:
        if 'df' not in st.session_state or st.session_state.get('_sav_name') != sav_file.name:
            with st.spinner("Učitavam .sav podatke..."):
                with tempfile.NamedTemporaryFile(suffix='.sav', delete=False) as tmp:
                    tmp.write(sav_file.read())
                    tmp_path = tmp.name
                try:
                    df, meta = pyreadstat.read_sav(tmp_path, apply_value_formats=False)
                    st.session_state['df'] = df
                    st.session_state['meta'] = meta
                    st.session_state['_sav_name'] = sav_file.name
                finally:
                    os.unlink(tmp_path)

        df = st.session_state['df']
        meta = st.session_state['meta']

        st.success(f"✅ Učitano: **{sav_file.name}** — {len(df)} ispitanika, {len(df.columns)} varijabli")
    else:
        df = None
        meta = None

    # ── Učitaj input.txt ako je uploadano ──
    if input_file is not None:
        if 'titles' not in st.session_state or st.session_state.get('_input_name') != input_file.name:
            raw = input_file.read()
            break_vars, titles, variables = parse_input_bytes(raw)
            st.session_state['break_vars'] = break_vars
            st.session_state['titles'] = titles
            st.session_state['variables'] = variables
            st.session_state['_input_name'] = input_file.name

        titles = st.session_state['titles']
        variables = st.session_state['variables']
        st.success(f"✅ Učitano: **{input_file.name}** — {len(titles)} tablica definirano")
    else:
        titles = None
        variables = None

    # ── Plan obrade (opcionalno) ──
    with st.expander("📋 Plan obrade — učitaj prethodno spremljenu konfiguraciju", expanded=False):
        plan_file = st.file_uploader(
            "Učitajte _po.json fajl",
            type=["json"],
            help="Prethodno spremljeni plan obrade sa svim postavkama",
        )
        if plan_file is not None and df is not None and titles is not None:
            if st.session_state.get('_plan_applied_name') != plan_file.name:
                try:
                    plan_data = json.loads(plan_file.read().decode('utf-8'))
                    st.session_state['_pending_plan'] = plan_data
                    st.session_state['_plan_applied_name'] = plan_file.name
                except (json.JSONDecodeError, UnicodeDecodeError):
                    st.error("❌ Neispravan JSON fajl.")

    if df is None or titles is None:
        st.info("👆 Učitajte oba fajla za nastavak.")
        return

    st.divider()

    # ══════════════════════════════════════════════════
    #  KORAK 2: Globalne postavke
    # ══════════════════════════════════════════════════
    st.header("2. Globalne postavke")

    all_vars = list(df.columns)
    labels_dict = getattr(meta, 'column_names_to_labels', {}) or {}
    val_labels_dict = getattr(meta, 'variable_value_labels', {}) or {}
    numeric_vars = [c for c in all_vars if pd.api.types.is_numeric_dtype(df[c])]

    def var_display(v):
        lbl = labels_dict.get(v, '')
        if lbl and lbl != v:
            return f"{v}  —  {lbl[:60]}"
        return v

    # ── Pre-apply global settings from pending plan ──
    _pp = st.session_state.get('_pending_plan')
    if _pp:
        _g = _pp.get('global', {})
        st.session_state['use_weight'] = _g.get('use_weight', False)
        wc = _g.get('weight_col')
        if wc and wc in numeric_vars:
            st.session_state['weight_idx'] = numeric_vars.index(wc)
        if 'start_num' in _g:
            st.session_state['start_num'] = _g['start_num']

    col_weight, col_start = st.columns(2)

    with col_weight:
        st.subheader("⚖️ Ponder")
        use_weight = st.checkbox("Koristi ponder", value=False, key="use_weight")
        weight_col = None
        if use_weight:
            weight_idx = st.selectbox(
                "Odaberite varijablu pondera:",
                options=range(len(numeric_vars)),
                format_func=lambda i: var_display(numeric_vars[i]),
                key="weight_idx",
            )
            weight_col = numeric_vars[weight_idx]
            st.caption(f"Suma: {df[weight_col].sum():.1f} | Prosjek: {df[weight_col].mean():.4f}")

    with col_start:
        st.subheader("🔢 Početni broj")
        start_num = st.number_input("Početni broj tablice:", min_value=1, value=1, step=1,
                                     key="start_num")

    # ── Priprema za filtre i krizanja ──
    var_groups = build_variable_groups(titles, variables, df.columns)
    st.session_state['var_groups'] = var_groups

    # Kategoricke varijable za banner
    cat_vars = []
    for v in df.columns:
        nunique = df[v].dropna().nunique()
        if 2 <= nunique <= 30:
            lbl = labels_dict.get(v) or ''
            disp = f"{v} — {lbl}" if lbl and lbl != v else v
            cat_vars.append((v, disp))
    cat_var_names = [cv[0] for cv in cat_vars]
    cat_var_displays = [cv[1] for cv in cat_vars]

    # Tablice iz input.txt
    table_options = []
    for i, t in enumerate(titles):
        tt = get_table_type(t)
        if tt in ('s', 'k', 'd', 'n', 'm'):
            tn = get_table_title(t)
            table_options.append((i, f"T{i + start_num} [{tt}] {tn[:70]}"))
    all_tbl_indices = [to[0] for to in table_options]
    all_tbl_displays = [to[1] for to in table_options]

    # Filter choices (prepared once, reused per output)
    filter_choices = []
    for gk, gv in var_groups.items():
        is_mr = gv['types'] & {'k', 'd'}
        if is_mr and len(gv['vars']) > 1:
            filter_choices.append({
                'display': f"{gv['label']}",
                'group_key': gk,
                'mode': 'multi',
                'vars': gv['vars'],
            })
        else:
            for v in gv['vars']:
                var_lbl = labels_dict.get(v) or ''
                if var_lbl and var_lbl != v:
                    disp = f"{gv['label']} — {var_lbl[:50]}"
                else:
                    disp = f"{gv['label']} — {v}"
                filter_choices.append({
                    'display': disp,
                    'group_key': gk,
                    'mode': 'single',
                    'vars': [v],
                })
    choice_displays = [fc['display'] for fc in filter_choices]

    # ── Apply pending plan (output settings) ──
    if '_pending_plan' in st.session_state:
        _apply_plan_outputs(
            st.session_state.pop('_pending_plan'),
            cat_var_names, filter_choices, all_tbl_indices,
            df, val_labels_dict,
        )
        st.rerun()

    st.divider()

    # ══════════════════════════════════════════════════
    #  KORAK 3: Outputi
    # ══════════════════════════════════════════════════
    st.header("3. Outputi")
    st.caption("Svaki output = jedan Excel sheet. Može biti **Total** (frekvencijske tablice) "
               "ili **Križanje** (banner tablice). Svaki output ima vlastiti filter.")

    if 'n_outputs' not in st.session_state:
        st.session_state['n_outputs'] = 1

    def _add_output():
        st.session_state['n_outputs'] += 1
    def _remove_output():
        if st.session_state['n_outputs'] > 1:
            st.session_state['n_outputs'] -= 1

    def _duplicate_output(src):
        """Copy all session_state keys from output *src* to a new output at the end."""
        n = st.session_state['n_outputs']
        st.session_state['n_outputs'] = n + 1
        dst = n  # 0-based index of the new output

        # Keys that are directly per-output
        for key_tpl in ('out_type_{}', 'out_name_{}', 'out_filt_{}',
                        'out_banner_{}', 'out_sig_{}', 'out_sigtot_{}',
                        'out_tblmode_{}', 'out_excl_{}', 'out_sel_{}',
                        'n_fg_{}'):
            k_src = key_tpl.format(src)
            if k_src in st.session_state:
                st.session_state[key_tpl.format(dst)] = st.session_state[k_src]

        # Append suffix to sheet name to avoid duplicate
        name_key = f'out_name_{dst}'
        if name_key in st.session_state:
            st.session_state[name_key] = st.session_state[name_key] + '_kopija'

        # Filter group rows
        n_fg = st.session_state.get(f'n_fg_{src}', 0)
        for fi in range(n_fg):
            for fg_tpl in ('fg_logic_{}_{}', 'fg_var_{}_{}', 'fg_vals_{}_{}'):
                k_src = fg_tpl.format(src, fi)
                if k_src in st.session_state:
                    st.session_state[fg_tpl.format(dst, fi)] = st.session_state[k_src]

    output_defs = []

    for oi in range(st.session_state['n_outputs']):
        with st.container(border=True):
            # ── Header: Output N ──
            h_col, type_col, name_col, dup_col = st.columns([1, 2, 2, 0.6])

            with h_col:
                st.markdown(f"### Output {oi + 1}")

            with dup_col:
                st.button("📋", key=f"dup_{oi}",
                          on_click=_duplicate_output, args=(oi,),
                          help="Dupliciraj ovaj output")

            with type_col:
                out_type = st.selectbox(
                    "Tip:",
                    options=['total', 'krizanje'],
                    format_func=lambda t: {'total': '📋 Total (frekvencije)',
                                           'krizanje': '📊 Križanje (banner)'}[t],
                    key=f"out_type_{oi}",
                )

            with name_col:
                default_name = 'Total' if out_type == 'total' else f'Kriz_{oi + 1}'
                sheet_name = st.text_input("Ime sheeta:", value=default_name,
                                           key=f"out_name_{oi}")

            # ── Filter (per-output) ──
            with st.expander("🔍 Filter", expanded=False):
                use_filt = st.checkbox("Koristi filter", value=False,
                                       key=f"out_filt_{oi}")
                out_filter_groups = []
                if use_filt and filter_choices:
                    if f'n_fg_{oi}' not in st.session_state:
                        st.session_state[f'n_fg_{oi}'] = 1

                    def _add_fg(o=oi):
                        st.session_state[f'n_fg_{o}'] += 1
                    def _rm_fg(o=oi):
                        if st.session_state[f'n_fg_{o}'] > 1:
                            st.session_state[f'n_fg_{o}'] -= 1

                    for fi in range(st.session_state[f'n_fg_{oi}']):
                        if fi > 0:
                            logic = st.radio(
                                "Veznik", ["I (AND)", "ILI (OR)"],
                                key=f"fg_logic_{oi}_{fi}",
                                horizontal=True,
                                label_visibility="collapsed",
                            )
                            row_logic = 'OR' if 'ILI' in logic else 'AND'
                        else:
                            row_logic = 'AND'

                        c_var, c_vals = st.columns([2, 3])

                        with c_var:
                            choice_idx = st.selectbox(
                                "Pitanje",
                                options=range(len(filter_choices)),
                                format_func=lambda i, cd=choice_displays: cd[i],
                                key=f"fg_var_{oi}_{fi}",
                                label_visibility="collapsed",
                            )
                            chosen = filter_choices[choice_idx]

                        with c_vals:
                            if chosen['mode'] == 'multi':
                                sub_vars = chosen['vars']
                                sub_displays = [labels_dict.get(sv, sv)
                                                for sv in sub_vars]
                                selected = st.multiselect(
                                    "Opcije",
                                    options=range(len(sub_vars)),
                                    format_func=lambda i, sd=sub_displays: sd[i],
                                    key=f"fg_vals_{oi}_{fi}",
                                    label_visibility="collapsed",
                                    placeholder="Odaberite...",
                                )
                                selected_vals = [sub_vars[i] for i in selected]
                                out_filter_groups.append({
                                    'mode': 'multi',
                                    'group_label': chosen['display'],
                                    'vals': selected_vals,
                                    'logic': row_logic,
                                })
                            else:
                                the_var = chosen['vars'][0]
                                var_vlabels = val_labels_dict.get(the_var, {})
                                try:
                                    unique_vals = sorted(
                                        df[the_var].dropna().unique(),
                                        key=lambda x: (0, float(x)) if isinstance(x, (int, float)) else (1, str(x))
                                    )
                                except TypeError:
                                    unique_vals = sorted(df[the_var].dropna().unique(), key=str)

                                val_options = []
                                for uv in unique_vals:
                                    lbl = var_vlabels.get(uv, '')
                                    if not lbl:
                                        try:
                                            lbl = var_vlabels.get(int(uv) if isinstance(uv, float) and uv == int(uv) else uv, '')
                                        except (ValueError, TypeError):
                                            pass
                                    val_options.append(f"{uv} — {lbl}" if lbl else str(uv))

                                selected = st.multiselect(
                                    "Vrijednosti",
                                    options=range(len(unique_vals)),
                                    format_func=lambda i, vo=val_options: vo[i],
                                    key=f"fg_vals_{oi}_{fi}",
                                    label_visibility="collapsed",
                                    placeholder="Odaberite...",
                                )
                                selected_vals = [unique_vals[i] for i in selected]
                                out_filter_groups.append({
                                    'mode': 'single',
                                    'var': the_var,
                                    'group_label': chosen['display'],
                                    'vals': selected_vals,
                                    'logic': row_logic,
                                })

                    bc1, bc2, _ = st.columns([1, 1, 4])
                    with bc1:
                        st.button("➕ Uvjet", on_click=_add_fg, key=f"fg_add_{oi}")
                    with bc2:
                        st.button("➖ Ukloni", on_click=_rm_fg, key=f"fg_rm_{oi}")

                    active_fg = [g for g in out_filter_groups if g.get('vals')]
                    if active_fg:
                        try:
                            filt_df = apply_filter_groups(df, active_fg)
                            st.caption(f"Filter: **{len(filt_df)}** od {len(df)} ispitanika")
                        except Exception:
                            pass

            # ── Križanje specifično: banner + tablice ──
            banner_vars_sel = []
            show_sig = False
            tbl_final_indices = list(all_tbl_indices)

            if out_type == 'krizanje':
                if not cat_vars:
                    st.warning("Nema kategoričkih varijabli za banner.")
                elif not table_options:
                    st.warning("Nema tablica za križanje.")
                else:
                    banner_vars_sel = st.multiselect(
                        "Banner varijable:",
                        options=range(len(cat_vars)),
                        format_func=lambda i, cd=cat_var_displays: cd[i],
                        key=f"out_banner_{oi}",
                        placeholder="Odaberite banner varijable...",
                    )

                    if banner_vars_sel:
                        for bi in banner_vars_sel:
                            bvar = cat_var_names[bi]
                            bvl = val_labels_dict.get(bvar, {})
                            bvals = sorted(
                                df[bvar].dropna().unique(),
                                key=lambda x: (0, float(x)) if isinstance(x, (int, float)) else (1, str(x))
                            )
                            cats = []
                            for bv in bvals[:8]:
                                lbl = bvl.get(bv, '')
                                if not lbl:
                                    try:
                                        lbl = bvl.get(int(bv) if isinstance(bv, float) and bv == int(bv) else bv, '')
                                    except (ValueError, TypeError):
                                        pass
                                cats.append(str(lbl) if lbl else str(bv))
                            more = '...' if len(bvals) > 8 else ''
                            st.caption(f"↳ **{bvar}** ({len(bvals)} kat.): {', '.join(cats)}{more}")

                    show_sig = st.checkbox("Značajnost (z-test 95%)", value=True,
                                           key=f"out_sig_{oi}",
                                           help="Generira dodatni sheet s _sig sufiksom")

                    show_sig_total = st.checkbox("Sig Total (Total vs kategorije)", value=False,
                                                key=f"out_sigtot_{oi}",
                                                help="Generira dodatni sheet s _sig_total — Total stupac se testira protiv svake kategorije")

                    tbl_mode = st.radio(
                        "Tablice:",
                        options=['all', 'exclude', 'select'],
                        format_func=lambda m: {
                            'all': f'Sve ({len(table_options)})',
                            'exclude': 'Sve osim isključenih',
                            'select': 'Samo odabrane',
                        }[m],
                        key=f"out_tblmode_{oi}",
                        horizontal=True,
                    )

                    if tbl_mode == 'exclude':
                        excluded = st.multiselect(
                            "Isključi:",
                            options=range(len(table_options)),
                            format_func=lambda i, td=all_tbl_displays: td[i],
                            key=f"out_excl_{oi}",
                        )
                        tbl_final_indices = [all_tbl_indices[j] for j in range(len(table_options)) if j not in excluded]
                    elif tbl_mode == 'select':
                        selected = st.multiselect(
                            "Odaberi:",
                            options=range(len(table_options)),
                            format_func=lambda i, td=all_tbl_displays: td[i],
                            key=f"out_sel_{oi}",
                        )
                        tbl_final_indices = [all_tbl_indices[j] for j in selected]

            # ── Spremi output definiciju ──
            out_def = {
                'type': out_type,
                'sheet_name': sheet_name[:31],
                'filter_groups': [g for g in out_filter_groups if g.get('vals')] if use_filt else [],
            }
            if out_type == 'krizanje':
                out_def['banner_vars'] = [cat_var_names[bi] for bi in banner_vars_sel]
                out_def['show_sig'] = show_sig
                out_def['show_sig_total'] = show_sig_total
                out_def['table_indices'] = tbl_final_indices
            output_defs.append(out_def)

    bc1, bc2, _ = st.columns([1, 1, 4])
    with bc1:
        st.button("➕ Dodaj output", on_click=_add_output)
    with bc2:
        st.button("➖ Ukloni output", on_click=_remove_output)

    # ── Spremi plan obrade ──
    if output_defs:
        plan_dict = collect_plan(output_defs, use_weight, weight_col, start_num)
        plan_json = json.dumps(plan_dict, ensure_ascii=False, indent=2, cls=_NumpyEncoder)
        sav_base = os.path.splitext(st.session_state.get('_sav_name', 'data'))[0]
        plan_filename = f"{sav_base}_po.json"
        st.download_button(
            label="💾 Spremi plan obrade",
            data=plan_json,
            file_name=plan_filename,
            mime="application/json",
        )

    st.divider()

    # ══════════════════════════════════════════════════
    #  KORAK 4: Pregled i generiranje
    # ══════════════════════════════════════════════════
    st.header("4. Generiraj tablice")

    # Pregled input-a
    with st.expander("📋 Pregled definiranih tablica", expanded=False):
        preview_data = []
        type_names = {
            's': 'Frekvencija', 'k': 'Multiple Response', 'd': 'Multi Dichotomy',
            'n': 'Numerička (full)', 'm': 'Numerička (mean)', 'f': 'Frequencies',
        }
        for i, t in enumerate(titles):
            tt = get_table_type(t)
            tn = get_table_title(t)
            preview_data.append({
                '#': i + start_num,
                'Tip': type_names.get(tt, tt),
                'Naslov': tn[:80],
            })
        st.dataframe(preview_data, width='stretch', hide_index=True)

    # Sažetak outputa
    out_summary = []
    for od in output_defs:
        filt_str = f" + filter ({len(od['filter_groups'])} uvjeta)" if od['filter_groups'] else ""
        if od['type'] == 'total':
            out_summary.append(f"**{od['sheet_name']}** — Total{filt_str}")
        else:
            nb = len(od.get('banner_vars', []))
            nt = len(od.get('table_indices', []))
            sig_str = (" + _sig" if od.get('show_sig') else "") + \
                      (" + _sig_total" if od.get('show_sig_total') else "")
            out_summary.append(f"**{od['sheet_name']}** — Križanje ({nb} bannera × {nt} tablica){sig_str}{filt_str}")
    st.markdown(" · ".join(out_summary) if out_summary else "Nema definiranih outputa.")

    # ── Gumb za generiranje ──
    if st.button("🚀 Generiraj Excel tablice", type="primary", use_container_width=True):

        progress = st.progress(0, text="Pripremam podatke...")
        t_start = time.time()

        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name

        from openpyxl import Workbook as _Workbook
        wb = _Workbook()
        # Ukloni default sheet
        wb.remove(wb.active)
        existing_sheets = []

        total_tables = 0
        total_xt = 0
        all_errors = []

        n_out = len(output_defs)
        for out_i, out_def in enumerate(output_defs):
            pct_base = int(10 + 80 * out_i / max(n_out, 1))
            progress.progress(pct_base, text=f"Output {out_i + 1}/{n_out}: {out_def['sheet_name']}...")

            # Primijeni per-output filter
            work_df = df.copy()
            if out_def['filter_groups']:
                work_df = apply_filter_groups(work_df, out_def['filter_groups'])

            # ── Unique sheet name ──
            def _unique_name(base):
                name = base[:31]
                while name in existing_sheets:
                    sfx = 2
                    while f"{name[:28]}_{sfx}" in existing_sheets:
                        sfx += 1
                    name = f"{name[:28]}_{sfx}"
                existing_sheets.append(name)
                return name

            if out_def['type'] == 'total':
                # ── TOTAL output → frekvencijske tablice ──
                tables, errs = generate_tables(
                    work_df, meta, titles, variables, weight_col, start_num
                )
                total_tables += len(tables)
                all_errors.extend(errs)

                # Piši u privremeni fajl pa kopiraj sheetove
                import tempfile as _tmpmod
                with _tmpmod.NamedTemporaryFile(suffix='.xlsx', delete=False) as _tf:
                    _tf_path = _tf.name
                write_tables_to_excel(tables, _tf_path)
                _twb = load_workbook(_tf_path)
                for _srcws in _twb.worksheets:
                    sname = _unique_name(out_def['sheet_name'])
                    _destws = wb.create_sheet(title=sname)
                    for row in _srcws.iter_rows():
                        for cell in row:
                            _destws.cell(row=cell.row, column=cell.column, value=cell.value)
                            if cell.has_style:
                                _destws.cell(row=cell.row, column=cell.column).font = cell.font.copy()
                                _destws.cell(row=cell.row, column=cell.column).fill = cell.fill.copy()
                                _destws.cell(row=cell.row, column=cell.column).border = cell.border.copy()
                                _destws.cell(row=cell.row, column=cell.column).alignment = cell.alignment.copy()
                                _destws.cell(row=cell.row, column=cell.column).number_format = cell.number_format
                    # Kopiraj širine stupaca
                    for col_letter, dim in _srcws.column_dimensions.items():
                        _destws.column_dimensions[col_letter].width = dim.width
                    # Kopiraj merge cells
                    for mc in _srcws.merged_cells.ranges:
                        _destws.merge_cells(str(mc))
                _twb.close()
                os.unlink(_tf_path)

            elif out_def['type'] == 'krizanje':
                # ── KRIŽANJE output → banner tablice ──
                banner_vars = out_def.get('banner_vars', [])
                tbl_indices = out_def.get('table_indices', [])
                show_sig = out_def.get('show_sig', True)
                show_sig_total = out_def.get('show_sig_total', False)

                if not banner_vars or not tbl_indices:
                    all_errors.append(f"Output '{out_def['sheet_name']}': nema banner varijabli ili tablica")
                    continue

                col_map = build_column_map(work_df)

                # Sheet bez sig-a (uvijek)
                ws_name = _unique_name(out_def['sheet_name'])
                ws = wb.create_sheet(title=ws_name)

                # Sheet sa sig-om (ako show_sig=True)
                ws_sig = None
                if show_sig:
                    sig_name = _unique_name(out_def['sheet_name'] + '_sig')
                    ws_sig = wb.create_sheet(title=sig_name)

                # Sheet sa sig_total (ako show_sig_total=True)
                ws_sig_total = None
                if show_sig_total:
                    sig_total_name = _unique_name(out_def['sheet_name'] + '_sig_total')
                    ws_sig_total = wb.create_sheet(title=sig_total_name)

                current_row = 1
                current_row_sig = 1
                current_row_st = 1

                for ti in tbl_indices:
                    title_line = titles[ti]
                    var_line = variables[ti]
                    table_type = get_table_type(title_line)
                    table_title = get_table_title(title_line)
                    table_num = ti + start_num

                    # Per-table: compute crosstab for each banner var
                    crosstabs = []
                    for break_var in banner_vars:
                        # Preskoči samo-križanje
                        if table_type == 's' and var_line.strip().lower() == break_var.lower():
                            continue
                        if break_var.lower() in var_line.lower():
                            continue

                        try:
                            if table_type == 's':
                                xt = make_crosstab_simple(
                                    work_df, var_line.strip(), break_var,
                                    meta, col_map, weight_col)
                            elif table_type in ('k', 'd'):
                                xt = make_crosstab_mr(
                                    work_df, var_line, break_var,
                                    meta, col_map, table_type, weight_col)
                            elif table_type in ('n', 'm'):
                                xt = make_crosstab_numeric(
                                    work_df, var_line, break_var,
                                    meta, col_map, table_type == 'n', weight_col)
                            else:
                                continue
                            crosstabs.append(xt)
                        except Exception as e:
                            all_errors.append(f"Kriz T{table_num} × {break_var}: {e}")

                    if not crosstabs:
                        continue

                    # Merge into banner
                    banner = merge_crosstabs_banner(crosstabs)
                    title_str = f"T{table_num} {table_title}"
                    total_xt += 1

                    # Write to plain sheet (no sig)
                    current_row = write_banner_to_sheet(
                        ws, banner, title_str,
                        start_row=current_row, show_sig=False)
                    current_row += 2

                    # Write to sig sheet (with sig)
                    if ws_sig is not None:
                        current_row_sig = write_banner_to_sheet(
                            ws_sig, banner, title_str,
                            start_row=current_row_sig, show_sig=True)
                        current_row_sig += 2

                    # Write to sig_total sheet (sig on Total column)
                    if ws_sig_total is not None:
                        current_row_st = write_banner_to_sheet(
                            ws_sig_total, banner, title_str,
                            start_row=current_row_st, show_sig=True,
                            show_sig_total=True)
                        current_row_st += 2

        # Spremi workbook
        if not wb.worksheets:
            wb.create_sheet("Sheet1")
        wb.save(tmp_path)
        wb.close()

        elapsed = time.time() - t_start

        with open(tmp_path, 'rb') as f:
            excel_bytes = f.read()
        os.unlink(tmp_path)

        progress.progress(100, text="Gotovo!")

        # Rezultati
        st.divider()

        col_res1, col_res2, col_res3, col_res4 = st.columns(4)
        col_res1.metric("Total tablica", total_tables)
        col_res2.metric("Banner tablica", total_xt)
        col_res3.metric("Vrijeme", f"{elapsed:.2f}s")
        col_res4.metric("Sheetova", len(existing_sheets))

        if all_errors:
            with st.expander(f"⚠️ {len(all_errors)} grešaka", expanded=True):
                for e in all_errors:
                    st.warning(e)

        # ── Download gumb ──
        sav_base = os.path.splitext(sav_file.name)[0]
        output_name = f"{sav_base}_tablice.xlsx"

        st.download_button(
            label=f"⬇️ Preuzmi {output_name}",
            data=excel_bytes,
            file_name=output_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True,
        )

        st.success("✅ Tablice uspješno generirane!")

    # ══════════════════════════════════════════════════
    #  SIDEBAR: Info
    # ══════════════════════════════════════════════════
    with st.sidebar:
        st.header("ℹ️ Upute")
        st.markdown("""
        **Kako koristiti:**

        1. **Učitajte .sav fajl** — vaši SPSS podaci
        2. **Učitajte input.txt** — definicije tablica
        3. **Globalne postavke** — ponder, početni broj
        4. **Outputi** — definirajte Total i/ili Križanje outpute
        5. **Kliknite Generiraj** i preuzmite Excel

        ---

        **💾 Plan obrade:**
        - Kliknite **Spremi plan obrade** za download _po.json
        - Učitajte plan u koraku 1 da vratite sve postavke

        ---

        **Tipovi tablica:**
        - **s** — Frekvencija (n, %)
        - **k** — Multiple Response
        - **d** — Multi Dichotomy
        - **n** — Numerička statistika
        - **m** — Numerička kratka (Mean, N)
        - **f** — Frequencies (sortirano)

        ---

        **Outputi:**
        - **Total** — frekvencijske tablice
        - **Križanje** — banner tablice (više break varijabli side-by-side)
        - Svaki output ima vlastiti filter
        - Značajnost → dva sheeta (bez + _sig)
        """)

        st.divider()
        st.caption("SPSS Tables Generator v2.0")


if __name__ == '__main__':
    main()
