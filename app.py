"""
═══════════════════════════════════════════════════════════════════════════
Tableau Dependency Analyzer - Developed by Gurpreet Singh (Datavizcanavs)
═══════════════════════════════════════════════════════════════════════════


OVERVIEW:

This tool analyzes Tableau workbooks (.twb/.twbx) to identify field dependencies
and calculate impact of changes. Generates interactive HTML report with:
- Dependency diagrams (Mermaid.js)
- Sortable/searchable tables
- Excel download on-demand
- Risk level assessment (High/Medium/Low/None)

Author: Gurpreet Singh
Date: Mar 2026
═══════════════════════════════════════════════════════════════════════════
"""


# ==================== IMPORTS ====================
import streamlit as st
import pandas as pd
import re
import json
import html
import tempfile
import os
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from datetime import datetime

st.set_page_config(page_title="Tableau Dependency Analyzer", page_icon="📊", layout="wide")

DEPENDENCY_THRESHOLDS = {'high': 5, 'medium': 2, 'low': 1}
COLORS = {
    'high': '#FF5722', 'medium': '#FFB300', 'low': '#4CAF50', 'none': '#9E9E9E',
    'default_field': '#FFB6D9', 'default_field_stroke': '#C2185B',
    'parameter': '#B3E5FC', 'parameter_stroke': '#0277BD',
    'calculated_field': '#C8E6C9', 'calculated_field_stroke': '#388E3C'
}

def sanitize_for_mermaid(text):
    if not text or pd.isna(text):
        return "Unknown"
    text = str(text).replace('"', "'").replace('\n', ' ').replace('\r', '').replace('`', "'")
    text = text.replace('\\', '/').replace('#', 'num').replace(';', ',').replace('(', ' ').replace(')', ' ')
    text = text.replace('<', '').replace('>', '').replace('{', '').replace('}', '').replace('|', ' ')
    text = text.replace('&', 'and').replace('%', 'pct')
    return re.sub(r'\s+', ' ', text).strip()[:60]

def sanitize_for_html(text):
    return html.escape(str(text)) if text and not pd.isna(text) else ""

def get_dependency_level(count):
    if count >= DEPENDENCY_THRESHOLDS['high']:
        return "HIGH", "🔴", COLORS['high']
    elif count >= DEPENDENCY_THRESHOLDS['medium']:
        return "MEDIUM", "🟡", COLORS['medium']
    elif count >= DEPENDENCY_THRESHOLDS['low']:
        return "LOW", "🟢", COLORS['low']
    return "NONE", "⚪", COLORS['none']

def parse_tableau_file(file_path):
    if file_path.endswith('.twbx'):
        with zipfile.ZipFile(file_path, 'r') as z:
            twb_files = [f for f in z.namelist() if f.endswith('.twb')]
            if not twb_files:
                raise ValueError("No .twb file found inside .twbx")
            with z.open(twb_files[0]) as twb:
                tree = ET.parse(twb)
    else:
        tree = ET.parse(file_path)
    return tree.getroot()

def extract_fields_from_xml(root):
    all_fields = []
    field_counter = 0
    seen_fields = set()
   
    for datasource in root.findall('.//datasource'):
        ds_name = datasource.get('name', 'Unknown')
        ds_caption = datasource.get('caption') or ds_name
       
        for column in datasource.findall('.//column'):
            field_id = column.get('name', '')
            if not field_id or field_id in seen_fields:
                continue
            seen_fields.add(field_id)
           
            field_caption = column.get('caption')
            if field_caption:
                display_name = field_caption
            else:
                display_name = field_id.strip('[]').replace('[', '').replace(']', '')
           
            calc_elem = column.find('calculation')
            calculation = calc_elem.get('formula', '') if calc_elem is not None else ''
           
            if ds_name == 'Parameters' or '[Parameters]' in field_id:
                field_type = 'Parameter'
            elif calculation and calculation.strip():
                field_type = 'Calculated_Field'
            else:
                field_type = 'Default_Field'
           
            all_fields.append({
                'index': field_counter,
                'datasource_name': ds_name,
                'datasource_caption': ds_caption,
                'field_id': field_id,
                'field_name': display_name,
                'field_caption': display_name,
                'field_calculation': calculation if calculation else None,
                'field_datatype': column.get('datatype', ''),
                'field_type': field_type,
            })
            field_counter += 1
   
    for param in root.findall('.//parameter'):
        param_id = param.get('name', '')
        if param_id in seen_fields:
            continue
        seen_fields.add(param_id)
       
        param_caption = param.get('caption')
        display_name = param_caption if param_caption else param_id.strip('[]')
       
        all_fields.append({
            'index': field_counter,
            'datasource_name': 'Parameters',
            'datasource_caption': 'Parameters',
            'field_id': param_id,
            'field_name': display_name,
            'field_caption': display_name,
            'field_calculation': None,
            'field_datatype': param.get('datatype', ''),
            'field_type': 'Parameter',
        })
        field_counter += 1
   
    df = pd.DataFrame(all_fields)
    if len(df) > 0:
        df['index'] = range(len(df))
    return df

def build_field_lookup_maps(df_fields):
    field_name_to_node, field_id_to_node, node_to_field_info = {}, {}, {}
   
    for idx, row in df_fields.iterrows():
        node_id = f"{row['field_type'][:3].upper()}_{row['index']}"
        node_to_field_info[node_id] = {
            'field_name': row['field_name'],
            'field_type': row['field_type'],
            'datasource': row['datasource_caption'],
            'field_id': row['field_id']
        }
        for variant in [row['field_id'], row['field_id'].lower(),
                       row['field_id'].strip('[]'), row['field_id'].strip('[]').lower(),
                       row['field_name'], row['field_name'].lower()]:
            field_name_to_node[variant] = node_id
            field_id_to_node[variant] = node_id
   
    return field_name_to_node, field_id_to_node, node_to_field_info

def find_field_dependencies(calculation_text, field_name_map, field_id_map):
    if not calculation_text or pd.isna(calculation_text):
        return set()
    calc_text = str(calculation_text)
    dependencies = set()
   
    for match in re.findall(r'\[([^\]]+)\]', calc_text):
        for variant in [match, match.lower(), f'[{match}]', f'[{match}]'.lower()]:
            if variant in field_id_map:
                dependencies.add(field_id_map[variant])
                break
    return dependencies

def analyze_field_dependencies(df_fields, field_name_map, field_id_map, node_info_map):
    relationships, dep_graph, rev_graph = [], defaultdict(list), defaultdict(list)
    processed = set()
   
    for _, row in df_fields[df_fields['field_type'] == 'Calculated_Field'].iterrows():
        calc_node = f"{row['field_type'][:3].upper()}_{row['index']}"
        deps = find_field_dependencies(row['field_calculation'], field_name_map, field_id_map)
        deps.discard(calc_node)
       
        for dep_node in deps:
            if dep_node not in node_info_map or (dep_node, calc_node) in processed:
                continue
            processed.add((dep_node, calc_node))
            relationships.append({
                'source_node': dep_node,
                'target_node': calc_node,
                'source_name': node_info_map[dep_node]['field_name'],
                'target_name': row['field_name']
            })
            dep_graph[dep_node].append(calc_node)
            rev_graph[calc_node].append(dep_node)
   
    return relationships, dep_graph, rev_graph

def calculate_impact_metrics(df_fields, dep_graph, rev_graph, node_info_map):
    results = []
    for _, row in df_fields.iterrows():
        node_id = f"{row['field_type'][:3].upper()}_{row['index']}"
        direct = dep_graph.get(node_id, [])
        visited, queue, all_down = set(), deque([node_id]), []
        while queue:
            curr = queue.popleft()
            if curr in visited:
                continue
            visited.add(curr)
            if curr != node_id:
                all_down.append(curr)
            queue.extend([d for d in dep_graph.get(curr, []) if d not in visited])
       
        level, icon, color = get_dependency_level(len(all_down))
        results.append({
            'node_id': node_id,
            'field_name': row['field_name'],
            'field_type': row['field_type'],
            'datasource': row['datasource_caption'],
            'field_calculation': row.get('field_calculation', None),
            'direct_impact_count': len(direct),
            'total_impact_count': len(all_down),
            'indirect_impact_count': len(all_down) - len(direct),
            'dependency_count': len(rev_graph.get(node_id, [])),
            'dependency_level': level,
            'level_icon': icon,
            'level_color': color,
            'direct_impacts': [node_info_map[n]['field_name'] for n in direct if n in node_info_map],
            'all_impacts': [node_info_map[n]['field_name'] for n in all_down if n in node_info_map],
            'dependencies': [node_info_map[n]['field_name'] for n in rev_graph.get(node_id, []) if n in node_info_map]
        })
    return pd.DataFrame(results).sort_values('total_impact_count', ascending=False)

def generate_mermaid_diagram(df_impact, relationships, node_info_map):
    lines = ["graph LR", "",
             f"    classDef defaultField fill:{COLORS['default_field']},stroke:{COLORS['default_field_stroke']},stroke-width:3px",
             f"    classDef parameter fill:{COLORS['parameter']},stroke:{COLORS['parameter_stroke']},stroke-width:3px",
             f"    classDef calcField fill:{COLORS['calculated_field']},stroke:{COLORS['calculated_field_stroke']},stroke-width:3px",
             ""]
   
    defined = set()
    nodes_in_rels = set()
    for rel in relationships:
        nodes_in_rels.add(rel['source_node'])
        nodes_in_rels.add(rel['target_node'])
   
    for level, css_id, label in [
        ('HIGH', 'highDep', '🟠 High Dependency - 5+ Impacts'),
        ('MEDIUM', 'mediumDep', '🟡 Medium Dependency - 2-4 Impacts'),
        ('LOW', 'lowDep', '🟢 Low Dependency - 1 Impact')
    ]:
        subset = df_impact[(df_impact['dependency_level'] == level) & (df_impact['total_impact_count'] > 0)]
        if len(subset) > 0:
            lines.append(f'    subgraph {css_id}["{label}"]')
            for _, r in subset.iterrows():
                cls = 'parameter' if r['field_type'] == 'Parameter' else ('defaultField' if r['field_type'] == 'Default_Field' else 'calcField')
                safe_name = sanitize_for_mermaid(r['field_name'])
                lines.append(f'    {r["node_id"]}["{safe_name}"]:::{cls}')
                defined.add(r['node_id'])
            lines.append('    end')
            lines.append('')
   
    for n in nodes_in_rels - defined:
        if n in node_info_map:
            info = node_info_map[n]
            cls = 'parameter' if info['field_type'] == 'Parameter' else ('defaultField' if info['field_type'] == 'Default_Field' else 'calcField')
            safe_name = sanitize_for_mermaid(info['field_name'])
            lines.append(f'    {n}["{safe_name}"]:::{cls}')
            defined.add(n)
   
    lines.append('')
    for rel in relationships:
        if rel['source_node'] in defined and rel['target_node'] in defined:
            lines.append(f"    {rel['source_node']} --> {rel['target_node']}")
   
    return '\n'.join(lines)

def replace_field_ids_with_names(formula, node_info):
    if not formula or not node_info:
        return formula
    result = formula
    id_to_name = {}
    for node_id, info in node_info.items():
        field_id = info.get('field_id', '')
        field_name = info.get('field_name', '')
        if field_id and field_name and field_id != field_name:
            id_to_name[field_id] = field_name
            field_id_stripped = field_id.strip('[]')
            if field_id_stripped and field_id_stripped != field_name:
                id_to_name[field_id_stripped] = field_name
    for field_id, field_name in id_to_name.items():
        escaped_id = re.escape(field_id)
        result = re.sub(r'\[' + escaped_id + r'\]', f'[{field_name}]', result)
    return result

def create_html_table_rows(df_subset, node_info_map, include_formula=False):
    rows = []
    for seq, (_, row) in enumerate(df_subset.iterrows(), 1):
        field_name_safe = sanitize_for_html(row['field_name'])
        datasource_safe = sanitize_for_html(row['datasource'])
        datasource_display = datasource_safe[:30] + '...' if len(datasource_safe) > 30 else datasource_safe
       
        direct_list = ', '.join([sanitize_for_html(n) for n in row['direct_impacts']]) if row['direct_impacts'] else '-'
        indirect_names = [n for n in row['all_impacts'] if n not in row['direct_impacts']]
        indirect_list = ', '.join([sanitize_for_html(n) for n in indirect_names]) if indirect_names else '-'
        deps_list = ', '.join([sanitize_for_html(n) for n in row['dependencies']]) if row['dependencies'] else '-'
       
        formula_cell = ""
        if include_formula:
            field_calc = row.get('field_calculation', None)
            if field_calc and pd.notna(field_calc) and str(field_calc).strip():
                formula_raw = replace_field_ids_with_names(str(field_calc), node_info_map)
                formula = sanitize_for_html(formula_raw)
                formula_display = formula[:150] + '...' if len(formula) > 150 else formula
                formula_cell = f'<td class="text-small formula-cell" title="{formula}">{formula_display}</td>'
            else:
                formula_cell = '<td class="text-small formula-cell">-</td>'
       
        rows.append(f'''<tr class="row-{row['dependency_level'].lower()}">
            <td class="text-center seq-col"><strong>{seq}</strong></td>
            <td><span class="dep-badge" style="background:{row['level_color']}">{row['level_icon']} {row['dependency_level']}</span></td>
            <td><strong>{field_name_safe}</strong></td>
            <td class="text-small" title="{datasource_safe}">{datasource_display}</td>
            <td><span class="type-badge type-{row['field_type'].lower().replace('_', '')}">{row['field_type'].replace('_', ' ')}</span></td>
            <td class="text-center">{row['direct_impact_count']}</td>
            <td class="text-center">{row['indirect_impact_count']}</td>
            <td class="text-center"><strong>{row['total_impact_count']}</strong></td>
            <td class="text-center">{row['dependency_count']}</td>
            <td class="text-small" title="{direct_list}">{direct_list[:100] + '...' if len(direct_list) > 100 else direct_list}</td>
            <td class="text-small" title="{indirect_list}">{indirect_list[:100] + '...' if len(indirect_list) > 100 else indirect_list}</td>
            <td class="text-small" title="{deps_list}">{deps_list[:100] + '...' if len(deps_list) > 100 else deps_list}</td>
            {formula_cell}
        </tr>''')
    return ''.join(rows)

def generate_complete_html(df_impact, df_all_fields, relationships, node_info_map, workbook_name):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
   
    total_fields = len(df_impact)
    default_count = len(df_all_fields[df_all_fields['field_type'] == 'Default_Field'])
    calculated_count = len(df_all_fields[df_all_fields['field_type'] == 'Calculated_Field'])
    parameter_count = len(df_all_fields[df_all_fields['field_type'] == 'Parameter'])
    high_dep_count = len(df_impact[df_impact['dependency_level'] == 'HIGH'])
    medium_dep_count = len(df_impact[df_impact['dependency_level'] == 'MEDIUM'])
    low_dep_count = len(df_impact[df_impact['dependency_level'] == 'LOW'])
    none_dep_count = len(df_impact[df_impact['dependency_level'] == 'NONE'])
    total_deps = len(relationships)
   
    mermaid_code = generate_mermaid_diagram(df_impact, relationships, node_info_map)
   
    relationship_map = {}
    for rel in relationships:
        for n in [rel['source_node'], rel['target_node']]:
            if n not in relationship_map:
                relationship_map[n] = {'targets': [], 'sources': []}
        relationship_map[rel['source_node']]['targets'].append(rel['target_node'])
        relationship_map[rel['target_node']]['sources'].append(rel['source_node'])
    relationship_json = json.dumps(relationship_map)
   
    table_all = create_html_table_rows(df_impact, node_info_map, False)
    table_high = create_html_table_rows(df_impact[df_impact['dependency_level'] == 'HIGH'], node_info_map, False)
    table_medium = create_html_table_rows(df_impact[df_impact['dependency_level'] == 'MEDIUM'], node_info_map, False)
    table_low = create_html_table_rows(df_impact[df_impact['dependency_level'] == 'LOW'], node_info_map, False)
    table_none = create_html_table_rows(df_impact[df_impact['dependency_level'] == 'NONE'], node_info_map, False)
    table_default = create_html_table_rows(df_impact[df_impact['field_type'] == 'Default_Field'], node_info_map, False)
    table_params = create_html_table_rows(df_impact[df_impact['field_type'] == 'Parameter'], node_info_map, False)
    table_calc = create_html_table_rows(df_impact[df_impact['field_type'] == 'Calculated_Field'], node_info_map, True)
   
    th_normal = '<thead><tr><th>#</th><th>Level</th><th>Field Name</th><th>Datasource</th><th>Type</th><th>Direct</th><th>Indirect</th><th>Total</th><th>Deps</th><th>Direct Impacts</th><th>Indirect Impacts</th><th>Depends On</th></tr></thead>'
    th_formula = '<thead><tr><th>#</th><th>Level</th><th>Field Name</th><th>Datasource</th><th>Type</th><th>Direct</th><th>Indirect</th><th>Total</th><th>Deps</th><th>Direct Impacts</th><th>Indirect Impacts</th><th>Depends On</th><th>Formula</th></tr></thead>'
   
    excel_icon = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18"><path fill="currentColor" d="M14,2H6A2,2 0 0,0 4,4V20A2,2 0 0,0 6,22H18A2,2 0 0,0 20,20V8L14,2M18,20H6V4H13V9H18V20M12,19L8,15H10.5V12H13.5V15H16L12,19Z"/></svg>'

    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{workbook_name} - Dependency Analysis v7.9</title>
<script type="module">
import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
mermaid.initialize({{startOnLoad:true,theme:'base',flowchart:{{useMaxWidth:false,htmlLabels:true,curve:'basis',padding:50,rankSpacing:200,nodeSpacing:150}},maxTextSize:50000000,securityLevel:'loose'}});
</script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
<script src="https://cdn.sheetjs.com/xlsx-0.20.1/package/dist/xlsx.full.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',sans-serif;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);min-height:100vh;padding:20px}}
.container{{max-width:98%;margin:0 auto}}
.header{{text-align:center;color:white;margin-bottom:30px}}
.header h1{{font-size:2.5em;margin-bottom:10px;text-shadow:2px 2px 4px rgba(0,0,0,0.3)}}
.version-badge{{display:inline-block;background:rgba(255,255,255,0.2);padding:5px 15px;border-radius:20px;font-size:0.9em;margin-top:10px}}
.kpi-formula{{background:rgba(255,255,255,0.95);padding:15px 25px;border-radius:10px;margin:15px auto;max-width:1000px;text-align:center;font-weight:600;color:#333;box-shadow:0 4px 8px rgba(0,0,0,0.2)}}
.stats-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:15px;margin-bottom:30px}}
.stat-card{{background:white;padding:20px 15px;border-radius:10px;text-align:center;box-shadow:0 4px 6px rgba(0,0,0,0.1)}}
.stat-value{{font-size:2.2em;font-weight:bold}}
.stat-value.high{{color:#FF5722}}.stat-value.medium{{color:#FFB300}}.stat-value.low{{color:#4CAF50}}.stat-value.total{{color:#667eea}}
.stat-label{{color:#666;margin-top:5px;font-size:0.85em}}
.section{{background:white;border-radius:12px;padding:30px;margin-bottom:30px;box-shadow:0 8px 16px rgba(0,0,0,0.2)}}
.section-title{{font-size:1.8em;margin-bottom:20px;color:#333;border-bottom:3px solid #667eea;padding-bottom:10px}}
.section-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px}}
.help-btn{{background:#2196F3;color:white;border:none;padding:10px 20px;border-radius:5px;cursor:pointer;font-weight:600;display:flex;align-items:center;gap:8px}}
.help-btn:hover{{background:#1976D2}}
.help-btn.active{{background:#f44336}}
.help-panel{{display:none;background:#f5f5f5;border-radius:8px;padding:20px;margin-bottom:20px;border:2px solid #2196F3}}
.help-panel.active{{display:block}}
.help-panel h3{{color:#1976D2;margin-bottom:15px;font-size:1.3em}}
.help-panel h4{{color:#667eea;margin:15px 0 10px 0;font-size:1.1em}}
.help-warning{{background:#fff3cd;padding:15px;border-radius:8px;margin-bottom:20px;border-left:5px solid #FFA000}}
.help-warning h4{{color:#F57C00;margin:0 0 10px 0}}
.help-warning ol{{line-height:2;margin-left:20px}}
.help-features{{background:white;padding:20px;border-radius:8px;margin-bottom:20px}}
.help-features ul{{line-height:2;margin-left:20px}}
.help-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:20px}}
.help-box{{background:white;padding:20px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1)}}
.help-box h4{{color:#667eea;margin-bottom:10px}}
.help-box p{{line-height:1.7;margin-bottom:10px}}
.help-example{{background:#e3f2fd;padding:10px;border-radius:4px;font-size:0.9em}}
.dep-explanation{{background:#e3f2fd;border-left:4px solid #2196F3;padding:20px;border-radius:8px;margin-bottom:20px}}
.dep-explanation h3{{color:#1976D2;margin-bottom:12px}}
.dep-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:15px}}
.dep-box{{padding:15px;border-radius:8px;border-left:5px solid;box-shadow:0 2px 4px rgba(0,0,0,0.1)}}
.dep-box.high{{background:#ffebee;border-color:#FF5722}}
.dep-box.medium{{background:#fff9c4;border-color:#FFB300}}
.dep-box.low{{background:#e8f5e9;border-color:#4CAF50}}
.dep-box.none{{background:#fafafa;border-color:#9E9E9E}}
.dep-box strong{{display:block;margin-bottom:8px}}
.dep-box.high strong{{color:#FF5722}}.dep-box.medium strong{{color:#F57C00}}.dep-box.low strong{{color:#2E7D32}}.dep-box.none strong{{color:#424242}}
.tabs{{display:flex;gap:10px;margin-bottom:20px;flex-wrap:wrap}}
.tab-button{{padding:12px 24px;background:#f0f0f0;border:none;border-radius:8px;cursor:pointer;font-size:1em;font-weight:600;transition:all 0.3s}}
.tab-button:hover{{background:#667eea;color:white}}
.tab-button.active{{background:#667eea;color:white}}
.tab-content{{display:none}}.tab-content.active{{display:block}}
.tab-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:15px;padding-bottom:10px;border-bottom:2px solid #e0e0e0}}
.tab-title{{font-size:1.2em;font-weight:600;color:#333}}
.download-excel-btn{{background:#217346;color:white;border:none;padding:10px 20px;border-radius:5px;cursor:pointer;font-weight:600;display:inline-flex;align-items:center;gap:8px}}
.download-excel-btn:hover{{background:#1a5c37}}
.table-wrapper{{overflow-x:auto;max-height:600px;overflow-y:auto;border:1px solid #ddd;border-radius:8px}}
table{{width:100%;border-collapse:collapse;font-size:0.9em}}
th{{background:#667eea;color:white;padding:12px 8px;text-align:left;position:sticky;top:0;z-index:10;cursor:pointer}}
th:hover{{background:#5568d3}}
td{{padding:10px 8px;border-bottom:1px solid #eee}}
tr:hover{{background:#f8f9fa}}
.row-high{{background:#ffebee}}.row-medium{{background:#fff9c4}}.row-low{{background:#e8f5e9}}.row-none{{background:#fafafa}}
.seq-col{{font-weight:600;color:#667eea;background:#f0f0f0}}
.dep-badge{{padding:4px 12px;border-radius:20px;font-weight:600;color:white;display:inline-block;font-size:0.9em}}
.type-badge{{padding:4px 10px;border-radius:4px;font-size:0.85em;font-weight:600}}
.type-defaultfield{{background:#FFB6D9;color:#C2185B}}
.type-parameter{{background:#B3E5FC;color:#0277BD}}
.type-calculatedfield{{background:#C8E6C9;color:#388E3C}}
.text-center{{text-align:center}}.text-small{{font-size:0.85em;color:#666}}
.formula-cell{{font-family:'Courier New',monospace;font-size:0.85em;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;background:#f5f5f5;padding:4px 8px;border-radius:3px}}
.search-box{{width:100%;padding:12px;font-size:1em;border:2px solid #667eea;border-radius:8px;margin-bottom:20px}}
.description{{background:#f8f9fa;padding:15px;border-radius:8px;margin-bottom:20px;border-left:4px solid #667eea}}
.diagram-wrapper{{width:100%;background:#fafafa;padding:20px;border-radius:8px;border:2px solid #e0e0e0}}
.diagram-controls{{background:white;padding:15px;border-radius:8px;margin-bottom:15px;display:flex;gap:10px;align-items:center;flex-wrap:wrap;justify-content:space-between}}
.zoom-controls{{display:flex;gap:10px;align-items:center}}
.zoom-btn{{background:#667eea;color:white;border:none;padding:10px 20px;border-radius:5px;cursor:pointer;font-size:18px;font-weight:bold;min-width:50px}}
.zoom-btn:hover{{background:#5568d3}}
.zoom-btn:disabled{{background:#ccc;cursor:not-allowed}}
.zoom-level{{background:#f0f0f0;padding:8px 16px;border-radius:5px;font-weight:600;min-width:80px;text-align:center}}
.export-btn{{background:#28a745;color:white;border:none;padding:12px 24px;border-radius:5px;cursor:pointer;font-weight:bold}}
.export-btn:hover{{background:#218838}}
.zoom-hint{{background:#fff3cd;padding:10px 15px;border-radius:5px;border-left:4px solid #ffc107;color:#856404;font-weight:600;flex:1;min-width:250px}}
.auto-scale-info{{background:#d1ecf1;padding:8px 12px;border-radius:5px;border-left:4px solid #17a2b8;color:#0c5460;font-size:0.9em;margin-top:10px;display:none}}
.highlight-info{{background:#e3f2fd;padding:10px 15px;border-radius:5px;border-left:4px solid #2196F3;color:#0d47a1;font-size:0.9em;margin-top:10px;display:none}}
.diagram-scroll-container{{width:100%;min-height:600px;max-height:1200px;overflow:auto;background:white;border:2px solid #ddd;border-radius:5px;padding:40px}}
.mermaid{{display:inline-block;transform-origin:top left;transition:transform 0.3s ease}}
.mermaid svg{{max-width:none!important}}
.mermaid g.cluster rect{{rx:12px!important;ry:12px!important}}
.mermaid g#highDep>rect,.mermaid [id*="highDep"]>rect{{stroke:#FF5722!important;stroke-width:6px!important;fill:#FFF5F5!important}}
.mermaid g#mediumDep>rect,.mermaid [id*="mediumDep"]>rect{{stroke:#FFB300!important;stroke-width:6px!important;fill:#FFFEF0!important}}
.mermaid g#lowDep>rect,.mermaid [id*="lowDep"]>rect{{stroke:#4CAF50!important;stroke-width:6px!important;fill:#F5FFF5!important}}
.mermaid .cluster-label{{font-weight:bold!important;font-size:26px!important}}
.legend{{display:flex;gap:20px;margin-bottom:20px;flex-wrap:wrap}}
.legend-item{{display:flex;align-items:center;gap:8px}}
.legend-box{{width:30px;height:20px;border-radius:4px;border:2px solid #333}}
.node-selected rect{{stroke:#FF1744!important;stroke-width:5px!important}}
.node-highlighted rect{{stroke:#FFD600!important;stroke-width:4px!important}}
.node-dimmed{{opacity:0.3!important}}
.footer{{text-align:center;color:white;padding:20px;margin-top:30px;background:rgba(255,255,255,0.1);border-radius:10px}}
</style>
</head>
<body>
<div class="container">
<div class="header">
<h1>📊 {workbook_name}</h1>
<p>Comprehensive Field Dependency Analysis & Impact Assessment</p>
<span class="version-badge">v7.9 - Interactive HTML Report</span>
</div>

<div class="kpi-formula"><strong>Total Fields ({total_fields})</strong> = Default ({default_count}) + Calculated ({calculated_count}) + Parameters ({parameter_count})</div>
<div class="kpi-formula"><strong>Dependencies ({total_deps})</strong> = Direct field-to-field relationships</div>
<div class="kpi-formula"><strong>Impact Levels ({high_dep_count + medium_dep_count + low_dep_count})</strong> = High ({high_dep_count}) + Medium ({medium_dep_count}) + Low ({low_dep_count})</div>

<div class="stats-grid">
<div class="stat-card"><div class="stat-value total">{total_fields}</div><div class="stat-label">Total Fields</div></div>
<div class="stat-card"><div class="stat-value total">{default_count}</div><div class="stat-label">📁 Default</div></div>
<div class="stat-card"><div class="stat-value total">{calculated_count}</div><div class="stat-label">🔢 Calculated</div></div>
<div class="stat-card"><div class="stat-value total">{parameter_count}</div><div class="stat-label">⚙️ Parameters</div></div>
<div class="stat-card"><div class="stat-value high">{high_dep_count}</div><div class="stat-label">🟠 High</div></div>
<div class="stat-card"><div class="stat-value medium">{medium_dep_count}</div><div class="stat-label">🟡 Medium</div></div>
<div class="stat-card"><div class="stat-value low">{low_dep_count}</div><div class="stat-label">🟢 Low</div></div>
<div class="stat-card"><div class="stat-value total">{total_deps}</div><div class="stat-label">🔗 Dependencies</div></div>
</div>

<div class="section">
<div class="section-header">
<h2 class="section-title" style="margin-bottom:0;border:none;padding:0">📊 Field Dependency Analysis Table</h2>
<button class="help-btn" id="help-btn" onclick="toggleHelp()">❓ Help & Guide</button>
</div>

<div class="help-panel" id="help-panel">
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:15px">
<h3>📚 Help & Guide</h3>
<button onclick="toggleHelp()" style="background:transparent;border:none;font-size:24px;cursor:pointer;color:#666">×</button>
</div>

<div class="help-warning">
<h4>🎯 Quick Start Guide for Beginners</h4>
<p style="margin-bottom:10px;font-weight:600">Follow these steps to analyze your workbook:</p>
<ol>
<li><strong>Start with High Risk:</strong> Click the <strong>"🟠 High"</strong> tab to see your most critical fields that affect many others</li>
<li><strong>Review Impact Numbers:</strong> Check the <strong>Total</strong> column to see how many fields would be affected by changes</li>
<li><strong>Export Data:</strong> Click <strong style="color:#217346">"Download Excel"</strong> on any tab to create a change management spreadsheet</li>
<li><strong>Visualize Connections:</strong> Scroll to the diagram below and click any field to see what it connects to</li>
<li><strong>Plan Changes:</strong> Before modifying any HIGH or MEDIUM field, document all its dependencies first</li>
</ol>
</div>

<div class="help-features">
<h4>💡 Key Features</h4>
<ul>
<li>✅ <strong style="color:#217346">Excel Download</strong> button on each tab for instant data export</li>
<li>✅ <strong>Calculated Fields</strong> tab includes <strong>Calculation Formula</strong> column</li>
<li>✅ Click any field in the diagram to highlight its dependency network</li>
<li>✅ Sortable columns - click any header to sort</li>
<li>✅ Search box on each tab to find specific fields</li>
<li>✅ Hover over truncated text to see full content</li>
</ul>
</div>

<div class="help-grid">
<div class="help-box">
<h4>🔗 Dependency</h4>
<p>When <strong>Field A</strong> uses <strong>Field B</strong> in its calculation formula, we say "Field A depends on Field B".</p>
<div class="help-example"><strong>Example:</strong><br>If <code>Profit Margin = Profit / Sales</code><br>Then "Profit Margin" <strong>depends on</strong> both "Profit" and "Sales"</div>
</div>
<div class="help-box">
<h4>📊 Impact</h4>
<p>The number of fields that would be <strong>affected, broken, or show incorrect values</strong> if you change or delete a field.</p>
<div class="help-example"><strong>Example:</strong><br>If 10 different fields use "Sales", then "Sales" has an <strong>impact of 10</strong>. Changing "Sales" affects all 10 fields.</div>
</div>
<div class="help-box">
<h4>🎯 Direct vs Indirect</h4>
<p><strong>Direct:</strong> Field explicitly referenced in formula<br><strong>Indirect:</strong> Affected through a chain of dependencies</p>
<div class="help-example"><strong>Example:</strong><br>A → B → C<br>Change C: B = direct impact, A = indirect impact</div>
</div>
<div class="help-box">
<h4>📁 Field Types</h4>
<p><strong style="color:#C2185B">Default:</strong> Raw data from database<br><strong style="color:#388E3C">Calculated:</strong> Formula-based computed fields<br><strong style="color:#0277BD">Parameter:</strong> User-controlled input values</p>
</div>
</div>
</div>

<div class="dep-explanation">
<h3>📖 Understanding Dependency Levels</h3>
<p style="margin-bottom:15px"><strong>What is a dependency?</strong> When one field uses another in its calculation. Changes cascade to dependent fields.</p>
<div class="dep-grid">
<div class="dep-box high"><strong>🟠 HIGH (5+ Impacted Fields)</strong><span><strong>⚠️ Critical Impact!</strong> Changing this field will affect 5 or more other fields. Test thoroughly before making any changes.</span></div>
<div class="dep-box medium"><strong>🟡 MEDIUM (2-4 Impacted Fields)</strong><span><strong>⚡ Moderate Impact.</strong> Changing this field will affect 2 to 4 other fields. Review all dependencies before modifying.</span></div>
<div class="dep-box low"><strong>🟢 LOW (1 Impacted Fields)</strong><span><strong>✓ Minor Impact.</strong> Only 1 other field depends on this. Relatively safer to modify.</span></div>
<div class="dep-box none"><strong>⚪ NONE (No Dependent Fields)</strong><span><strong>✓ No Impact.</strong> No other fields depend on this. Safest to modify or delete.</span></div>
</div>
</div>

<div class="tabs">
<button class="tab-button active" onclick="openTab(event,'tab-all')">All ({total_fields})</button>
<button class="tab-button" onclick="openTab(event,'tab-high')">🟠 High ({high_dep_count})</button>
<button class="tab-button" onclick="openTab(event,'tab-medium')">🟡 Medium ({medium_dep_count})</button>
<button class="tab-button" onclick="openTab(event,'tab-low')">🟢 Low ({low_dep_count})</button>
<button class="tab-button" onclick="openTab(event,'tab-none')">⚪ None ({none_dep_count})</button>
<button class="tab-button" onclick="openTab(event,'tab-default')">📁 Default ({len(df_impact[df_impact['field_type']=='Default_Field'])})</button>
<button class="tab-button" onclick="openTab(event,'tab-params')">⚙️ Parameters ({len(df_impact[df_impact['field_type']=='Parameter'])})</button>
<button class="tab-button" onclick="openTab(event,'tab-calc')">🔢 Calculated ({len(df_impact[df_impact['field_type']=='Calculated_Field'])})</button>
</div>

<div id="tab-all" class="tab-content active">
<div class="tab-header"><div class="tab-title">All Fields</div><button class="download-excel-btn" onclick="downloadExcel('tbl-all','All_Fields')">{excel_icon} Download Excel</button></div>
<input type="text" class="search-box" placeholder="🔎 Search fields..." onkeyup="searchTable('tbl-all',this.value)">
<div class="table-wrapper"><table id="tbl-all">{th_normal}<tbody>{table_all}</tbody></table></div>
</div>

<div id="tab-high" class="tab-content">
<div class="tab-header"><div class="tab-title">High Dependency Fields</div><button class="download-excel-btn" onclick="downloadExcel('tbl-high','High_Dependency')">{excel_icon} Download Excel</button></div>
<input type="text" class="search-box" placeholder="🔎 Search fields..." onkeyup="searchTable('tbl-high',this.value)">
<div class="table-wrapper"><table id="tbl-high">{th_normal}<tbody>{table_high}</tbody></table></div>
</div>

<div id="tab-medium" class="tab-content">
<div class="tab-header"><div class="tab-title">Medium Dependency Fields</div><button class="download-excel-btn" onclick="downloadExcel('tbl-medium','Medium_Dependency')">{excel_icon} Download Excel</button></div>
<input type="text" class="search-box" placeholder="🔎 Search fields..." onkeyup="searchTable('tbl-medium',this.value)">
<div class="table-wrapper"><table id="tbl-medium">{th_normal}<tbody>{table_medium}</tbody></table></div>
</div>

<div id="tab-low" class="tab-content">
<div class="tab-header"><div class="tab-title">Low Dependency Fields</div><button class="download-excel-btn" onclick="downloadExcel('tbl-low','Low_Dependency')">{excel_icon} Download Excel</button></div>
<input type="text" class="search-box" placeholder="🔎 Search fields..." onkeyup="searchTable('tbl-low',this.value)">
<div class="table-wrapper"><table id="tbl-low">{th_normal}<tbody>{table_low}</tbody></table></div>
</div>

<div id="tab-none" class="tab-content">
<div class="tab-header"><div class="tab-title">No Impact Fields</div><button class="download-excel-btn" onclick="downloadExcel('tbl-none','No_Impact')">{excel_icon} Download Excel</button></div>
<input type="text" class="search-box" placeholder="🔎 Search fields..." onkeyup="searchTable('tbl-none',this.value)">
<div class="table-wrapper"><table id="tbl-none">{th_normal}<tbody>{table_none}</tbody></table></div>
</div>

<div id="tab-default" class="tab-content">
<div class="tab-header"><div class="tab-title">Default Fields</div><button class="download-excel-btn" onclick="downloadExcel('tbl-default','Default_Fields')">{excel_icon} Download Excel</button></div>
<input type="text" class="search-box" placeholder="🔎 Search fields..." onkeyup="searchTable('tbl-default',this.value)">
<div class="table-wrapper"><table id="tbl-default">{th_normal}<tbody>{table_default}</tbody></table></div>
</div>

<div id="tab-params" class="tab-content">
<div class="tab-header"><div class="tab-title">Parameters</div><button class="download-excel-btn" onclick="downloadExcel('tbl-params','Parameters')">{excel_icon} Download Excel</button></div>
<input type="text" class="search-box" placeholder="🔎 Search fields..." onkeyup="searchTable('tbl-params',this.value)">
<div class="table-wrapper"><table id="tbl-params">{th_normal}<tbody>{table_params}</tbody></table></div>
</div>

<div id="tab-calc" class="tab-content">
<div class="tab-header"><div class="tab-title">Calculated Fields (with Formulas)</div><button class="download-excel-btn" onclick="downloadExcel('tbl-calc','Calculated_Fields')">{excel_icon} Download Excel</button></div>
<input type="text" class="search-box" placeholder="🔎 Search fields..." onkeyup="searchTable('tbl-calc',this.value)">
<div class="table-wrapper"><table id="tbl-calc">{th_formula}<tbody>{table_calc}</tbody></table></div>
</div>
</div>

<div class="section">
<h2 class="section-title">📊 Interactive Dependency Map</h2>
<div class="description"><strong>🖱️ How to use:</strong> Click any node to highlight its impacts and dependencies. Click background to clear. Use zoom controls for large diagrams.</div>
<div class="legend">
<div class="legend-item"><div class="legend-box" style="background:#FFB6D9;border-color:#C2185B"></div><span><strong>Default Fields</strong> (from database)</span></div>
<div class="legend-item"><div class="legend-box" style="background:#B3E5FC;border-color:#0277BD"></div><span><strong>Parameters</strong> (user inputs)</span></div>
<div class="legend-item"><div class="legend-box" style="background:#C8E6C9;border-color:#388E3C"></div><span><strong>Calculated Fields</strong> (formulas)</span></div>
<div class="legend-item"><div class="legend-box" style="background:#fff;border:3px solid #FF1744"></div><span><strong>High Impact Field</strong> (group)</span></div>
<div class="legend-item"><div class="legend-box" style="background:#fff;border:3px solid #FFD600"></div><span><strong>Medium Impact Fields</strong> (group)</span></div>
<div class="legend-item"><div class="legend-box" style="background:#fff;border:3px solid #388E3C"></div><span><strong>Low Impact Fields</strong> (group)</span></div>
</div>
<div class="diagram-wrapper">
<div class="diagram-controls">
<div class="zoom-controls">
<button class="zoom-btn" onclick="zoomOut()" id="zoom-out-btn">−</button>
<div class="zoom-level" id="zoom-display">100%</div>
<button class="zoom-btn" onclick="zoomIn()" id="zoom-in-btn">+</button>
<button class="zoom-btn" onclick="zoomReset()" style="min-width:80px">Reset</button>
</div>
<button class="export-btn" onclick="exportPNG()" id="export-btn">📷 Export PNG</button>
<div class="zoom-hint">💡 Click any field box in the diagram to see what it impacts. Auto-scaled to fit your screen.</div>
</div>
<div class="auto-scale-info" id="auto-scale-info"></div>
<div class="highlight-info" id="highlight-info"></div>
<div class="diagram-scroll-container" id="diagram-container">
<div class="mermaid" id="main-diagram">{mermaid_code}</div>
</div>
</div>
</div>

<div class="footer">
<strong>Generated:</strong> {timestamp} | <strong>Version:</strong> 7.9 Interactive<br>
<strong>Workbook:</strong> {workbook_name} | <strong>Total Fields:</strong> {total_fields} | <strong>Dependencies:</strong> {total_deps}<br>
<strong>Features:</strong> Excel downloads • Formula column • Interactive highlighting • Help & Guide • Zoom controls • PNG export
</div>
</div>

<script>
var cZ=1.0,aZ=1.0,minZ=0.15,maxZ=3.0;
var relationshipMap={relationship_json};
var selectedNode=null;

function toggleHelp(){{var p=document.getElementById('help-panel'),b=document.getElementById('help-btn');p.classList.toggle('active');b.classList.toggle('active');b.textContent=p.classList.contains('active')?'✕ Close Help':'❓ Help & Guide'}}

function openTab(e,t){{document.querySelectorAll('.tab-content').forEach(c=>c.classList.remove('active'));document.querySelectorAll('.tab-button').forEach(b=>b.classList.remove('active'));document.getElementById(t).classList.add('active');e.currentTarget.classList.add('active')}}

function searchTable(t,q){{var f=q.toUpperCase(),r=document.getElementById(t).getElementsByTagName('tr');for(var i=1;i<r.length;i++){{var c=r[i].getElementsByTagName('td')[2];r[i].style.display=(c&&(c.textContent||c.innerText).toUpperCase().indexOf(f)>-1)?'':'none'}}}}

function downloadExcel(tid,fname){{try{{var t=document.getElementById(tid);var wb=XLSX.utils.table_to_book(t,{{sheet:'Data'}});XLSX.writeFile(wb,fname+'_'+new Date().toISOString().slice(0,10)+'.xlsx')}}catch(e){{alert('Download failed: '+e.message)}}}}

function zoomIn(){{cZ=Math.min(cZ+0.2,maxZ);applyZoom()}}
function zoomOut(){{cZ=Math.max(cZ-0.2,minZ);applyZoom()}}
function zoomReset(){{cZ=aZ;applyZoom()}}
function applyZoom(){{document.getElementById('main-diagram').style.transform='scale('+cZ+')';document.getElementById('zoom-display').textContent=Math.round(cZ*100)+'%';document.getElementById('zoom-in-btn').disabled=(cZ>=maxZ);document.getElementById('zoom-out-btn').disabled=(cZ<=minZ)}}

function clearHighlights(){{var svg=document.querySelector('#main-diagram svg');if(!svg)return;svg.querySelectorAll('.node').forEach(n=>{{n.classList.remove('node-selected','node-highlighted','node-dimmed')}});selectedNode=null;document.getElementById('highlight-info').style.display='none'}}

function highlightNode(nid){{clearHighlights();if(!nid||!relationshipMap[nid])return;selectedNode=nid;var svg=document.querySelector('#main-diagram svg');if(!svg)return;var targets=relationshipMap[nid].targets||[];var sources=relationshipMap[nid].sources||[];svg.querySelectorAll('.node').forEach(n=>{{var id=n.id.replace('flowchart-','').replace(/-[0-9]+$/,'');if(id===nid)n.classList.add('node-selected');else if(targets.indexOf(id)>-1||sources.indexOf(id)>-1)n.classList.add('node-highlighted');else n.classList.add('node-dimmed')}});var info=document.getElementById('highlight-info');info.textContent='🎯 Selected: '+nid+' | Impacts: '+targets.length+' | Dependencies: '+sources.length;info.style.display='block'}}

function setupClickHandlers(){{setTimeout(function(){{var svg=document.querySelector('#main-diagram svg');if(!svg){{setTimeout(setupClickHandlers,500);return}}svg.querySelectorAll('.node').forEach(n=>{{n.style.cursor='pointer';n.addEventListener('click',function(e){{e.stopPropagation();var nid=this.id.replace('flowchart-','').replace(/-[0-9]+$/,'');if(selectedNode===nid)clearHighlights();else highlightNode(nid)}})}}); svg.addEventListener('click',function(e){{if(e.target.tagName==='svg')clearHighlights()}})}},2000)}}

function autoScale(){{setTimeout(function(){{var c=document.getElementById('diagram-container'),d=document.getElementById('main-diagram'),inf=document.getElementById('auto-scale-info');try{{var svg=d.querySelector('svg');if(!svg){{setTimeout(autoScale,1000);return}}var b=svg.getBBox(),w=b.width,h=b.height,cw=c.clientWidth-80,ch=c.clientHeight-80,sx=cw/w,sy=ch/h,as=Math.min(sx,sy,1.0);if(as<1.0){{cZ=Math.max(as,0.15);aZ=cZ;d.style.transform='scale('+cZ+')';document.getElementById('zoom-display').textContent=Math.round(cZ*100)+'%';inf.textContent='🎯 Auto-scaled to '+Math.round(cZ*100)+'%';inf.style.display='block'}}else{{inf.textContent='✅ Fits at 100%';inf.style.display='block'}}setupClickHandlers()}}catch(e){{console.error(e)}}}},2000)}}

function exportPNG(){{clearHighlights();var d=document.getElementById('main-diagram'),b=document.getElementById('export-btn');b.disabled=true;b.textContent='⏳ Generating...';setTimeout(function(){{html2canvas(d,{{scale:2,useCORS:true,backgroundColor:'#ffffff'}}).then(function(canvas){{var link=document.createElement('a');link.download='{workbook_name}_diagram.png';link.href=canvas.toDataURL('image/png');link.click();b.disabled=false;b.textContent='📷 Export PNG'}}).catch(function(e){{b.disabled=false;b.textContent='📷 Export PNG';alert('Export failed')}})}},200)}}

document.addEventListener("DOMContentLoaded",function(){{autoScale()}});
</script>
</body>
</html>'''
    return html_content

st.title("📊 Tableau Dependency Analyzer")
st.markdown("Upload a Tableau workbook (.twb or .twbx) to analyze field dependencies.")

uploaded_file = st.file_uploader("📁 Upload Tableau File", type=['twb', 'twbx'])

if uploaded_file:
    st.success(f"✅ Uploaded: {uploaded_file.name}")
   
    if st.button("🚀 Analyze Dependencies", type="primary", use_container_width=True):
        progress = st.progress(0, "Starting...")
       
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name
       
        try:
            progress.progress(15, "Parsing Tableau file...")
            root = parse_tableau_file(tmp_path)
           
            progress.progress(30, "Extracting fields...")
            df_all = extract_fields_from_xml(root)
           
            if len(df_all) == 0:
                st.error("❌ No fields found in workbook")
                st.stop()
           
            progress.progress(50, "Analyzing dependencies...")
            name_map, id_map, node_info = build_field_lookup_maps(df_all)
            rels, dep_g, rev_g = analyze_field_dependencies(df_all, name_map, id_map, node_info)
           
            progress.progress(70, "Calculating impacts...")
            df_impact = calculate_impact_metrics(df_all, dep_g, rev_g, node_info)
           
            progress.progress(90, "Generating HTML report...")
            wb_name = uploaded_file.name.replace('.twbx', '').replace('.twb', '')
            html_out = generate_complete_html(df_impact, df_all, rels, node_info, wb_name)
           
            progress.progress(100, "Complete!")
           
            st.session_state.update({'html': html_out, 'df_impact': df_impact, 'df_all': df_all, 'wb_name': wb_name, 'rels': rels, 'done': True})
           
        except Exception as e:
            st.error(f"❌ Error: {e}")
            import traceback
            st.code(traceback.format_exc())
        finally:
            os.unlink(tmp_path)

if st.session_state.get('done'):
    df_impact = st.session_state['df_impact']
    df_all = st.session_state['df_all']
   
    st.divider()
    st.subheader("📈 Analysis Results")
   
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Fields", len(df_impact))
    col2.metric("🟠 High Risk", len(df_impact[df_impact['dependency_level'] == 'HIGH']))
    col3.metric("🟡 Medium Risk", len(df_impact[df_impact['dependency_level'] == 'MEDIUM']))
    col4.metric("🟢 Low Risk", len(df_impact[df_impact['dependency_level'] == 'LOW']))
   
    col5, col6, col7, col8 = st.columns(4)
    col5.metric("📁 Default", len(df_all[df_all['field_type'] == 'Default_Field']))
    col6.metric("🔢 Calculated", len(df_all[df_all['field_type'] == 'Calculated_Field']))
    col7.metric("⚙️ Parameters", len(df_all[df_all['field_type'] == 'Parameter']))
    col8.metric("🔗 Dependencies", len(st.session_state['rels']))
   
    tab1, tab2, tab3, tab4 = st.tabs(["🟠 High Risk", "📊 All Fields", "🔢 Calculated", "📁 Default"])
   
    with tab1:
        high_df = df_impact[df_impact['dependency_level'] == 'HIGH'][['field_name', 'field_type', 'total_impact_count', 'direct_impact_count', 'datasource']].copy()
        high_df.columns = ['Field', 'Type', 'Total Impact', 'Direct', 'Datasource']
        st.dataframe(high_df, use_container_width=True, height=300)
   
    with tab2:
        all_df = df_impact[['field_name', 'field_type', 'dependency_level', 'total_impact_count', 'direct_impact_count']].copy()
        all_df.columns = ['Field', 'Type', 'Risk Level', 'Total Impact', 'Direct']
        st.dataframe(all_df, use_container_width=True, height=400)
   
    with tab3:
        calc_df = df_impact[df_impact['field_type'] == 'Calculated_Field'][['field_name', 'dependency_level', 'total_impact_count']].copy()
        calc_df.columns = ['Field', 'Risk Level', 'Total Impact']
        st.dataframe(calc_df, use_container_width=True, height=300)
   
    with tab4:
        def_df = df_impact[df_impact['field_type'] == 'Default_Field'][['field_name', 'dependency_level', 'total_impact_count', 'datasource']].copy()
        def_df.columns = ['Field', 'Risk Level', 'Total Impact', 'Datasource']
        st.dataframe(def_df, use_container_width=True, height=300)
   
    st.divider()
    st.subheader("⬇️ Download Report")
   
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        st.download_button("📄 Download Interactive HTML Report", st.session_state['html'],
                          f"{st.session_state['wb_name']}_dependency_analysis.html", "text/html", type="primary", use_container_width=True)
    with col_dl2:
        csv_data = df_impact.to_csv(index=False)
        st.download_button("📊 Download CSV Data", csv_data, f"{st.session_state['wb_name']}_data.csv", "text/csv", use_container_width=True)
   
    st.info("💡 **Tip:** Open the HTML file in any browser for the full interactive experience with diagrams, 8 tabs, Excel downloads, zoom controls, and click-to-highlight dependencies.")
