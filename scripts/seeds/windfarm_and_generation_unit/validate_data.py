#!/usr/bin/env python3
"""
Validate Excel data before import.
Checks for missing required fields, duplicates, and data quality issues.
"""

import sys
from pathlib import Path
from collections import defaultdict

import pandas as pd

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))


def validate_excel_data(excel_path: str, limit: int = None):
    """Validate Excel data and report issues."""
    print("="*60)
    print("DATA VALIDATION")
    print("="*60)
    
    # Read Excel
    df = pd.read_excel(excel_path, nrows=limit)
    print(f"\nValidating {len(df)} rows from {excel_path}")
    
    issues = defaultdict(list)
    warnings = defaultdict(list)
    stats = {
        'total_rows': len(df),
        'unique_windfarms': set(),
        'unique_generation_units': set(),
        'missing_countries': 0,
        'missing_windfarm_names': 0,
        'duplicate_gen_units': set(),
        'invalid_statuses': set(),
    }
    
    # Check each row
    for idx, row in df.iterrows():
        row_num = idx + 1
        
        # Required: windfarm_name
        windfarm_name = str(row.get('windfarm_name', '')).strip() if pd.notna(row.get('windfarm_name')) else ''
        if not windfarm_name:
            issues['missing_windfarm_name'].append(row_num)
            stats['missing_windfarm_names'] += 1
        else:
            stats['unique_windfarms'].add(windfarm_name)
        
        # Required: country
        country = str(row.get('country', '')).strip() if pd.notna(row.get('country')) else ''
        if not country and windfarm_name:  # Only check if windfarm exists
            issues['missing_country'].append(f"Row {row_num}: {windfarm_name}")
            stats['missing_countries'] += 1
        
        # Generation unit name
        gen_unit_name = str(row.get('generation_unit_name', '')).strip() if pd.notna(row.get('generation_unit_name')) else ''
        if gen_unit_name:
            unit_key = f"{windfarm_name}|{gen_unit_name}"
            if unit_key in stats['unique_generation_units']:
                stats['duplicate_gen_units'].add(unit_key)
                warnings['duplicate_units'].append(f"Row {row_num}: {gen_unit_name} (windfarm: {windfarm_name})")
            stats['unique_generation_units'].add(unit_key)
        
        # Validate status
        status = str(row.get('status', '')).strip() if pd.notna(row.get('status')) else ''
        if status:
            valid_statuses = ['Operational', 'Decommissioned', 'Under Installation', 'Expanded']
            if status not in valid_statuses:
                stats['invalid_statuses'].add(status)
                warnings['invalid_status'].append(f"Row {row_num}: '{status}'")
        
        # Check capacity
        capacity = row.get('nameplate_capacity_mw')
        if pd.notna(capacity):
            try:
                cap_value = float(capacity)
                if cap_value <= 0:
                    warnings['invalid_capacity'].append(f"Row {row_num}: {cap_value} MW")
                elif cap_value > 10000:  # Suspiciously high
                    warnings['suspicious_capacity'].append(f"Row {row_num}: {cap_value} MW")
            except:
                warnings['unparseable_capacity'].append(f"Row {row_num}: {capacity}")
        
        # Check dates
        cod = row.get('commercial_operational_date')
        if pd.notna(cod):
            try:
                date_val = pd.to_datetime(cod)
                if date_val.year < 1950 or date_val.year > 2050:
                    warnings['suspicious_date'].append(f"Row {row_num}: COD {date_val.date()}")
            except:
                warnings['unparseable_date'].append(f"Row {row_num}: COD {cod}")
        
        # Check coordinates
        lat = row.get('centroid_latitude')
        lng = row.get('centroid_longitude')
        if pd.notna(lat) and pd.notna(lng):
            try:
                lat_val = float(lat)
                lng_val = float(lng)
                if not (-90 <= lat_val <= 90):
                    warnings['invalid_coordinates'].append(f"Row {row_num}: lat={lat_val}")
                if not (-180 <= lng_val <= 180):
                    warnings['invalid_coordinates'].append(f"Row {row_num}: lng={lng_val}")
            except:
                warnings['unparseable_coordinates'].append(f"Row {row_num}: lat={lat}, lng={lng}")
        
        # Check ownership percentages
        owner_pct_total = 0
        for i in range(1, 6):
            pct_col = f'percentage{i}'
            if pct_col in row and pd.notna(row[pct_col]):
                try:
                    pct = float(str(row[pct_col]).replace('%', '').strip())
                    if pct <= 1:  # Assume decimal
                        pct = pct * 100
                    owner_pct_total += pct
                except:
                    pass
        
        if owner_pct_total > 0 and abs(owner_pct_total - 100) > 0.1:
            warnings['ownership_mismatch'].append(f"Row {row_num}: {owner_pct_total:.1f}% (windfarm: {windfarm_name})")
    
    # Print summary
    print("\n" + "-"*60)
    print("VALIDATION SUMMARY")
    print("-"*60)
    
    print(f"\n📊 Statistics:")
    print(f"  Total rows: {stats['total_rows']}")
    print(f"  Unique windfarms: {len(stats['unique_windfarms'])}")
    print(f"  Unique generation units: {len(stats['unique_generation_units'])}")
    print(f"  Duplicate generation units: {len(stats['duplicate_gen_units'])}")
    
    if issues:
        print(f"\n❌ Critical Issues (must fix):")
        for issue_type, items in issues.items():
            print(f"\n  {issue_type.replace('_', ' ').title()}: {len(items)} issues")
            for item in items[:5]:  # Show first 5
                print(f"    - {item}")
            if len(items) > 5:
                print(f"    ... and {len(items) - 5} more")
    else:
        print(f"\n✅ No critical issues found")
    
    if warnings:
        print(f"\n⚠️  Warnings (should review):")
        for warning_type, items in warnings.items():
            print(f"\n  {warning_type.replace('_', ' ').title()}: {len(items)} warnings")
            for item in items[:3]:  # Show first 3
                print(f"    - {item}")
            if len(items) > 3:
                print(f"    ... and {len(items) - 3} more")
    else:
        print(f"\n✅ No warnings found")
    
    # Data quality score
    total_issues = sum(len(items) for items in issues.values())
    total_warnings = sum(len(items) for items in warnings.values())
    quality_score = max(0, 100 - (total_issues * 5) - (total_warnings * 1))
    
    print(f"\n📈 Data Quality Score: {quality_score}/100")
    
    if quality_score >= 90:
        print("   ✅ Excellent - Ready for import")
    elif quality_score >= 70:
        print("   ⚠️  Good - Review warnings before import")
    elif quality_score >= 50:
        print("   ⚠️  Fair - Fix critical issues before import")
    else:
        print("   ❌ Poor - Significant data quality issues")
    
    print("\n" + "="*60)
    
    return quality_score >= 50  # Return True if data is acceptable


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate Excel data')
    parser.add_argument('--excel', default='scripts/seeds/generation_unit_seed.xlsx',
                        help='Path to Excel file')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rows to validate')
    args = parser.parse_args()
    
    excel_path = Path(args.excel)
    if not excel_path.exists():
        print(f"Error: Excel file not found: {excel_path}")
        sys.exit(1)
    
    is_valid = validate_excel_data(str(excel_path), args.limit)
    sys.exit(0 if is_valid else 1)


if __name__ == "__main__":
    main()