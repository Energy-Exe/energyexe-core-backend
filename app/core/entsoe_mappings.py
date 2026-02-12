"""Centralized ENTSOE area code to EIC code mappings.

Derived from the entsoe-py library Area enum. Used for:
- Converting short area codes (e.g., 'NO_1') to EIC codes (e.g., '10YNO-1--------2')
  when storing API-fetched price data in price_data_raw
- Defining which bidzones to import prices for in daily cron jobs
"""

# Short area code → EIC code mapping
AREA_CODE_TO_EIC = {
    "DE_LU": "10Y1001A1001A82H",
    "BE": "10YBE----------2",
    "DK_1": "10YDK-1--------W",
    "DK_2": "10YDK-2--------M",
    "FR": "10YFR-RTE------C",
    "GB": "10YGB----------A",
    "NL": "10YNL----------L",
    "NO_1": "10YNO-1--------2",
    "NO_2": "10YNO-2--------T",
    "NO_3": "10YNO-3--------J",
    "NO_4": "10YNO-4--------9",
    "NO_5": "10Y1001A1001A48H",
    "SE_1": "10Y1001A1001A44P",
    "SE_2": "10Y1001A1001A45N",
    "SE_3": "10Y1001A1001A46L",
    "SE_4": "10Y1001A1001A47J",
    "FI": "10YFI-1--------U",
    "ES": "10YES-REE------0",
    "AT": "10YAT-APG------L",
    "CZ": "10YCZ-CEPS-----N",
    "PL": "10YPL-AREA-----S",
    "HU": "10YHU-MAVIR----U",
    "SK": "10YSK-SEPS-----K",
    "RO": "10YRO-TEL------P",
    "GR": "10YGR-HTSO-----Y",
    "SI": "10YSI-ELES-----O",
    "HR": "10YHR-HEP------M",
    "BG": "10YCA-BULGARIA-R",
    "RS": "10YCS-SERBIATSOV",
    "LT": "10YLT-1001A0008Q",
    "LV": "10YLV-1001A00074",
    "EE": "10Y1001A1001A39I",
    "ME": "10YCS-CG-TSO---S",
    "MK": "10YMK-MEPSO----8",
    "CH": "10YCH-SWISSGRIDZ",
    "PT": "10YPT-REN------W",
    "BA": "10YBA-JPCC-----D",
    "AL": "10YAL-KESH-----5",
}

# Reverse mapping: EIC code → short area code
EIC_TO_AREA_CODE = {v: k for k, v in AREA_CODE_TO_EIC.items()}

# Bidzones that have windfarms needing daily price imports.
# These are the 11 bidzones with active windfarms in the system.
PRICE_IMPORT_BIDZONES = [
    "DE_LU",
    "BE",
    "DK_1",
    "DK_2",
    "FR",
    "GB",
    "NL",
    "NO_1",
    "NO_2",
    "NO_3",
    "NO_4",
]
