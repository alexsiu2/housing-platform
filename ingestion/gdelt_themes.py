"""
GDELT GKG themes that are about housing markets. Shared by the BigQuery
history pull (gdelt_client) and the live 15-minute producer
(streaming/gdelt_producer.py); no dependencies so either can import it.

Chosen from real data: keyword matches like 'RENT' or 'HOUS' also catch
UNREST_BELLIGERENT, EPU_POLICY_WHITE_HOUSE, WAREHOUSING... Broad ones such
as NEW_CONSTRUCTION and PROPERTY_LAWS are left out.
"""

HOUSING_THEMES = [
    "ECON_HOUSING_PRICES",
    "WB_612_HOUSING_FINANCE",
    "WB_817_LAND_AND_HOUSING",
    "WB_904_HOUSING_MARKETS",
    "WB_870_HOUSING_CONSTRUCTION",
    "WB_2186_SOCIAL_HOUSING",
    "WB_1722_HOUSING_POLICY_AND_INSTITUTIONS",
    "TAX_FNCACT_LANDLORD",
    "TAX_FNCACT_LANDLORDS",
]
