"""Unit tests for country_codes.py normalization."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from country_codes import normalize_country_name, COUNTRY_ALIASES


class TestNormalizeCountryName:
    def test_czechia_to_czech_republic(self):
        assert normalize_country_name('Czechia') == 'Czech Republic'

    def test_turkiye_to_turkey(self):
        assert normalize_country_name('Türkiye') == 'Turkey'

    def test_eswatini_to_swaziland(self):
        assert normalize_country_name('Eswatini') == 'Swaziland'

    def test_timor_leste_to_east_timor(self):
        assert normalize_country_name('Timor-Leste') == 'East Timor'

    def test_cabo_verde_to_cape_verde(self):
        assert normalize_country_name('Cabo Verde') == 'Cape Verde'

    def test_passthrough_normal_country(self):
        assert normalize_country_name('France') == 'France'

    def test_passthrough_unknown(self):
        assert normalize_country_name('Unknown') == 'Unknown'

    def test_passthrough_empty(self):
        assert normalize_country_name('') == ''

    def test_passthrough_none(self):
        assert normalize_country_name(None) is None

    def test_passthrough_lost_at_sea(self):
        assert normalize_country_name('Lost at Sea') == 'Lost at Sea'

    def test_all_aliases_defined(self):
        """All aliases should map to a non-empty string."""
        for alias, canonical in COUNTRY_ALIASES.items():
            assert isinstance(alias, str)
            assert isinstance(canonical, str)
            assert len(canonical) > 0
