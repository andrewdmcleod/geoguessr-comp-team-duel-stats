#!/usr/bin/env python3
"""Test geocoding providers to verify API keys and connectivity."""

import json
import time
import sys

# Test coordinates: Eiffel Tower, Paris
TEST_LAT = 48.8584
TEST_LNG = 2.2945
EXPECTED_COUNTRY = "France"

with open('config.json') as f:
    config = json.load(f)


def test_nominatim():
    from geopy.geocoders import Nominatim
    print("Testing Nominatim...")
    geocoder = Nominatim(user_agent="geoguessr_stats_tool", timeout=10)
    try:
        location = geocoder.reverse(f"{TEST_LAT}, {TEST_LNG}", language='en')
        if location and 'address' in location.raw:
            country = location.raw['address'].get('country')
            print(f"  Result: {country}")
            assert country == EXPECTED_COUNTRY, f"Expected {EXPECTED_COUNTRY}, got {country}"
            print("  ✅ Nominatim OK")
            return True
        print("  ❌ No result returned")
        return False
    except Exception as e:
        print(f"  ❌ Nominatim failed: {e}")
        return False


def test_opencage():
    import requests
    api_key = config.get('opencage_api_key')
    if not api_key:
        print("  ⏭️  Skipped (no opencage_api_key in config.json)")
        return None

    print("Testing OpenCage...")
    url = "https://api.opencagedata.com/geocode/v1/json"
    params = {
        'q': f"{TEST_LAT},{TEST_LNG}",
        'key': api_key,
        'no_annotations': 1,
        'language': 'en',
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        status = data.get('status', {})
        print(f"  API status: {status.get('code')} - {status.get('message')}")

        rate = data.get('rate', {})
        if rate:
            print(f"  Rate limit: {rate.get('remaining')}/{rate.get('limit')} remaining, resets {rate.get('reset')}")

        results = data.get('results', [])
        if results:
            country = results[0].get('components', {}).get('country')
            print(f"  Result: {country}")
            assert country == EXPECTED_COUNTRY, f"Expected {EXPECTED_COUNTRY}, got {country}"
            print("  ✅ OpenCage OK")
            return True
        print("  ❌ No results returned")
        return False
    except Exception as e:
        print(f"  ❌ OpenCage failed: {e}")
        return False


def test_google():
    import requests
    api_key = config.get('google_maps_api_key')
    if not api_key:
        print("  ⏭️  Skipped (no google_maps_api_key in config.json)")
        return None

    print("Testing Google Maps Geocoding...")
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        'latlng': f"{TEST_LAT},{TEST_LNG}",
        'key': api_key,
        'result_type': 'country',
        'language': 'en',
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        status = data.get('status')
        print(f"  API status: {status}")

        if status == 'REQUEST_DENIED':
            error_msg = data.get('error_message', 'Unknown error')
            print(f"  ❌ Request denied: {error_msg}")
            return False

        results = data.get('results', [])
        if results:
            for result in results:
                for component in result.get('address_components', []):
                    if 'country' in component.get('types', []):
                        country = component.get('long_name')
                        print(f"  Result: {country}")
                        assert country == EXPECTED_COUNTRY, f"Expected {EXPECTED_COUNTRY}, got {country}"
                        print("  ✅ Google OK")
                        return True
        print(f"  ❌ No country found in results (got {len(results)} results)")
        return False
    except Exception as e:
        print(f"  ❌ Google failed: {e}")
        return False


if __name__ == '__main__':
    print(f"Geocoding test: ({TEST_LAT}, {TEST_LNG}) → expected '{EXPECTED_COUNTRY}'")
    print("=" * 50)

    results = {}

    results['nominatim'] = test_nominatim()
    time.sleep(1)
    results['opencage'] = test_opencage()
    time.sleep(1)
    results['google'] = test_google()

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    for provider, result in results.items():
        if result is True:
            print(f"  {provider}: ✅ Working")
        elif result is False:
            print(f"  {provider}: ❌ Failed")
        else:
            print(f"  {provider}: ⏭️  Skipped")

    # Exit with error if any tested provider failed
    if any(r is False for r in results.values()):
        sys.exit(1)
