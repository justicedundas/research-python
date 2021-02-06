from requests import HTTPError


class WBRequestError(HTTPError):
    """An error occured when downloading the WB data"""


TIME_ARGS = {
    "A": {"prefix": "annual", "period_type": "12M"},
    "Q": {"prefix": "quarterly", "period_type": "3M"},
    "M": {"prefix": "monthly", "period_type": "1M"},
    None: {},
}

_freq = list(TIME_ARGS.keys())


_per_page = 25000
_format = "json"

_CONFIG = {
    "wb": {
        "path": "https://api.worldbank.org/v2/{module}",
        "query": {
            "formatted": {"required": False, "default": False},
            "params": {
                "required": True,
                "default": {"format": _format, "per_page": _per_page},
            },
        },
        # "modules": {
        #     "required": True,
        #     "default": None,
        #     "options": list(_MODULES_DICT.keys()),
        # },
        "country": {
            "extract_values": ["region", "adminregion", "incomeLevel", "lendingType"],
            "convert": {"float": ["latitude", "longitude"]},
            "rename": {"id": "iso3", "iso2Code": "iso"},
            "id_col": "iso",
            "outfile": "WorldBank-Countries.csv",
        },
    },
    "geonames": {
        "countries": {
            "filenames": ["countryInfo.txt"],
            "comment": "#",
            "names": [
                "ISO",
                "ISO3",
                "ISO-Numeric",
                "fips",
                "Country",
                "Capital",
                "Area(in sq km)",
                "Population",
                "Continent",
                "tld",
                "CurrencyCode",
                "CurrencyName",
                "Phone",
                "Postal Code Format",
                "Postal Code Regex",
                "Languages",
                "geonameid",
                "neighbours",
                "EquivalentFipsCode",
            ],
            "low_memory": True,
            "keep_cols": [
                "id",
                "name",
                "iso",
                "iso3",
                "isoNumeric",
                "areaSqKm",
                "currencyCode",
                "currencyName",
                "geonameId",
                "neighbours",
                "languages",
            ],
            "rename_cols": {
                "ISO": "iso",
                "ISO3": "iso3",
                "ISO-Numeric": "isoNumeric",
                "Country": "name",
                "Population": "population",
                "Area(in sq km)": "areaSqKm",
                "geonameid": "geonameId",
                "Languages": "languages",
                "CurrencyCode": "currencyCode",
                "CurrencyName": "currencyName",
            },
            "header": None,
            "outfile": "GeoNames-Countries.csv",
        },
        "cities": {
            "filenames": [
                "cities15000.zip",
                "cities5000.zip",
                "cities1000.zip",
                "cities500.zip",
            ],
            "names": [
                "geonameid",
                "name",
                "asciiname",
                "alternatenames",
                "latitude",
                "longitude",
                "feature class",
                "feature code",
                "country code",
                "cc2",
                "admin1 code",
                "admin2 code",
                "admin3 code",
                "admin4 code",
                "population",
                "elevation",
                "dem",
                "timezone",
                "modification date",
            ],
            "comment": None,
            "low_memory": False,
            "keep_cols": ["id", "name", "parentId", "geonameId"],
            "rename_cols": {"geonameid": "geonameId", "asciiname": "name"},
            "header": None,
            "outfile": "GeoNames-Cities.csv",
        },
        "admin1": {
            "filenames": ["admin1CodesASCII.txt"],
            "names": ["code", "name", "name_ascii", "geonameid"],
            "comment": None,
            "low_memory": True,
            "keep_cols": ["id", "name", "code", "parentId", "geonameId"],
            "rename_cols": {
                "code": "id",
                "name_ascii": "name",
                "geonameid": "geonameId",
            },
            "header": None,
            "outfile": "GeoNames-Admin1.csv",
        },
        "admin2": {
            "filenames": ["admin2Codes.txt"],
            "names": ["code", "name", "name_ascii", "geonameid"],
            "comment": None,
            "low_memory": True,
            "keep_cols": ["id", "name", "parentId", "geonameId"],
            "rename_cols": {
                "code": "id",
                "name_ascii": "name",
                "geonameid": "geonameId",
            },
            "header": None,
            "outfile": "GeoNames-Admin2.csv",
        },
        "coordinates": {
            "filenames": ["allCountries.zip"],
            "names": [
                "geonameid",
                "name",
                "asciiname",
                "alternatenames",
                "latitude",
                "longitude",
                "feature class",
                "feature code",
                "country code",
                "cc2",
                "admin1 code",
                "admin2 code",
                "admin3 code",
                "admin4 code",
                "population",
                "elevation",
                "dem",
                "timezone",
                "modification date",
            ],
            "comment": None,
            "low_memory": False,
            "keep_cols": ["geonameId", "latitude", "longitude", "population"],
            "rename_cols": {"geonameid": "geonameId", "asciiname": "name"},
            "header": None,
            "outfile": "GeoNames-Coordinates.csv",
        },
        # "postalcodes": {
        #     "filenames": ["allCountries.zip"],
        #     "names": [
        #         "country code",
        #         "postal code",
        #         "place name",
        #         "admin name1",
        #         "admin code1",
        #         "admin name2",
        #         "admin code2",
        #         "admin name3",
        #         "admin code3",
        #         "latitude",
        #         "longitude",
        #         "accuracy",
        #     ],
        #     "rename_cols": {"postal code": "postalCode", "place name": "placeName"},
        #     "keep_cols": [
        #         "postalCode",
        #         "admin2_id",
        #         "placeName",
        #         "latitude",
        #         "longitude",
        #     ],
        #     "comment": None,
        #     "low_memory": False,
        #     "outfile": "GeoNames-PostalCodes.csv",
        #     "header": 0,
        # },
        # "continents": {
        #     "data": {
        #         "code": ["AF", "AS", "EU", "NA", "OC", "SA", "AN"],
        #         "name": [
        #             "Africa",
        #             "Asia",
        #             "Europe",
        #             "North America",
        #             "Oceania",
        #             "South America",
        #             "Antarctica",
        #         ],
        #         "geonameId": [
        #             "6255146",
        #             "6255147",
        #             "6255148",
        #             "6255149",
        #             "6255151",
        #             "6255150",
        #             "6255152",
        #         ],
        #     },
        #     "outfile": "GeoName-Continents.csv",
        # },
    },
}


temp = {
    "un": {
        "regions": {
            "url": "https://unstats.un.org/unsd/methodology/m49/overview",
            "attrs": {"id": "downloadTableEN"},
            "rename_cols": {
                "Region Name": "UNRegion",
                "Region Code": "UNRegionCode",
                "Sub-region Name": "UNSubRegion",
                "Sub-region Code": "UNSubRegionCode",
                "Intermediate Region Name": "UNIntermediateRegion",
                "Intermediate Region Code": "UNIntermediateRegionCode",
                "ISO-alpha3 Code": "iso3",
            },
            "outfile": "UN-AllRegions.csv",
            "intermediate_outfile": "UN-IntermediateRegion.csv",
            "subregion_outfile": "UN-SubRegion.csv",
            "region_outfile": "UN-Region.csv",
        },
        "population": {
            "total": {
                "url": "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_TotalPopulationBySex.csv",
                "outfile": "UN-TotalPopulation.csv",
            },
            "age": {
                "url": "https://population.un.org/wpp/Download/Files/1_Indicators%20(Standard)/CSV_FILES/WPP2019_PopulationBySingleAgeSex_1950-2019.csv",
                "outfile": "UN-PopulationByAge.csv",
            },
        },
    }
}
