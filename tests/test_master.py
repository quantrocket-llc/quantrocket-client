# Copyright 2019 QuantRocket LLC - All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# To run: python -m unittest discover -s tests/ -p test*.py -t .

import unittest
try:
    from unittest.mock import patch
except ImportError:
    # py27
    from mock import patch
import pandas as pd
import pytz
import numpy as np
from quantrocket.master import (
    get_securities,
    get_securities_reindexed_like,
    get_contract_nums_reindexed_like)
from quantrocket.exceptions import ParameterError

class GetSecuritiesTestCase(unittest.TestCase):

    @patch("quantrocket.master.download_master_file")
    def test_pass_args_correctly(self, mock_download_master_file):
        """
        Tests that args are correctly passed to the download_master_file
        function.
        """
        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                          "FI23456"],
                     Symbol=["ABC",
                             "DEF"],
                     Etf=[1,
                          0],
                     Delisted=[0,
                               1],
                     Currency=["USD",
                               "USD"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        kwargs = dict(
            symbols=["ABC", "DEF"],
            exchanges="XNYS",
            currencies="USD",
            sec_types=["STK", "ETF"],
            universes="my-universe",
            sids=["FI12345", "FI23456"],
            exclude_universes="exclude-me",
            exclude_sids="FI34567",
            exclude_delisted=True,
            exclude_expired=True,
            frontmonth=True,
            vendors=["usstock"],
            fields=["Symbol", "Etf", "Delisted", "Currency"]
        )

        get_securities(**kwargs)

        download_master_file_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = download_master_file_call
        self.assertListEqual(kwargs["symbols"], ["ABC", "DEF"])
        self.assertEqual(kwargs["exchanges"], "XNYS")
        self.assertEqual(kwargs["currencies"], "USD")
        self.assertListEqual(kwargs["sec_types"], ["STK", "ETF"])
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertEqual(kwargs["universes"], "my-universe")
        self.assertEqual(kwargs["exclude_universes"], "exclude-me")
        self.assertEqual(kwargs["exclude_sids"], "FI34567")
        self.assertTrue(kwargs["exclude_delisted"])
        self.assertTrue(kwargs["exclude_expired"])
        self.assertTrue(kwargs["frontmonth"])
        self.assertListEqual(kwargs["vendors"], ["usstock"])
        self.assertListEqual(kwargs["fields"], ["Symbol", "Etf", "Delisted", "Currency"])

    @patch("quantrocket.master.download_master_file")
    def test_cast_boolean_and_date_fields(self, mock_download_master_file):
        """
        Tests that master fields are correctly cast to boolean and datetime.
        """
        self.maxDiff = None
        def _mock_download_master_file(f, *args, **kwargs):
            stks = [{
                'Sid': 'FIBBG000B9XRY4',
                'Symbol': 'AAPL',
                'Exchange': 'XNAS',
                'Country': 'US',
                'Currency': 'USD',
                'SecType': 'STK',
                'Etf': 0,
                'Timezone': 'America/New_York',
                'Name': 'APPLE INC',
                'PriceMagnifier': 1,
                'Multiplier': 1,
                'Delisted': 0,
                'DateDelisted': None,
                'LastTradeDate': None,
                'RolloverDate': None,
                'alpaca_AssetClass': 'us_equity',
                'alpaca_AssetId': 'b0b6dd9d-8b9b-48a9-ba46-b9d54906e415',
                'alpaca_EasyToBorrow': 1,
                'alpaca_Exchange': 'NASDAQ',
                'alpaca_Marginable': 1,
                'alpaca_Name': 'Apple Inc. Common Stock',
                'alpaca_Shortable': 1,
                'alpaca_Status': 'active',
                'alpaca_Symbol': 'AAPL',
                'alpaca_Tradable': 1,
                'edi_Cik': 320193,
                'edi_CountryInc': 'United States of America',
                'edi_CountryListed': 'United States of America',
                'edi_Currency': 'USD',
                'edi_DateDelisted': None,
                'edi_ExchangeListingStatus': 'Listed',
                'edi_FirstPriceDate': '2017-03-01',
                'edi_GlobalListingStatus': 'Active',
                'edi_Industry': 'Information Technology',
                'edi_IsPrimaryListing': 1,
                'edi_IsoCountryInc': 'US',
                'edi_IsoCountryListed': 'US',
                'edi_IssuerId': 30017,
                'edi_IssuerName': 'Apple Inc',
                'edi_LastPriceDate': '2020-11-04',
                'edi_LocalSymbol': 'AAPL',
                'edi_Mic': 'XNAS',
                'edi_MicSegment': 'XNGS',
                'edi_MicTimezone': 'America/New_York',
                'edi_PreferredName': 'Apple Inc',
                'edi_PrimaryMic': 'XNAS',
                'edi_RecordCreated': '2001-05-05',
                'edi_RecordModified': '2020-11-02 03:37:23',
                'edi_SecId': 33449,
                'edi_SecTypeCode': 'EQS',
                'edi_SecTypeDesc': 'Equity Shares',
                'edi_SecurityDesc': 'Ordinary Shares',
                'edi_Sic': 'Electronic Computers',
                'edi_SicCode': 3571,
                'edi_SicDivision': 'Manufacturing',
                'edi_SicIndustryGroup': 'Computer And Office Equipment',
                'edi_SicMajorGroup': 'Industrial And Commercial Machinery And Computer Equipment',
                'edi_StructureCode': None,
                'edi_StructureDesc': None,
                'ibkr_AggGroup': 1,
                'ibkr_Category': 'Computers',
                'ibkr_ComboLegs': None,
                'ibkr_ConId': 265598,
                'ibkr_ContractMonth': None,
                'ibkr_Currency': 'USD',
                'ibkr_Cusip': None,
                'ibkr_DateDelisted': None,
                'ibkr_Delisted': 0,
                'ibkr_Etf': 0,
                'ibkr_EvMultiplier': 0.0,
                'ibkr_EvRule': None,
                'ibkr_Industry': 'Computers',
                'ibkr_Isin': 'US0378331005',
                'ibkr_LastTradeDate': None,
                'ibkr_LocalSymbol': 'AAPL',
                'ibkr_LongName': 'APPLE INC',
                'ibkr_MarketName': 'NMS',
                'ibkr_MarketRuleIds': '26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26',
                'ibkr_MdSizeMultiplier': 100,
                'ibkr_MinTick': 0.01,
                'ibkr_Multiplier': None,
                'ibkr_PriceMagnifier': 1,
                'ibkr_PrimaryExchange': 'NASDAQ',
                'ibkr_RealExpirationDate': None,
                'ibkr_Right': None,
                'ibkr_SecType': 'STK',
                'ibkr_Sector': 'Technology',
                'ibkr_Strike': 0.0,
                'ibkr_Symbol': 'AAPL',
                'ibkr_Timezone': 'America/New_York',
                'ibkr_TradingClass': 'NMS',
                'ibkr_UnderConId': 0,
                'ibkr_UnderSecType': None,
                'ibkr_UnderSymbol': None,
                'ibkr_ValidExchanges': 'SMART,AMEX,NYSE,CBOE,PHLX,ISE,CHX,ARCA,ISLAND,DRCTEDGE,BEX,BATS,EDGEA,CSFBALGO,JEFFALGO,BYX,IEX,EDGX,FOXRIVER,TPLUS1,NYSENAT,LTSE,MEMX,PSX',
                'sharadar_Category': 'Domestic Common Stock',
                'sharadar_CompanySite': 'http://www.apple.com',
                'sharadar_CountryListed': 'US',
                'sharadar_Currency': 'USD',
                'sharadar_Cusips': 37833100,
                'sharadar_DateDelisted': None,
                'sharadar_Delisted': 0,
                'sharadar_Exchange': 'NASDAQ',
                'sharadar_FamaIndustry': 'Computers',
                'sharadar_FamaSector': None,
                'sharadar_FirstAdded': '2014-09-24',
                'sharadar_FirstPriceDate': '1986-01-01',
                'sharadar_FirstQuarter': '1996-09-30',
                'sharadar_Industry': 'Consumer Electronics',
                'sharadar_LastPriceDate': '2020-11-04',
                'sharadar_LastQuarter': '2020-09-30',
                'sharadar_LastUpdated': '2020-11-04',
                'sharadar_Location': 'California; U.S.A',
                'sharadar_Name': 'Apple Inc',
                'sharadar_Permaticker': 199059,
                'sharadar_RelatedTickers': None,
                'sharadar_ScaleMarketCap': '6 - Mega',
                'sharadar_ScaleRevenue': '6 - Mega',
                'sharadar_SecFilings': 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193',
                'sharadar_Sector': 'Technology',
                'sharadar_SicCode': 3571,
                'sharadar_SicIndustry': 'Electronic Computers',
                'sharadar_SicSector': 'Manufacturing',
                'sharadar_Ticker': 'AAPL',
                'usstock_DateDelisted': None,
                'usstock_FirstPriceDate': '2007-01-03',
                'usstock_LastPriceDate': '2020-11-18',
                'usstock_Mic': 'XNAS',
                'usstock_Name': 'APPLE INC',
                'usstock_SecurityType': 'Common Stock',
                'usstock_SecurityType2': 'Common Stock',
                'usstock_Sic': 'Electronic Computers',
                'usstock_SicCode': 3571,
                'usstock_SicDivision': 'Manufacturing',
                'usstock_SicIndustryGroup': 'Computer And Office Equipment',
                'usstock_SicMajorGroup': 'Industrial And Commercial Machinery And Computer Equipment',
                'usstock_Symbol': 'AAPL'},
                {'Sid': 'FIBBG000001PV7',
                'Symbol': 'GOODO',
                'Exchange': 'XNAS',
                'Country': 'US',
                'Currency': 'USD',
                'SecType': 'STK',
                'Etf': 0,
                'Timezone': 'America/New_York',
                'Name': 'GLADSTONE COMMERCIAL COR',
                'PriceMagnifier': 1,
                'Multiplier': 1,
                'Delisted': 1,
                'DateDelisted': '2019-10-25',
                'LastTradeDate': None,
                'RolloverDate': None,
                'edi_Cik': 1234006,
                'edi_CountryInc': 'United States of America',
                'edi_CountryListed': 'United States of America',
                'edi_Currency': 'USD',
                'edi_DateDelisted': '2019-10-25',
                'edi_ExchangeListingStatus': 'Delisted',
                'edi_FirstPriceDate': '2017-03-01',
                'edi_GlobalListingStatus': 'Inactive',
                'edi_Industry': 'Real Estate',
                'edi_IsPrimaryListing': 1,
                'edi_IsoCountryInc': 'US',
                'edi_IsoCountryListed': 'US',
                'edi_IssuerId': 74929,
                'edi_IssuerName': 'Gladstone Commercial Corp',
                'edi_LastPriceDate': '2019-10-25',
                'edi_LocalSymbol': 'GOODO',
                'edi_Mic': 'XNAS',
                'edi_MicSegment': 'XNGS',
                'edi_MicTimezone': 'America/New_York',
                'edi_PreferredName': 'Gladstone Commercial Corp',
                'edi_PrimaryMic': 'XNAS',
                'edi_RecordCreated': '2006-10-26',
                'edi_RecordModified': '2019-11-04 03:52:18',
                'edi_SecId': 426219,
                'edi_SecTypeCode': 'PRF',
                'edi_SecTypeDesc': 'Preference Share',
                'edi_SecurityDesc': '7.50% PRF PERPETUAL USD 25 - Reg S Ser B',
                'edi_Sic': 'Lessors of Real Property, Not Elsewhere Classified',
                'edi_SicCode': 6519,
                'edi_SicDivision': 'Finance, Insurance, And Real Estate',
                'edi_SicIndustryGroup': 'Real Estate Operators (except Developers) And Lessors',
                'edi_SicMajorGroup': 'Real Estate',
                'edi_StructureCode': 'REIT',
                'edi_StructureDesc': 'Real Estate Investment Trust',
                'sharadar_Category': 'Domestic Preferred Stock',
                'sharadar_CompanySite': None,
                'sharadar_CountryListed': 'US',
                'sharadar_Currency': 'USD',
                'sharadar_Cusips': 376536306,
                'sharadar_DateDelisted': '2019-10-25',
                'sharadar_Delisted': 1,
                'sharadar_Exchange': 'NASDAQ',
                'sharadar_FamaIndustry': 'Real Estate',
                'sharadar_FamaSector': None,
                'sharadar_FirstAdded': '2018-12-30',
                'sharadar_FirstPriceDate': '2006-10-25',
                'sharadar_FirstQuarter': None,
                'sharadar_Industry': 'REIT - Diversified',
                'sharadar_LastPriceDate': '2019-10-25',
                'sharadar_LastQuarter': None,
                'sharadar_LastUpdated': '2020-07-01',
                'sharadar_Location': 'Virginia; U.S.A',
                'sharadar_Name': 'Gladstone Commercial Corp',
                'sharadar_Permaticker': 112800,
                'sharadar_RelatedTickers': 'GOODP GOODM GOOD',
                'sharadar_ScaleMarketCap': None,
                'sharadar_ScaleRevenue': None,
                'sharadar_SecFilings': None,
                'sharadar_Sector': 'Real Estate',
                'sharadar_SicCode': 6519,
                'sharadar_SicIndustry': 'Lessors Of Real Property Nec',
                'sharadar_SicSector': 'Finance Insurance And Real Estate',
                'sharadar_Ticker': 'GOODO',
                'usstock_DateDelisted': '2019-10-25',
                'usstock_FirstPriceDate': '2007-01-03',
                'usstock_LastPriceDate': '2019-10-25',
                'usstock_Mic': 'XNAS',
                'usstock_Name': 'GLADSTONE COMMERCIAL COR',
                'usstock_SecurityType': 'PUBLIC',
                'usstock_SecurityType2': 'Preferred Stock',
                'usstock_Sic': 'Lessors of Real Property, Not Elsewhere Classified',
                'usstock_SicCode': 6519,
                'usstock_SicDivision': 'Finance, Insurance, And Real Estate',
                'usstock_SicIndustryGroup': 'Real Estate Operators (except Developers) And Lessors',
                'usstock_SicMajorGroup': 'Real Estate',
                'usstock_Symbol': 'GOODO'},
                {'Sid': 'QF000000000004',
                'Symbol': 'ESH7',
                'Exchange': 'XCME',
                'Country': 'US',
                'Currency': 'USD',
                'SecType': 'FUT',
                'Etf': 0,
                'Timezone': 'America/Chicago',
                'Name': 'E-mini S&P 500',
                'PriceMagnifier': 1,
                'Multiplier': 50,
                'Delisted': 0,
                'DateDelisted': None,
                'LastTradeDate': '2017-03-17',
                'RolloverDate': '2017-03-15',
                'ibkr_AggGroup': 2147483647,
                'ibkr_Category': None,
                'ibkr_ComboLegs': None,
                'ibkr_ConId': 215465490,
                'ibkr_ContractMonth': 201703,
                'ibkr_Currency': 'USD',
                'ibkr_Cusip': None,
                'ibkr_DateDelisted': None,
                'ibkr_Delisted': 0,
                'ibkr_Etf': 0,
                'ibkr_EvMultiplier': 0.0,
                'ibkr_EvRule': None,
                'ibkr_Industry': None,
                'ibkr_Isin': None,
                'ibkr_LastTradeDate': '2017-03-17',
                'ibkr_LocalSymbol': 'ESH7',
                'ibkr_LongName': 'E-mini S&P 500',
                'ibkr_MarketName': 'ES',
                'ibkr_MarketRuleIds': None,
                'ibkr_MdSizeMultiplier': 1,
                'ibkr_MinTick': 0.25,
                'ibkr_Multiplier': 50,
                'ibkr_PriceMagnifier': 1,
                'ibkr_PrimaryExchange': 'GLOBEX',
                'ibkr_RealExpirationDate': None,
                'ibkr_Right': None,
                'ibkr_SecType': 'FUT',
                'ibkr_Sector': None,
                'ibkr_Strike': 0.0,
                'ibkr_Symbol': 'ES',
                'ibkr_Timezone': 'America/Chicago',
                'ibkr_TradingClass': 'ES',
                'ibkr_UnderConId': 11004968,
                'ibkr_UnderSecType': 'IND',
                'ibkr_UnderSymbol': 'ES',
                'ibkr_ValidExchanges': 'GLOBEX'}
            ]
            securities = pd.DataFrame(stks)
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

            securities = get_securities(sids=["FIBBG000B9XRY4", "FIBBG000001PV7", "QF000000000004"], fields="*")

        self.assertListEqual(
            securities.reset_index().sort_values("Sid").fillna('nan').to_dict(orient="records"),
            [{'Sid': 'FIBBG000001PV7',
            'Symbol': 'GOODO',
            'Exchange': 'XNAS',
            'Country': 'US',
            'Currency': 'USD',
            'SecType': 'STK',
            'Etf': False,
            'Timezone': 'America/New_York',
            'Name': 'GLADSTONE COMMERCIAL COR',
            'PriceMagnifier': 1,
            'Multiplier': 1,
            'Delisted': True,
            'DateDelisted': pd.Timestamp('2019-10-25 00:00:00'),
            'LastTradeDate': 'nan',
            'RolloverDate': 'nan',
            'alpaca_AssetClass': 'nan',
            'alpaca_AssetId': 'nan',
            'alpaca_EasyToBorrow': False,
            'alpaca_Exchange': 'nan',
            'alpaca_Marginable': False,
            'alpaca_Name': 'nan',
            'alpaca_Shortable': False,
            'alpaca_Status': 'nan',
            'alpaca_Symbol': 'nan',
            'alpaca_Tradable': False,
            'edi_Cik': 1234006.0,
            'edi_CountryInc': 'United States of America',
            'edi_CountryListed': 'United States of America',
            'edi_Currency': 'USD',
            'edi_DateDelisted': pd.Timestamp('2019-10-25 00:00:00'),
            'edi_ExchangeListingStatus': 'Delisted',
            'edi_FirstPriceDate': pd.Timestamp('2017-03-01 00:00:00'),
            'edi_GlobalListingStatus': 'Inactive',
            'edi_Industry': 'Real Estate',
            'edi_IsPrimaryListing': True,
            'edi_IsoCountryInc': 'US',
            'edi_IsoCountryListed': 'US',
            'edi_IssuerId': 74929.0,
            'edi_IssuerName': 'Gladstone Commercial Corp',
            'edi_LastPriceDate': pd.Timestamp('2019-10-25 00:00:00'),
            'edi_LocalSymbol': 'GOODO',
            'edi_Mic': 'XNAS',
            'edi_MicSegment': 'XNGS',
            'edi_MicTimezone': 'America/New_York',
            'edi_PreferredName': 'Gladstone Commercial Corp',
            'edi_PrimaryMic': 'XNAS',
            'edi_RecordCreated': pd.Timestamp('2006-10-26 00:00:00'),
            'edi_RecordModified': pd.Timestamp('2019-11-04 03:52:18'),
            'edi_SecId': 426219.0,
            'edi_SecTypeCode': 'PRF',
            'edi_SecTypeDesc': 'Preference Share',
            'edi_SecurityDesc': '7.50% PRF PERPETUAL USD 25 - Reg S Ser B',
            'edi_Sic': 'Lessors of Real Property, Not Elsewhere Classified',
            'edi_SicCode': 6519.0,
            'edi_SicDivision': 'Finance, Insurance, And Real Estate',
            'edi_SicIndustryGroup': 'Real Estate Operators (except Developers) And Lessors',
            'edi_SicMajorGroup': 'Real Estate',
            'edi_StructureCode': 'REIT',
            'edi_StructureDesc': 'Real Estate Investment Trust',
            'ibkr_AggGroup': 'nan',
            'ibkr_Category': 'nan',
            'ibkr_ComboLegs': 'nan',
            'ibkr_ConId': 'nan',
            'ibkr_ContractMonth': 'nan',
            'ibkr_Currency': 'nan',
            'ibkr_Cusip': 'nan',
            'ibkr_DateDelisted': 'nan',
            'ibkr_Delisted': False,
            'ibkr_Etf': False,
            'ibkr_EvMultiplier': 'nan',
            'ibkr_EvRule': 'nan',
            'ibkr_Industry': 'nan',
            'ibkr_Isin': 'nan',
            'ibkr_LastTradeDate': 'nan',
            'ibkr_LocalSymbol': 'nan',
            'ibkr_LongName': 'nan',
            'ibkr_MarketName': 'nan',
            'ibkr_MarketRuleIds': 'nan',
            'ibkr_MdSizeMultiplier': 'nan',
            'ibkr_MinTick': 'nan',
            'ibkr_Multiplier': 'nan',
            'ibkr_PriceMagnifier': 'nan',
            'ibkr_PrimaryExchange': 'nan',
            'ibkr_RealExpirationDate': 'nan',
            'ibkr_Right': 'nan',
            'ibkr_SecType': 'nan',
            'ibkr_Sector': 'nan',
            'ibkr_Strike': 'nan',
            'ibkr_Symbol': 'nan',
            'ibkr_Timezone': 'nan',
            'ibkr_TradingClass': 'nan',
            'ibkr_UnderConId': 'nan',
            'ibkr_UnderSecType': 'nan',
            'ibkr_UnderSymbol': 'nan',
            'ibkr_ValidExchanges': 'nan',
            'sharadar_Category': 'Domestic Preferred Stock',
            'sharadar_CompanySite': 'nan',
            'sharadar_CountryListed': 'US',
            'sharadar_Currency': 'USD',
            'sharadar_Cusips': 376536306.0,
            'sharadar_DateDelisted': pd.Timestamp('2019-10-25 00:00:00'),
            'sharadar_Delisted': True,
            'sharadar_Exchange': 'NASDAQ',
            'sharadar_FamaIndustry': 'Real Estate',
            'sharadar_FamaSector': 'nan',
            'sharadar_FirstAdded': pd.Timestamp('2018-12-30 00:00:00'),
            'sharadar_FirstPriceDate': pd.Timestamp('2006-10-25 00:00:00'),
            'sharadar_FirstQuarter': 'nan',
            'sharadar_Industry': 'REIT - Diversified',
            'sharadar_LastPriceDate': pd.Timestamp('2019-10-25 00:00:00'),
            'sharadar_LastQuarter': 'nan',
            'sharadar_LastUpdated': pd.Timestamp('2020-07-01 00:00:00'),
            'sharadar_Location': 'Virginia; U.S.A',
            'sharadar_Name': 'Gladstone Commercial Corp',
            'sharadar_Permaticker': 112800.0,
            'sharadar_RelatedTickers': 'GOODP GOODM GOOD',
            'sharadar_ScaleMarketCap': 'nan',
            'sharadar_ScaleRevenue': 'nan',
            'sharadar_SecFilings': 'nan',
            'sharadar_Sector': 'Real Estate',
            'sharadar_SicCode': 6519.0,
            'sharadar_SicIndustry': 'Lessors Of Real Property Nec',
            'sharadar_SicSector': 'Finance Insurance And Real Estate',
            'sharadar_Ticker': 'GOODO',
            'usstock_DateDelisted': pd.Timestamp('2019-10-25 00:00:00'),
            'usstock_FirstPriceDate': pd.Timestamp('2007-01-03 00:00:00'),
            'usstock_LastPriceDate': pd.Timestamp('2019-10-25 00:00:00'),
            'usstock_Mic': 'XNAS',
            'usstock_Name': 'GLADSTONE COMMERCIAL COR',
            'usstock_SecurityType': 'PUBLIC',
            'usstock_SecurityType2': 'Preferred Stock',
            'usstock_Sic': 'Lessors of Real Property, Not Elsewhere Classified',
            'usstock_SicCode': 6519.0,
            'usstock_SicDivision': 'Finance, Insurance, And Real Estate',
            'usstock_SicIndustryGroup': 'Real Estate Operators (except Developers) And Lessors',
            'usstock_SicMajorGroup': 'Real Estate',
            'usstock_Symbol': 'GOODO'},
            {'Sid': 'FIBBG000B9XRY4',
            'Symbol': 'AAPL',
            'Exchange': 'XNAS',
            'Country': 'US',
            'Currency': 'USD',
            'SecType': 'STK',
            'Etf': False,
            'Timezone': 'America/New_York',
            'Name': 'APPLE INC',
            'PriceMagnifier': 1,
            'Multiplier': 1,
            'Delisted': False,
            'DateDelisted': 'nan',
            'LastTradeDate': 'nan',
            'RolloverDate': 'nan',
            'alpaca_AssetClass': 'us_equity',
            'alpaca_AssetId': 'b0b6dd9d-8b9b-48a9-ba46-b9d54906e415',
            'alpaca_EasyToBorrow': True,
            'alpaca_Exchange': 'NASDAQ',
            'alpaca_Marginable': True,
            'alpaca_Name': 'Apple Inc. Common Stock',
            'alpaca_Shortable': True,
            'alpaca_Status': 'active',
            'alpaca_Symbol': 'AAPL',
            'alpaca_Tradable': True,
            'edi_Cik': 320193.0,
            'edi_CountryInc': 'United States of America',
            'edi_CountryListed': 'United States of America',
            'edi_Currency': 'USD',
            'edi_DateDelisted': 'nan',
            'edi_ExchangeListingStatus': 'Listed',
            'edi_FirstPriceDate': pd.Timestamp('2017-03-01 00:00:00'),
            'edi_GlobalListingStatus': 'Active',
            'edi_Industry': 'Information Technology',
            'edi_IsPrimaryListing': True,
            'edi_IsoCountryInc': 'US',
            'edi_IsoCountryListed': 'US',
            'edi_IssuerId': 30017.0,
            'edi_IssuerName': 'Apple Inc',
            'edi_LastPriceDate': pd.Timestamp('2020-11-04 00:00:00'),
            'edi_LocalSymbol': 'AAPL',
            'edi_Mic': 'XNAS',
            'edi_MicSegment': 'XNGS',
            'edi_MicTimezone': 'America/New_York',
            'edi_PreferredName': 'Apple Inc',
            'edi_PrimaryMic': 'XNAS',
            'edi_RecordCreated': pd.Timestamp('2001-05-05 00:00:00'),
            'edi_RecordModified': pd.Timestamp('2020-11-02 03:37:23'),
            'edi_SecId': 33449.0,
            'edi_SecTypeCode': 'EQS',
            'edi_SecTypeDesc': 'Equity Shares',
            'edi_SecurityDesc': 'Ordinary Shares',
            'edi_Sic': 'Electronic Computers',
            'edi_SicCode': 3571.0,
            'edi_SicDivision': 'Manufacturing',
            'edi_SicIndustryGroup': 'Computer And Office Equipment',
            'edi_SicMajorGroup': 'Industrial And Commercial Machinery And Computer Equipment',
            'edi_StructureCode': 'nan',
            'edi_StructureDesc': 'nan',
            'ibkr_AggGroup': 1.0,
            'ibkr_Category': 'Computers',
            'ibkr_ComboLegs': 'nan',
            'ibkr_ConId': 265598.0,
            'ibkr_ContractMonth': 'nan',
            'ibkr_Currency': 'USD',
            'ibkr_Cusip': 'nan',
            'ibkr_DateDelisted': 'nan',
            'ibkr_Delisted': False,
            'ibkr_Etf': False,
            'ibkr_EvMultiplier': 0.0,
            'ibkr_EvRule': 'nan',
            'ibkr_Industry': 'Computers',
            'ibkr_Isin': 'US0378331005',
            'ibkr_LastTradeDate': 'nan',
            'ibkr_LocalSymbol': 'AAPL',
            'ibkr_LongName': 'APPLE INC',
            'ibkr_MarketName': 'NMS',
            'ibkr_MarketRuleIds': '26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26',
            'ibkr_MdSizeMultiplier': 100.0,
            'ibkr_MinTick': 0.01,
            'ibkr_Multiplier': 'nan',
            'ibkr_PriceMagnifier': 1.0,
            'ibkr_PrimaryExchange': 'NASDAQ',
            'ibkr_RealExpirationDate': 'nan',
            'ibkr_Right': 'nan',
            'ibkr_SecType': 'STK',
            'ibkr_Sector': 'Technology',
            'ibkr_Strike': 0.0,
            'ibkr_Symbol': 'AAPL',
            'ibkr_Timezone': 'America/New_York',
            'ibkr_TradingClass': 'NMS',
            'ibkr_UnderConId': 0.0,
            'ibkr_UnderSecType': 'nan',
            'ibkr_UnderSymbol': 'nan',
            'ibkr_ValidExchanges': 'SMART,AMEX,NYSE,CBOE,PHLX,ISE,CHX,ARCA,ISLAND,DRCTEDGE,BEX,BATS,EDGEA,CSFBALGO,JEFFALGO,BYX,IEX,EDGX,FOXRIVER,TPLUS1,NYSENAT,LTSE,MEMX,PSX',
            'sharadar_Category': 'Domestic Common Stock',
            'sharadar_CompanySite': 'http://www.apple.com',
            'sharadar_CountryListed': 'US',
            'sharadar_Currency': 'USD',
            'sharadar_Cusips': 37833100.0,
            'sharadar_DateDelisted': 'nan',
            'sharadar_Delisted': False,
            'sharadar_Exchange': 'NASDAQ',
            'sharadar_FamaIndustry': 'Computers',
            'sharadar_FamaSector': 'nan',
            'sharadar_FirstAdded': pd.Timestamp('2014-09-24 00:00:00'),
            'sharadar_FirstPriceDate': pd.Timestamp('1986-01-01 00:00:00'),
            'sharadar_FirstQuarter': pd.Timestamp('1996-09-30 00:00:00'),
            'sharadar_Industry': 'Consumer Electronics',
            'sharadar_LastPriceDate': pd.Timestamp('2020-11-04 00:00:00'),
            'sharadar_LastQuarter': pd.Timestamp('2020-09-30 00:00:00'),
            'sharadar_LastUpdated': pd.Timestamp('2020-11-04 00:00:00'),
            'sharadar_Location': 'California; U.S.A',
            'sharadar_Name': 'Apple Inc',
            'sharadar_Permaticker': 199059.0,
            'sharadar_RelatedTickers': 'nan',
            'sharadar_ScaleMarketCap': '6 - Mega',
            'sharadar_ScaleRevenue': '6 - Mega',
            'sharadar_SecFilings': 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193',
            'sharadar_Sector': 'Technology',
            'sharadar_SicCode': 3571.0,
            'sharadar_SicIndustry': 'Electronic Computers',
            'sharadar_SicSector': 'Manufacturing',
            'sharadar_Ticker': 'AAPL',
            'usstock_DateDelisted': 'nan',
            'usstock_FirstPriceDate': pd.Timestamp('2007-01-03 00:00:00'),
            'usstock_LastPriceDate': pd.Timestamp('2020-11-18 00:00:00'),
            'usstock_Mic': 'XNAS',
            'usstock_Name': 'APPLE INC',
            'usstock_SecurityType': 'Common Stock',
            'usstock_SecurityType2': 'Common Stock',
            'usstock_Sic': 'Electronic Computers',
            'usstock_SicCode': 3571.0,
            'usstock_SicDivision': 'Manufacturing',
            'usstock_SicIndustryGroup': 'Computer And Office Equipment',
            'usstock_SicMajorGroup': 'Industrial And Commercial Machinery And Computer Equipment',
            'usstock_Symbol': 'AAPL'},
            {'Sid': 'QF000000000004',
            'Symbol': 'ESH7',
            'Exchange': 'XCME',
            'Country': 'US',
            'Currency': 'USD',
            'SecType': 'FUT',
            'Etf': False,
            'Timezone': 'America/Chicago',
            'Name': 'E-mini S&P 500',
            'PriceMagnifier': 1,
            'Multiplier': 50,
            'Delisted': False,
            'DateDelisted': 'nan',
            'LastTradeDate': pd.Timestamp('2017-03-17 00:00:00'),
            'RolloverDate': pd.Timestamp('2017-03-15 00:00:00'),
            'alpaca_AssetClass': 'nan',
            'alpaca_AssetId': 'nan',
            'alpaca_EasyToBorrow': False,
            'alpaca_Exchange': 'nan',
            'alpaca_Marginable': False,
            'alpaca_Name': 'nan',
            'alpaca_Shortable': False,
            'alpaca_Status': 'nan',
            'alpaca_Symbol': 'nan',
            'alpaca_Tradable': False,
            'edi_Cik': 'nan',
            'edi_CountryInc': 'nan',
            'edi_CountryListed': 'nan',
            'edi_Currency': 'nan',
            'edi_DateDelisted': 'nan',
            'edi_ExchangeListingStatus': 'nan',
            'edi_FirstPriceDate': 'nan',
            'edi_GlobalListingStatus': 'nan',
            'edi_Industry': 'nan',
            'edi_IsPrimaryListing': False,
            'edi_IsoCountryInc': 'nan',
            'edi_IsoCountryListed': 'nan',
            'edi_IssuerId': 'nan',
            'edi_IssuerName': 'nan',
            'edi_LastPriceDate': 'nan',
            'edi_LocalSymbol': 'nan',
            'edi_Mic': 'nan',
            'edi_MicSegment': 'nan',
            'edi_MicTimezone': 'nan',
            'edi_PreferredName': 'nan',
            'edi_PrimaryMic': 'nan',
            'edi_RecordCreated': 'nan',
            'edi_RecordModified': 'nan',
            'edi_SecId': 'nan',
            'edi_SecTypeCode': 'nan',
            'edi_SecTypeDesc': 'nan',
            'edi_SecurityDesc': 'nan',
            'edi_Sic': 'nan',
            'edi_SicCode': 'nan',
            'edi_SicDivision': 'nan',
            'edi_SicIndustryGroup': 'nan',
            'edi_SicMajorGroup': 'nan',
            'edi_StructureCode': 'nan',
            'edi_StructureDesc': 'nan',
            'ibkr_AggGroup': 2147483647.0,
            'ibkr_Category': 'nan',
            'ibkr_ComboLegs': 'nan',
            'ibkr_ConId': 215465490.0,
            'ibkr_ContractMonth': 201703.0,
            'ibkr_Currency': 'USD',
            'ibkr_Cusip': 'nan',
            'ibkr_DateDelisted': 'nan',
            'ibkr_Delisted': False,
            'ibkr_Etf': False,
            'ibkr_EvMultiplier': 0.0,
            'ibkr_EvRule': 'nan',
            'ibkr_Industry': 'nan',
            'ibkr_Isin': 'nan',
            'ibkr_LastTradeDate': pd.Timestamp('2017-03-17 00:00:00'),
            'ibkr_LocalSymbol': 'ESH7',
            'ibkr_LongName': 'E-mini S&P 500',
            'ibkr_MarketName': 'ES',
            'ibkr_MarketRuleIds': 'nan',
            'ibkr_MdSizeMultiplier': 1.0,
            'ibkr_MinTick': 0.25,
            'ibkr_Multiplier': 50.0,
            'ibkr_PriceMagnifier': 1.0,
            'ibkr_PrimaryExchange': 'GLOBEX',
            'ibkr_RealExpirationDate': 'nan',
            'ibkr_Right': 'nan',
            'ibkr_SecType': 'FUT',
            'ibkr_Sector': 'nan',
            'ibkr_Strike': 0.0,
            'ibkr_Symbol': 'ES',
            'ibkr_Timezone': 'America/Chicago',
            'ibkr_TradingClass': 'ES',
            'ibkr_UnderConId': 11004968.0,
            'ibkr_UnderSecType': 'IND',
            'ibkr_UnderSymbol': 'ES',
            'ibkr_ValidExchanges': 'GLOBEX',
            'sharadar_Category': 'nan',
            'sharadar_CompanySite': 'nan',
            'sharadar_CountryListed': 'nan',
            'sharadar_Currency': 'nan',
            'sharadar_Cusips': 'nan',
            'sharadar_DateDelisted': 'nan',
            'sharadar_Delisted': False,
            'sharadar_Exchange': 'nan',
            'sharadar_FamaIndustry': 'nan',
            'sharadar_FamaSector': 'nan',
            'sharadar_FirstAdded': 'nan',
            'sharadar_FirstPriceDate': 'nan',
            'sharadar_FirstQuarter': 'nan',
            'sharadar_Industry': 'nan',
            'sharadar_LastPriceDate': 'nan',
            'sharadar_LastQuarter': 'nan',
            'sharadar_LastUpdated': 'nan',
            'sharadar_Location': 'nan',
            'sharadar_Name': 'nan',
            'sharadar_Permaticker': 'nan',
            'sharadar_RelatedTickers': 'nan',
            'sharadar_ScaleMarketCap': 'nan',
            'sharadar_ScaleRevenue': 'nan',
            'sharadar_SecFilings': 'nan',
            'sharadar_Sector': 'nan',
            'sharadar_SicCode': 'nan',
            'sharadar_SicIndustry': 'nan',
            'sharadar_SicSector': 'nan',
            'sharadar_Ticker': 'nan',
            'usstock_DateDelisted': 'nan',
            'usstock_FirstPriceDate': 'nan',
            'usstock_LastPriceDate': 'nan',
            'usstock_Mic': 'nan',
            'usstock_Name': 'nan',
            'usstock_SecurityType': 'nan',
            'usstock_SecurityType2': 'nan',
            'usstock_Sic': 'nan',
            'usstock_SicCode': 'nan',
            'usstock_SicDivision': 'nan',
            'usstock_SicIndustryGroup': 'nan',
            'usstock_SicMajorGroup': 'nan',
            'usstock_Symbol': 'nan'}]
                    )

class SecuritiesReindexedLikeTestCase(unittest.TestCase):

    @patch("quantrocket.master.download_master_file")
    def test_pass_sids_based_on_reindex_like(self, mock_download_master_file):
        """
        Tests that sids are correctly passed to the download_master_file
        function based on reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                          "FI23456"],
                     Symbol=["ABC",
                             "DEF"],
                     Etf=[1,
                          0],
                     Delisted=[0,
                               1],
                     Currency=["USD",
                               "USD"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        get_securities_reindexed_like(closes, fields=["Symbol", "Etf", "Delisted", "Currency"])

        download_master_file_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = download_master_file_call
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertListEqual(kwargs["fields"], ["Symbol", "Etf", "Delisted", "Currency"])

    @patch("quantrocket.master.download_master_file")
    def test_cast_boolean_fields(self, mock_download_master_file):
        """
        Tests that master fields Etf and Delisted are cast to boolean.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(dict(Sid=["FI12345","FI23456"],
                                           Symbol=["ABC","DEF"],
                                           Delisted=[0, 1],
                                           Etf=[1, 0],
                                           edi_Delisted=[0,1],
                                           ibkr_Etf=[1, 0]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        with patch("quantrocket.price.download_master_file", new=mock_download_master_file):

            securities = get_securities_reindexed_like(
                closes,
                fields=["Symbol", "Etf", "Delisted", "edi_Delisted", "ibkr_Etf"])

        symbols = securities.loc["Symbol"]
        symbols = symbols.reset_index()
        symbols.loc[:, "Date"] = symbols.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        self.assertListEqual(
            symbols.to_dict(orient="records"),
            [{'Date': '2018-05-01T00:00:00', "FI12345": "ABC", "FI23456": "DEF"},
            {'Date': '2018-05-02T00:00:00', "FI12345": "ABC", "FI23456": "DEF"},
            {'Date': '2018-05-03T00:00:00', "FI12345": "ABC", "FI23456": "DEF"}]
        )

        for field in ("Delisted", "edi_Delisted"):
            delisted = securities.loc[field]
            delisted = delisted.reset_index()
            delisted.loc[:, "Date"] = delisted.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                delisted.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', "FI12345": False, "FI23456": True},
                {'Date': '2018-05-02T00:00:00', "FI12345": False, "FI23456": True},
                {'Date': '2018-05-03T00:00:00', "FI12345": False, "FI23456": True}]
            )

        for field in ("Etf", "ibkr_Etf"):
            etfs = securities.loc[field]
            etfs = etfs.reset_index()
            etfs.loc[:, "Date"] = etfs.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.assertListEqual(
                etfs.to_dict(orient="records"),
                [{'Date': '2018-05-01T00:00:00', "FI12345": True, "FI23456": False},
                {'Date': '2018-05-02T00:00:00', "FI12345": True, "FI23456": False},
                {'Date': '2018-05-03T00:00:00', "FI12345": True, "FI23456": False},
                ]
            )

    def test_securities_reindexed_like(self):
        """
        Tests get_securities_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456"],
                     Symbol=["ABC",
                             "DEF"],
                     Etf=[1,
                          0],
                     Delisted=[0,
                               1],
                     Currency=["USD",
                               "EUR"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            securities = get_securities_reindexed_like(
                closes,
                fields=["Symbol", "Etf", "Delisted", "Currency"])

            self.maxDiff = None

            securities = securities.reset_index()
            securities.loc[:, "Date"] = securities.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

            self.assertListEqual(
                securities.to_dict(orient="records"),
                [{'Field': 'Currency',
                  'Date': '2018-05-01T00:00:00-0400',
                  "FI12345": 'USD',
                  "FI23456": 'EUR'},
                 {'Field': 'Currency',
                  'Date': '2018-05-02T00:00:00-0400',
                  "FI12345": 'USD',
                  "FI23456": 'EUR'},
                 {'Field': 'Currency',
                  'Date': '2018-05-03T00:00:00-0400',
                  "FI12345": 'USD',
                  "FI23456": 'EUR'},
                 {'Field': 'Delisted',
                  'Date': '2018-05-01T00:00:00-0400',
                  "FI12345": False,
                  "FI23456": True},
                 {'Field': 'Delisted',
                  'Date': '2018-05-02T00:00:00-0400',
                  "FI12345": False,
                  "FI23456": True},
                 {'Field': 'Delisted',
                  'Date': '2018-05-03T00:00:00-0400',
                  "FI12345": False,
                  "FI23456": True},
                 {'Field': 'Etf',
                  'Date': '2018-05-01T00:00:00-0400',
                  "FI12345": True,
                  "FI23456": False},
                 {'Field': 'Etf',
                  'Date': '2018-05-02T00:00:00-0400',
                  "FI12345": True,
                  "FI23456": False},
                 {'Field': 'Etf',
                  'Date': '2018-05-03T00:00:00-0400',
                  "FI12345": True,
                  "FI23456": False},
                 {'Field': 'Symbol',
                  'Date': '2018-05-01T00:00:00-0400',
                  "FI12345": 'ABC',
                  "FI23456": 'DEF'},
                 {'Field': 'Symbol',
                  'Date': '2018-05-02T00:00:00-0400',
                  "FI12345": 'ABC',
                  "FI23456": 'DEF'},
                 {'Field': 'Symbol',
                  'Date': '2018-05-03T00:00:00-0400',
                  "FI12345": 'ABC',
                  "FI23456": 'DEF'}]
            )

    def test_securities_reindexed_like_intraday(self):
        """
        Tests get_securities_reindexed_like with Date and Time in the index.
        """
        closes = pd.DataFrame(
            np.random.rand(4,2),
            columns=["FI12345","FI23456"],
            index=pd.MultiIndex.from_product([
                pd.date_range(start="2018-05-01",
                              periods=2,
                              freq="D",
                              tz="America/New_York",
                              name="Date"),
                ["09:30:00", "09:31:00"],
                ], names=["Date", "Time"]))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456"],
                     Symbol=["ABC",
                             "DEF"]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            securities = get_securities_reindexed_like(
                closes,
                fields="Symbol")

            securities = securities.reset_index()
            securities.loc[:, "Date"] = securities.Date.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

            self.assertListEqual(
                securities.to_dict(orient="records"),
                [{"FI12345": 'ABC',
                  "FI23456": 'DEF',
                  'Date': '2018-05-01T00:00:00-0400',
                  'Field': 'Symbol',
                  'Time': '09:30:00'},
                 {"FI12345": 'ABC',
                  "FI23456": 'DEF',
                  'Date': '2018-05-01T00:00:00-0400',
                  'Field': 'Symbol',
                  'Time': '09:31:00'},
                 {"FI12345": 'ABC',
                  "FI23456": 'DEF',
                  'Date': '2018-05-02T00:00:00-0400',
                  'Field': 'Symbol',
                  'Time': '09:30:00'},
                 {"FI12345": 'ABC',
                  "FI23456": 'DEF',
                  'Date': '2018-05-02T00:00:00-0400',
                  'Field': 'Symbol',
                  'Time': '09:31:00'}]
            )
class ContractNumsReindexedLikeTestCase(unittest.TestCase):

    @patch("quantrocket.master.download_master_file")
    def test_pass_sids_based_on_reindex_like(self, mock_download_master_file):
        """
        Tests that sids are correctly passed to the download_master_file
        function based on reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01", periods=3, freq="D", name="Date"))

        def _mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456"],
                     ibkr_UnderConId=[1,
                                 1],
                     SecType=["FUT",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-07-04"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        mock_download_master_file.side_effect = _mock_download_master_file

        get_contract_nums_reindexed_like(closes)

        download_master_file_call = mock_download_master_file.mock_calls[0]
        _, args, kwargs = download_master_file_call
        self.assertListEqual(kwargs["sids"], ["FI12345","FI23456"])
        self.assertListEqual(kwargs["fields"], ["RolloverDate", "ibkr_UnderConId", "SecType"])

    def test_complain_if_no_fut(self):
        """
        Tests error handling when you pass a DataFrame with no FUTs.
        """
        closes = pd.DataFrame(
            np.random.rand(3,2),
            columns=["FI12345","FI23456"],
            index=pd.date_range(start="2018-05-01",
                                periods=3,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                    dict(Sid=["FI12345",
                                "FI23456"],
                         ibkr_UnderConId=[1,
                                     1],
                         SecType=["STK",
                                  "STK"],
                         RolloverDate=[
                             None,
                             None
                         ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            with self.assertRaises(ParameterError) as cm:
                get_contract_nums_reindexed_like(closes)

        self.assertIn("input DataFrame does not appear to contain any futures contracts",
                      repr(cm.exception))

    def test_contract_nums_reindexed_like(self):
        """
        Tests get_contract_nums_reindexed_like.
        """
        closes = pd.DataFrame(
            np.random.rand(4,6),
            columns=["FI12345","FI23456", "FI34567", "FI45678", "FI56789", "FI67890"],
            index=pd.date_range(start="2018-05-01",
                                periods=4,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456",
                            "FI34567",
                            "FI45678",
                            "FI56789",
                            "FI67890"],
                     ibkr_UnderConId=[1,
                                 2,
                                 1,
                                 2,
                                 None,
                                 2],
                     SecType=["FUT",
                              "FUT",
                              "FUT",
                              "FUT",
                              "STK",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-06-02",
                         "2018-06-03",
                         "2018-05-03",
                         None,
                         "2018-07-03"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            contract_nums = get_contract_nums_reindexed_like(closes)

        contract_nums.index = contract_nums.index.strftime("%Y-%m-%d")
        contract_nums = contract_nums.fillna("nan")
        self.assertDictEqual(
            contract_nums.to_dict(orient="index"),
            {'2018-05-01': {
                "FI12345": 1.0,
                "FI23456": 2.0,
                "FI34567": 2.0,
                "FI45678": 1.0,
                "FI56789": 'nan',
                "FI67890": 3.0},
             '2018-05-02': {
                 "FI12345": 1.0,
                 "FI23456": 2.0,
                 "FI34567": 2.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 3.0
                },
             '2018-05-03': {
                 "FI12345": 'nan',
                 "FI23456": 2.0,
                 "FI34567": 1.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 3.0},
             '2018-05-04': {
                 "FI12345": 'nan',
                 "FI23456": 1.0,
                 "FI34567": 1.0,
                 "FI45678": 'nan',
                 "FI56789": 'nan',
                 "FI67890": 2.0}}
        )

    def test_contract_nums_reindexed_like_tz_naive(self):
        """
        Tests get_contract_nums_reindexed_like with a tz-naive reindex_like.
        """
        closes = pd.DataFrame(
            np.random.rand(4,6),
            columns=["FI12345","FI23456", "FI34567", "FI45678", "FI56789", "FI67890"],
            index=pd.date_range(start="2018-05-01",
                                periods=4,
                                freq="D",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456",
                            "FI34567",
                            "FI45678",
                            "FI56789",
                            "FI67890"],
                     ibkr_UnderConId=[1,
                                 2,
                                 1,
                                 2,
                                 None,
                                 2],
                     SecType=["FUT",
                              "FUT",
                              "FUT",
                              "FUT",
                              "STK",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-06-02",
                         "2018-06-03",
                         "2018-05-03",
                         None,
                         "2018-07-03"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            contract_nums = get_contract_nums_reindexed_like(closes)

        contract_nums.index = contract_nums.index.strftime("%Y-%m-%d")
        contract_nums = contract_nums.fillna("nan")
        self.assertDictEqual(
            contract_nums.to_dict(orient="index"),
            {'2018-05-01': {
                "FI12345": 1.0,
                "FI23456": 2.0,
                "FI34567": 2.0,
                "FI45678": 1.0,
                "FI56789": 'nan',
                "FI67890": 3.0},
             '2018-05-02': {
                 "FI12345": 1.0,
                 "FI23456": 2.0,
                 "FI34567": 2.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 3.0
                },
             '2018-05-03': {
                 "FI12345": 'nan',
                 "FI23456": 2.0,
                 "FI34567": 1.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 3.0},
             '2018-05-04': {
                 "FI12345": 'nan',
                 "FI23456": 1.0,
                 "FI34567": 1.0,
                 "FI45678": 'nan',
                 "FI56789": 'nan',
                 "FI67890": 2.0}}
        )

    def test_limit_sequence_num(self):
        """
        Tests get_contract_nums_reindexed_like with the limit option.
        """
        closes = pd.DataFrame(
            np.random.rand(4,6),
            columns=["FI12345","FI23456", "FI34567", "FI45678", "FI56789", "FI67890"],
            index=pd.date_range(start="2018-05-01",
                                periods=4,
                                freq="D",
                                tz="America/New_York",
                                name="Date"))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456",
                            "FI34567",
                            "FI45678",
                            "FI56789",
                            "FI67890"],
                     ibkr_UnderConId=[1,
                                 2,
                                 1,
                                 2,
                                 None,
                                 2],
                     SecType=["FUT",
                              "FUT",
                              "FUT",
                              "FUT",
                              "STK",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-06-02",
                         "2018-06-03",
                         "2018-05-03",
                         None,
                         "2018-07-03"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            contract_nums = get_contract_nums_reindexed_like(closes, limit=2)

        contract_nums.index = contract_nums.index.strftime("%Y-%m-%d")
        contract_nums = contract_nums.fillna("nan")
        self.assertDictEqual(
            contract_nums.to_dict(orient="index"),
            {'2018-05-01': {
                "FI12345": 1.0,
                "FI23456": 2.0,
                "FI34567": 2.0,
                "FI45678": 1.0,
                "FI56789": 'nan',
                "FI67890": 'nan'},
             '2018-05-02': {
                 "FI12345": 1.0,
                 "FI23456": 2.0,
                 "FI34567": 2.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 'nan'
                },
             '2018-05-03': {
                 "FI12345": 'nan',
                 "FI23456": 2.0,
                 "FI34567": 1.0,
                 "FI45678": 1.0,
                 "FI56789": 'nan',
                 "FI67890": 'nan'},
             '2018-05-04': {
                 "FI12345": 'nan',
                 "FI23456": 1.0,
                 "FI34567": 1.0,
                 "FI45678": 'nan',
                 "FI56789": 'nan',
                 "FI67890": 2.0}}
        )

    def test_contract_nums_reindexed_like_intraday(self):
        """
        Tests get_contract_nums_reindexed_like when the input DataFrame includes
        Dates and Times.
        """
        closes = pd.DataFrame(
            np.random.rand(8,6),
            columns=["FI12345","FI23456", "FI34567", "FI45678", "FI56789", "FI67890"],
            index=pd.MultiIndex.from_product([
                pd.date_range(start="2018-05-01",
                              periods=4,
                              freq="D",
                              tz="America/New_York",
                              name="Date"),
                ["09:30:00","09:31:00"],
            ], names=["Date","Time"]))

        def mock_download_master_file(f, *args, **kwargs):
            securities = pd.DataFrame(
                dict(Sid=["FI12345",
                            "FI23456",
                            "FI34567",
                            "FI45678",
                            "FI56789",
                            "FI67890"],
                     ibkr_UnderConId=[1,
                                 2,
                                 1,
                                 2,
                                 None,
                                 2],
                     SecType=["FUT",
                              "FUT",
                              "FUT",
                              "FUT",
                              "STK",
                              "FUT"],
                     RolloverDate=[
                         "2018-05-02",
                         "2018-06-02",
                         "2018-06-03",
                         "2018-05-03",
                         None,
                         "2018-07-03"
                     ]))
            securities.to_csv(f, index=False)
            f.seek(0)

        with patch('quantrocket.master.download_master_file', new=mock_download_master_file):

            contract_nums = get_contract_nums_reindexed_like(closes)

        self.assertEqual(list(contract_nums.index.names), ["Date","Time"])

        contract_nums = contract_nums.reset_index().fillna("nan")
        contract_nums.loc[:, "Date"] = contract_nums.Date.dt.strftime("%Y-%m-%d")

        self.assertListEqual(
            contract_nums.to_dict(orient="records"),
            [{"FI12345": 1.0,
              "FI23456": 2.0,
              "FI34567": 2.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-01',
              'Time': '09:30:00'},
             {"FI12345": 1.0,
              "FI23456": 2.0,
              "FI34567": 2.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-01',
              'Time': '09:31:00'},
             {"FI12345": 1.0,
              "FI23456": 2.0,
              "FI34567": 2.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-02',
              'Time': '09:30:00'},
             {"FI12345": 1.0,
              "FI23456": 2.0,
              "FI34567": 2.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-02',
              'Time': '09:31:00'},
             {"FI12345": 'nan',
              "FI23456": 2.0,
              "FI34567": 1.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-03',
              'Time': '09:30:00'},
             {"FI12345": 'nan',
              "FI23456": 2.0,
              "FI34567": 1.0,
              "FI45678": 1.0,
              "FI56789": 'nan',
              "FI67890": 3.0,
              'Date': '2018-05-03',
              'Time': '09:31:00'},
             {"FI12345": 'nan',
              "FI23456": 1.0,
              "FI34567": 1.0,
              "FI45678": 'nan',
              "FI56789": 'nan',
              "FI67890": 2.0,
              'Date': '2018-05-04',
              'Time': '09:30:00'},
             {"FI12345": 'nan',
              "FI23456": 1.0,
              "FI34567": 1.0,
              "FI45678": 'nan',
              "FI56789": 'nan',
              "FI67890": 2.0,
              'Date': '2018-05-04',
              'Time': '09:31:00'}]
        )
