# Copyright 2017-2024 QuantRocket - All Rights Reserved
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

from quantrocket._cli.utils.parse import HelpFormatter
from quantrocket._cli.utils import completers
from quantrocket._cli.utils.completers.countries import (
    IBKR_SUBSIDIARIES,
    IBKR_STOCKLOAN_COUNTRIES
)

def add_subparser(subparsers):
    _parser = subparsers.add_parser("fundamental", description="QuantRocket fundamental data CLI", help="Collect and query fundamental data")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Collect fundamental data from Sharadar and save to database.

Notes
-----
Usage Guide:

* Sharadar Fundamentals: https://qrok.it/dl/qr/sharadar-fundamentals

Examples
--------

.. code-block:: bash

    quantrocket fundamental collect-sharadar-fundamentals
    """
    parser = _subparsers.add_parser(
        "collect-sharadar-fundamentals",
        help="collect fundamental data from Sharadar and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect fundamentals for. Possible choices: US, FREE")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_sharadar_fundamentals")

    examples = """
Collect insider holdings data from Sharadar and save to database.

Notes
-----
Usage Guide:

* Sharadar Insiders: https://qrok.it/dl/qr/sharadar-insiders

Examples
--------

.. code-block:: bash

    quantrocket fundamental collect-sharadar-insiders
    """
    parser = _subparsers.add_parser(
        "collect-sharadar-insiders",
        help="collect insider holdings data from Sharadar and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect insider holdings data for. Possible choices: US, FREE")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_sharadar_insiders")

    examples = """
Collect institutional investor data from Sharadar and save to database.

Notes
-----
Usage Guide:

* Sharadar Institutions: https://qrok.it/dl/qr/sharadar-institutions

Examples
--------

Collect institutional investor data aggregated by security:

.. code-block:: bash

    quantrocket fundamental collect-sharadar-institutions

Collect detailed institutional investor data (not aggregated by security):

.. code-block:: bash

    quantrocket fundamental collect-sharadar-institutions -d
    """
    parser = _subparsers.add_parser(
        "collect-sharadar-institutions",
        help="collect institutional investor data from Sharadar and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect institutional investor data for. Possible choices: US, FREE")
    parser.add_argument(
        "-d", "--detail",
        action="store_true",
        help="collect detailed investor data (separate record per "
        "investor per security per quarter). If omitted, "
        "collect data aggregated by security (separate record per "
        "security per quarter)")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_sharadar_institutions")

    examples = """
Collect SEC Form 8-K events from Sharadar and save to database.

Notes
-----
Usage Guide:

* Sharadar SEC Form 8-K: https://qrok.it/dl/qr/sharadar-sec8

Examples
--------

.. code-block:: bash

    quantrocket fundamental collect-sharadar-sec8
    """
    parser = _subparsers.add_parser(
        "collect-sharadar-sec8",
        help="collect SEC Form 8-K events from Sharadar and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect events data for. Possible choices: US, FREE")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_sharadar_sec8")

    examples = """
Collect historical S&P 500 index constituents from Sharadar and save to
database.

Notes
-----
Usage Guide:

* Sharadar S&P 500: https://qrok.it/dl/qr/sharadar-sp500

Examples
--------

.. code-block:: bash

    quantrocket fundamental collect-sharadar-sp500
    """
    parser = _subparsers.add_parser(
        "collect-sharadar-sp500",
        help="collect historical S&P 500 index constituents from Sharadar and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--country",
        metavar="COUNTRY",
        choices=["US","FREE"],
        default="US",
        help="country to collect S&P 500 constituents data for. Possible choices: US, FREE")
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_sharadar_sp500")

    examples = """
Collect Interactive Brokers shortable shares data and save to database.

Data is organized by country and updated every 15 minutes. Historical
data is available from April 15, 2018. Detailed intraday data as well as
aggregated daily data will be saved to the database.

Notes
-----
Usage Guide:

* IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short

Examples
--------

Collect shortable shares data for US stocks:

.. code-block:: bash

    quantrocket fundamental collect-ibkr-shortshares --countries usa

Collect shortable shares data for all stocks:

.. code-block:: bash

    quantrocket fundamental collect-ibkr-shortshares
    """
    parser = _subparsers.add_parser(
        "collect-ibkr-shortshares",
        help="collect Interactive Brokers shortable shares data and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY",
        help="limit to these countries (pass '?' or any invalid country to see "
        "available countries)").completer = completers.completer_from_dict(IBKR_STOCKLOAN_COUNTRIES)
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_ibkr_shortable_shares")

    examples = """
Collect Interactive Brokers borrow fees data and save to database.

Data is organized by country. Historical data is available from April
2018.

Notes
-----
Usage Guide:

* IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short

Examples
--------

Collect borrow fees for US stocks:

.. code-block:: bash

    quantrocket fundamental collect-ibkr-borrowfees --countries usa

Collect borrow fees for all stocks:

.. code-block:: bash

    quantrocket fundamental collect-ibkr-borrowfees
    """
    parser = _subparsers.add_parser(
        "collect-ibkr-borrowfees",
        help="collect Interactive Brokers borrow fees data and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY",
        help="limit to these countries (pass '?' or any invalid country to see "
        "available countries)").completer = completers.completer_from_dict(IBKR_STOCKLOAN_COUNTRIES)
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_ibkr_borrow_fees")

    examples = """
Collect Interactive Brokers margin requirements data and save to database.

The `country` parameter refers to the country of the IBKR subsidiary
where your account is located. (Margin requirements vary by IBKR
subsidiary.) Note that this differs from the IBKR shortable shares
or borrow fees APIs, where the `countries` parameter refers to the
country of the security rather than the country of the account.

Historical data is available from April 2018.

Notes
-----
Usage Guide:

* IBKR Margin Requirements: https://qrok.it/dl/qr/ibkr-margin

Examples
--------

Collect margin requirements for a US-based account:

.. code-block:: bash

    quantrocket fundamental collect-ibkr-margin --country usa
    """
    parser = _subparsers.add_parser(
        "collect-ibkr-margin",
        help="collect Interactive Brokers margin requirements data and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--country",
        required=True,
        metavar="COUNTRY",
        help="the country of the IBKR subsidiary where your account is located "
        "(pass '?' or any invalid country to see available countries)"
        ).completer = completers.completer_from_dict(IBKR_SUBSIDIARIES)
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_ibkr_margin_requirements")

    examples = """
Collect Alpaca easy-to-borrow data and save to database.

Data is updated daily. Historical data is available from March 2019.

Notes
-----
Usage Guide:

* Alpaca ETB: https://qrok.it/dl/qr/alpaca-etb

Examples
--------

Collect easy-to-borrow data:

.. code-block:: bash

    quantrocket fundamental collect-alpaca-etb
    """
    parser = _subparsers.add_parser(
        "collect-alpaca-etb",
        help="collect Alpaca easy-to-borrow data and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_alpaca_etb")

    examples = """
Collect Brain Sentiment Indicator (BSI) data and save to database.

This dataset provides news sentiment scores for US stocks, with history back to
August 2, 2016.

Notes
-----
Usage Guide:

* Brain Sentiment Indicator: https://qrok.it/dl/qr/brain-bsi

Examples
--------

Collect Brain Sentiment Indicator data:

.. code-block:: bash

    quantrocket fundamental collect-brain-bsi
    """
    parser = _subparsers.add_parser(
        "collect-brain-bsi",
        help="collect Brain Sentiment Indicator (BSI) data and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_brain_bsi")

    examples = """
Collect Brain Language Metrics on Company Filings (BLMCF) data and
save to database.

This dataset provides sentiment scores and other language metrics for
10-K and 10-Q company filings for US stocks, with history back to
January 1, 2010.

Notes
-----
Usage Guide:

* Brain Language Metrics on Company Filings: https://qrok.it/dl/qr/brain-blmcf

Examples
--------

Collect Brain Language Metrics on Company Filings data:

.. code-block:: bash

    quantrocket fundamental collect-brain-blmcf
    """
    parser = _subparsers.add_parser(
        "collect-brain-blmcf",
        help="collect Brain Language Metrics on Company Filings (BLMCF) data and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_brain_blmcf")

    examples = """
Collect Brain Language Metrics on Earnings Call Transcripts (BLMECT) data
and save to database.

This dataset provides sentiment scores and other language metrics for
earnings call transcripts for US stocks, with history back to January 1, 2012.


Notes
-----
Usage Guide:

* Brain Language Metrics on Earnings Call Transcripts: https://qrok.it/dl/qr/brain-blmect

Examples
--------

Collect Brain Language Metrics on Earnings Call Transcripts data:

.. code-block:: bash

    quantrocket fundamental collect-brain-blmect
    """
    parser = _subparsers.add_parser(
        "collect-brain-blmect",
        help="collect Brain Language Metrics on Earnings Call Transcripts (BLMECT) data and save to database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.fundamental._cli_collect_brain_blmect")

    examples = """
Query Sharadar Fundamentals from the local database and download to file.

Notes
-----
Usage Guide:

* Sharadar Fundamentals: https://qrok.it/dl/qr/sharadar-fundamentals

Examples
--------

Query as-reported trailing twelve month (ART) fundamentals for all indicators for
a particular sid:

.. code-block:: bash

    quantrocket fundamental sharadar-fundamentals -i FIBBG12345 --dimensions ART -o aapl_fundamentals.csv

Query as-reported quarterly (ARQ) fundamentals for select indicators for a universe:

.. code-block:: bash

    quantrocket fundamental sharadar-fundamentals -u usa-stk --dimensions ARQ -f REVENUE EPS -o sharadar_fundamentals.csv
    """
    dimensions_choices = {
        "ART": "as-reported trailing twelve months",
        "ARQ": "as-reported quarterly",
        "ARY": "as-reported annual",
        "MRT": "most recent reported trailing twelve months",
        "MRQ": "most recent reported quarterly",
        "MRY": "most recent reported annual",
    }
    parser = _subparsers.add_parser(
        "sharadar-fundamentals",
        help="query Sharadar Fundamentals from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to fundamentals on or after this fiscal period end date"
        ).completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to fundamentals on or before this fiscal period end date"
        ).completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    filters.add_argument(
        "-m", "--dimensions",
        nargs="*",
        choices=dimensions_choices.keys(),
        help=f"limit to these dimensions. Possible choices: {', '.join(dimensions_choices.keys())}. "
        "AR=As Reported, MR=Most Recent Reported, Q=Quarterly, Y=Annual, "
        "T=Trailing Twelve Month.").completer = completers.completer_from_dict(dimensions_choices)
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="sharadar_fundamentals")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields))").completer = completers.sharadar_fundamentals_fields_completer
    parser.set_defaults(func="quantrocket.fundamental._cli_download_sharadar_fundamentals")

    examples = """
Query Sharadar insider holdings data from the local database and download
to file.

Notes
-----
Usage Guide:

* Sharadar Insiders: https://qrok.it/dl/qr/sharadar-insiders

Examples
--------

Query insider holdings data for a particular sid:

.. code-block:: bash

    quantrocket fundamental sharadar-insiders -i FIBBG000B9XRY4 -o aapl_insiders.csv
    """
    parser = _subparsers.add_parser(
        "sharadar-insiders",
        help="query Sharadar insider holdings data from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this filing date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this filing date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="sharadar_insiders")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)"
        ).completer = completers.sharadar_insiders_fields_completer
    parser.set_defaults(func="quantrocket.fundamental._cli_download_sharadar_insiders")

    examples = """
Query Sharadar institutional investor data from the local database and
download to file.

Notes
-----
Usage Guide:

* Sharadar Institutions: https://qrok.it/dl/qr/sharadar-institutions

Examples
--------

Query institutional investor data aggregated by security:

.. code-block:: bash

    quantrocket fundamental sharadar-institutions -u usa-stk -s 2019-01-01 -o institutions.csv
    """
    parser = _subparsers.add_parser(
        "sharadar-institutions",
        help="query Sharadar institutional investor data from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this quarter end date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this quarter end date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="sharadar_institutions")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)"
        ).completer = completers.sharadar_institutions_fields_completer
    outputs.add_argument(
        "-d", "--detail",
        action="store_true",
        help="query detailed investor data (separate record per "
        "investor per security per quarter). If omitted, "
        "query data aggregated by security (separate record per "
        "security per quarter)"
    )
    parser.set_defaults(func="quantrocket.fundamental._cli_download_sharadar_institutions")

    examples = """
Query Sharadar SEC Form 8-K events data from the local database and download
to file.

Notes
-----
Usage Guide:

* Sharadar SEC Form 8-K: https://qrok.it/dl/qr/sharadar-sec8

Examples
--------

Query event code 13 (Bankruptcy) for a universe of securities:

.. code-block:: bash

    quantrocket fundamental sharadar-sec8 -u usa-stk --event-codes 13 -o bankruptcies.csv
    """
    parser = _subparsers.add_parser(
        "sharadar-sec8",
        help="query Sharadar SEC Form 8-K events data from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this filing date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this filing date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    filters.add_argument(
        "-c", "--event-codes",
        nargs="*",
        type=int,
        metavar="INT",
        help="limit to these event codes")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="sharadar_sec8")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_sharadar_sec8")

    examples = """
Query Sharadar S&P 500 index changes (additions and removals) from the
local database and download to file.

Notes
-----
Usage Guide:

* Sharadar S&P 500: https://qrok.it/dl/qr/sharadar-sp500

Examples
--------

Query S&P 500 index changes since 2010:

.. code-block:: bash

    quantrocket fundamental sharadar-sp500 -s 2010-01-01 -o sp500_changes.csv
    """
    parser = _subparsers.add_parser(
        "sharadar-sp500",
        help="query Sharadar S&P 500 index changes (additions and removals) from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to index changes on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to index changes on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="sharadar_sp500")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_sharadar_sp500")

    examples = """
Query intraday or daily Interactive Brokers shortable shares data from the
local database and download to file.

Intraday data timestamps are UTC.

Notes
-----
Usage Guide:

* IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short

Examples
--------

Query shortable shares for a universe of Australian stocks:

.. code-block:: bash

    quantrocket fundamental ibkr-shortshares -u asx-stk -o asx_shortables.csv

Query aggregated daily data instead:

.. code-block:: bash

    quantrocket fundamental ibkr-shortshares -u asx-stk -o asx_shortables.csv --aggregate
    """
    parser = _subparsers.add_parser(
        "ibkr-shortshares",
        help="query intraday or daily Interactive Brokers shortable shares data from the "
        "local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="ibkr_shortable_shares")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-a", "--aggregate",
        action="store_true",
        help="return aggregated daily data containing the min, max, mean, and last "
        "shortable share quantities per security per day. If omitted, "
        "return intraday data.")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_ibkr_shortable_shares")

    examples = """
Query Interactive Brokers borrow fees from the local database and download to file.

Notes
-----
Usage Guide:

* IBKR Short Sale Data: https://qrok.it/dl/qr/ibkr-short

Examples
--------

Query borrow fees for a universe of Australian stocks:

.. code-block:: bash

    quantrocket fundamental ibkr-borrowfees -u asx-stk -o asx_borrow_fees.csv
    """
    parser = _subparsers.add_parser(
        "ibkr-borrowfees",
        help="query Interactive Brokers borrow fees from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="ibkr_borrowfees")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_ibkr_borrow_fees")

    examples = """
Query Interactive Brokers margin requirements from the local database and
download to file.

Only stocks with special margin requirements are included in the dataset.
Default margin requirements apply to stocks that are omitted from the
dataset. 0 in the dataset is a placeholder value that also indicates that
default margin requirements apply.

Margin requirements are expressed in percentages, as whole numbers, for
example 50 means 50% margin requirement, which is equivalent to 0.5.

Data timestamps are UTC.

Notes
-----
Usage Guide:

* IBKR Margin Requirements: https://qrok.it/dl/qr/ibkr-margin

Examples
--------

Query margin requirements for a universe of US stocks:

.. code-block:: bash

    quantrocket fundamental ibkr-margin -u usa-stk -o usa_margin_requirements.csv
    """
    parser = _subparsers.add_parser(
        "ibkr-margin",
        help="query Interactive Brokers margin requirements from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="ibkr_margin_requirements")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_ibkr_margin_requirements")

    examples = """
Query Alpaca easy-to-borrow data from the local database and download to file.

Notes
-----
Usage Guide:

* Alpaca ETB: https://qrok.it/dl/qr/alpaca-etb

Examples
--------

Query easy-to-borrow data for a universe of US stocks:

.. code-block:: bash

    quantrocket fundamental alpaca-etb -u usa-stk -o usa_etb.csv
    """
    parser = _subparsers.add_parser(
        "alpaca-etb",
        help="query Alpaca easy-to-borrow data from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to data on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="alpaca_etb")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_alpaca_etb")

    examples = """
Query Brain Sentiment Indicator (BSI) data from the local database and download
to file.

Notes
-----
Usage Guide:

* Brain Sentiment Indicator: https://qrok.it/dl/qr/brain-bsi

Examples
--------
Download news sentiment scores averaged over the last 7 days for
a universe of tech stocks:

.. code-block:: bash

    quantrocket fundamental brain-bsi -N 7 -u usa-tech -s 2024-01-01 -o bsi7.csv
    """
    parser = _subparsers.add_parser(
        "brain-bsi",
        help="query Brain Sentiment Indicator (BSI) data from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-N",
        type=int,
        metavar="INT",
        choices=[1, 7, 30],
        help="limit to records with this calculation window. Possible choices: 1, 7, 30")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to records on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to records on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="brain_bsi")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields))")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_brain_bsi")

    examples = """
Query Brain Language Metrics on Company Filings (BLMCF) data from the local
database and download to file.

Notes
-----
Usage Guide:

* Brain Language Metrics on Company Filings: https://qrok.it/dl/qr/brain-blmcf

Examples
--------
Download language metrics on company filings for all available stocks for a single
year:

.. code-block:: bash

    quantrocket fundamental brain-blmcf -s 2023-01-01 -e 2024-01-01 -o blmcf.csv
    """
    parser = _subparsers.add_parser(
        "brain-blmcf",
        help="query Brain Language Metrics on Company Filings (BLMCF) data from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-c", "--report-category",
        metavar="CATEGORY",
        choices=["10-K", "10-Q"],
        help="limit to this report category. Possible choices: 10-K, 10-Q. If omitted, "
        "both report categories are returned.")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to records on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to records on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="brain_blmcf")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields). Metrics are calculated separately for the Risk Factors "
        "section of the report (fields starting with RF), the Management Discussion "
        "and Analysis section (fields starting with MD), and the report as a whole "
        '(fields not starting with RF or MD). Fields containing "DELTA" or "SIMILARITY" '
        "in the name compare the current report with the previous report of the same "
        "period and category.")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_brain_blmcf")

    examples = """
Query Brain Language Metrics on Earnings Call Transcripts (BLMECT) data from the
local database and download to file.

Notes
-----
Usage Guide:

* Brain Language Metrics on Earnings Call Transcripts: https://qrok.it/dl/qr/brain-blmect

Examples
--------
Download language metrics on earnings call transcripts for all available stocks
for a single year:

.. code-block:: bash

    quantrocket fundamental brain-blmect -s 2023-01-01 -e 2024-01-01 -o blmect.csv
    """
    parser = _subparsers.add_parser(
        "brain-blmect",
        help="query Brain Language Metrics on Earnings Call Transcripts (BLMECT) data from the local database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to records on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to records on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="brain_blmect")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields). Fields are organized into three sections, "
        "corresponding to three sections of the earnings call transcripts: "
        '"Management Discussion" (MD), "Analyst Questions" (AQ), and '
        '"Management Answers" (MA). Fields containing "DELTA" or "SIMILARITY" '
        "in the name compare the current earnings call transcript to the "
        "previous earnings call transcript.")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_brain_blmect")

    examples = """
Query financial statements from the Reuters financials database and
download to file.

DEPRECATED. This data is no longer available from Interactive Brokers. Only
data that was previously saved to the local database can be queried.

Examples
--------

Query total revenue (COA code RTLR) for a universe of Australian stocks:

.. code-block:: bash

    quantrocket fundamental reuters-financials RTLR -u asx-stk -s 2014-01-01 -e 2017-01-01 -o rtlr.csv

Query net income (COA code NINC) from interim reports for two securities
(identified by sid) and exclude restatements:

.. code-block:: bash

    quantrocket fundamental reuters-financials NINC -i FIBBG123456 FIBBG234567 --interim --exclude-restatements -o ninc.csv

Query common and preferred shares outstanding (COA codes QTCO and QTPO) and return a
minimal set of fields (several required fields will always be returned)

.. code-block:: bash

    quantrocket fundamental reuters-financials QTCO QTPO -u nyse-stk --fields Amount -o nyse_float.csv
    """
    parser = _subparsers.add_parser(
        "reuters-financials",
        help="[DEPRECATED] query financial statements from the Reuters financials database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="+",
        help="the Chart of Account (COA) code(s) to query")
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to statements on or after this fiscal period end date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to statements on or before this fiscal period end date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    filters.add_argument(
        "-q", "--interim",
        action="store_true",
        help="return interim reports (default is to return annual reports, "
        "which provide deeper history)")
    filters.add_argument(
        "-r", "--exclude-restatements",
        action="store_true",
        help="exclude restatements (default is to include them)")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="reuters_financials")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_reuters_financials")

    examples = """
Query estimates and actuals from the Reuters estimates database and
download to file.

DEPRECATED. This data is no longer available from Interactive Brokers. Only
data that was previously saved to the local database can be queried.

Examples
--------

Query EPS estimates and actuals for a universe of Australian stocks:

.. code-block:: bash

    quantrocket fundamental reuters-estimates EPS -u asx-stk -s 2014-01-01 -e 2017-01-01 -o eps_estimates.csv
    """
    parser = _subparsers.add_parser(
        "reuters-estimates",
        help="[DEPRECATED] query estimates and actuals from the Reuters estimates database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "codes",
        metavar="CODE",
        nargs="+",
        help="the indicator code(s) to query")
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to estimates and actuals on or after this fiscal period end date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to estimates and actuals on or before this fiscal period end date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    filters.add_argument(
        "-t", "--period-types",
        nargs="*",
        choices=["A", "Q", "S"],
        metavar="PERIOD_TYPE",
        help="limit to these fiscal period types. Possible choices: A, Q, S, where "
        "A=Annual, Q=Quarterly, S=Semi-Annual")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="reuters_estimates")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_reuters_estimates")

    examples = """
Query earnings announcement dates from the Wall Street Horizon
announcements database and download to file.

DEPRECATED. This data is no longer available from Interactive Brokers. Only
data that was previously saved to the local database can be queried.

Examples
--------

Query earnings dates for a universe of US stocks:

.. code-block:: bash

    quantrocket fundamental wsh -u usa-stk -s 2019-01-01 -e 2019-04-01 -o announcements.csv
    """
    parser = _subparsers.add_parser(
        "wsh",
        help="[DEPRECATED] query earnings announcement dates from the Wall Street Horizon announcements database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-s", "--start-date",
        metavar="YYYY-MM-DD",
        help="limit to announcements on or after this date").completer = completers.start_date_completer
    filters.add_argument(
        "-e", "--end-date",
        metavar="YYYY-MM-DD",
        help="limit to announcements on or before this date").completer = completers.end_date_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    filters.add_argument(
        "--exclude-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="exclude these universes").completer = completers.universe_completer
    filters.add_argument(
        "--exclude-sids",
        nargs="*",
        metavar="SID",
        help="exclude these sids").completer = completers.sid_completer
    filters.add_argument(
        "-t", "--statuses",
        nargs="*",
        choices=["Confirmed", "Unconfirmed"],
        metavar="STATUS",
        help="limit to these confirmation statuses. Possible choices: Confirmed, Unconfirmed")
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="wsh_earnings_dates")
    output_format_group = outputs.add_mutually_exclusive_group()
    output_format_group.add_argument(
        "-j", "--json",
        action="store_const",
        const="json",
        dest="output",
        help="format output as JSON (default is CSV)")
    outputs.add_argument(
        "-f", "--fields",
        metavar="FIELD",
        nargs="*",
        help="only return these fields (pass '?' or any invalid fieldname to see "
        "available fields)")
    parser.set_defaults(func="quantrocket.fundamental._cli_download_wsh_earnings_dates")
