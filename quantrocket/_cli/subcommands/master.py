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

def add_subparser(subparsers):
    _parser = subparsers.add_parser("master", description="QuantRocket securities master CLI", help="Manage and query the securities master database")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    examples = """
Collect securities listings from Alpaca and store in securities master
database.

Notes
-----
Usage Guide:

* Alpaca listings: https://qrok.it/dl/qr/master-alpaca

Examples
--------

.. code-block:: bash

    quantrocket master collect-alpaca
    """
    parser = _subparsers.add_parser(
        "collect-alpaca",
        help="collect securities listings from Alpaca and store in securities master database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.master._cli_collect_alpaca_listings")

    examples = """
Collect securities listings from EDI and store in securities master
database.

Notes
-----
Usage Guide:

* EDI listings: https://qrok.it/dl/qr/master-edi

Examples
--------

Collect sample listings:

.. code-block:: bash

    quantrocket master collect-edi --exchanges FREE

Collect listings for all permitted exchanges

.. code-block:: bash

    quantrocket master collect-edi

Collect all Chinese stock listings:

.. code-block:: bash

    quantrocket master collect-edi -e XSHG XSHE
    """
    parser = _subparsers.add_parser(
        "collect-edi",
        help="collect securities listings from EDI and store in securities master database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-e", "--exchanges",
        nargs="*",
        metavar="MIC",
        help="collect listings for these exchanges (identified by MICs)"
        ).completer = completers.example_completer(["XSHG", "XSHE"])
    parser.set_defaults(func="quantrocket.master._cli_collect_edi_listings")

    examples = """
Collect securities listings from Bloomberg OpenFIGI and store
in securities master database.

OpenFIGI provides several useful security attributes including
market sector, a detailed security type, and share class-level
FIGI identifier.

The collected data fields show up in the master file under the
prefix "figi_*".

This command does not directly query the OpenFIGI API but rather
downloads a dump of all FIGIs which QuantRocket has previously
mapped to securities from other vendors.

Notes
-----
Usage Guide:

* FIGI listings: https://qrok.it/dl/qr/master-figi

Examples
--------

.. code-block:: bash

    quantrocket master collect-figi
    """
    parser = _subparsers.add_parser(
        "collect-figi",
        help="collect securities listings from Bloomberg OpenFIGI and store in securities master database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.master._cli_collect_figi_listings")

    examples = """
Collect securities listings from Interactive Brokers and store in
securities master database.

Specify a country or exchange (optionally filtering by security type,
currency, and/or symbol) to collect listings from the IBKR website and
collect associated contract details from the IBKR API. Or, specify
universes or sids to collect details from the IBKR API, bypassing the
website.

Notes
-----
Usage Guide:

* Interactive Brokers listings: https://qrok.it/dl/qr/master-ibkr

Examples
--------

Collect free sample listings:

.. code-block:: bash

    quantrocket master collect-ibkr --exchanges FREE

Collect all US stock and ETF listings:

.. code-block:: bash

    quantrocket master collect-ibkr --countries US --sec-types STK ETF

Collect specific symbols:

.. code-block:: bash

    quantrocket master collect-ibkr -c US --symbols AAPL GOOG NFLX

Re-collect contract details for an existing universe called "japan-fin":

.. code-block:: bash

    quantrocket master collect-ibkr --universes japan-fin
    """
    sectype_choices = [
        "STK",
        "ETF",
        "FUT",
        "CASH",
        "IND",
        "CFD"
    ]
    parser = _subparsers.add_parser(
        "collect-ibkr",
        help="collect securities listings from Interactive Brokers and store "
        "in securities master database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY_CODE",
        help="one or more two-letter country codes to collect listings for. "
        "For sample data use country code 'FREE'"
        ).completer = completers.ibkr_country_completer
    parser.add_argument(
        "-e", "--exchanges",
        nargs="*",
        metavar="EXCHANGE",
        help="one or more exchange codes to collect listings for"
        ).completer = completers.ibkr_exchange_completer
    parser.add_argument(
        "-t", "--sec-types",
        nargs="*",
        metavar="SEC_TYPE",
        choices=sectype_choices,
        help=f"limit to these security types. Possible choices: {', '.join(sectype_choices)}"
        ).completer = completers.sec_type_completer(sectype_choices)
    parser.add_argument(
        "--currencies",
        nargs="*",
        metavar="CURRENCY",
        help="limit to these currencies"
        ).completer = completers.currency_completer
    parser.add_argument(
        "-s", "--symbols",
        nargs="*",
        metavar="SYMBOL",
        help="limit to these symbols").completer = completers.symbol_completer
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    parser.set_defaults(func="quantrocket.master._cli_collect_ibkr_listings")

    examples = """
Collect securities listings from Sharadar and store in securities master
database.

Notes
-----
Usage Guide:

* Sharadar listings: https://qrok.it/dl/qr/master-sharadar

Examples
--------

Collect sample listings:

.. code-block:: bash

    quantrocket master collect-sharadar --countries FREE

Collect all US listings:

.. code-block:: bash

    quantrocket master collect-sharadar --countries US
    """
    parser = _subparsers.add_parser(
        "collect-sharadar",
        help="collect securities listings from Sharadar and store in securities master database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-c", "--countries",
        nargs="*",
        metavar="COUNTRY",
        default=["US"],
        choices=["US", "FREE"],
        help="collect listings for these countries. Possible choices: US, FREE")
    parser.set_defaults(func="quantrocket.master._cli_collect_sharadar_listings")

    examples = """
Collect US stock listings from QuantRocket and store in securities master
database.

Notes
-----
Usage Guide:

* US Stock listings: https://qrok.it/dl/qr/master-usstock

Examples
--------

.. code-block:: bash

    quantrocket master collect-usstock
    """
    parser = _subparsers.add_parser(
        "collect-usstock",
        help="collect US stock listings from QuantRocket and store in securities master database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.master._cli_collect_usstock_listings")

    examples = """
Collect IBKR option chains for underlying securities.

Note: option chains often consist of hundreds, sometimes thousands of options
per underlying security. Be aware that requesting option chains for large
universes of underlying securities, such as all stocks on the NYSE, can take
numerous hours to complete.

Notes
-----
Usage Guide:

* IBKR Option Chains: https://qrok.it/dl/qr/option-chains

Examples
--------

Collect option chains for several underlying securities:

.. code-block:: bash

    quantrocket master collect-ibkr-options --sids FIBBG000LV0836 FIBBG000B9XRY4

Collect option chains for NQ futures:

.. code-block:: bash

    quantrocket master get -e CME -s NQ -t FUT | quantrocket master collect-ibkr-options -f -

Collect option chains for a large universe of stocks called "nyse-stk" (see note above):

.. code-block:: bash

    quantrocket master collect-ibkr-options -u "nyse-stk"
    """
    parser = _subparsers.add_parser(
        "collect-ibkr-options",
        help="collect IBKR option chains for underlying securities",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="collect options for these universes of underlying securities").completer = completers.universe_completer
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="collect options for these underlying sids").completer = completers.sid_completer
    parser.add_argument(
        "-f", "--infile",
        metavar="INFILE",
        dest="infilepath_or_buffer",
        help="collect options for the sids in this CSV file (specify '-' to read "
        "file from stdin)").completer = completers.infile_completer(["csv"], allow_stdin=True)
    parser.set_defaults(func="quantrocket.master._cli_collect_ibkr_option_chains")

    examples = """
Query security details from the securities master database and download to
file.

Notes
-----

Parameters for filtering query results are combined according to the following
rules. First, the master service determines what to include in the result set,
based on the inclusion filters: `--exchanges`, `--sec-types`, `--currencies`,
`--universes`, `--symbols`, and `--sids`. With the exception of `--sids`, these
parameters are ANDed together. That is, securities must satisfy all of the
parameters to be included. If `--vendors` is provided, only those vendors are
searched for the purpose of determining matches.

The `--sids` parameter is treated differently. Securities matching `--sids` are
always included, regardless of whether they meet the other inclusion criteria.

After determining what to include, the master service then applies the exclusion
filters (`--exclude-sids`, `--exclude-universes`, `--exclude-delisted`,
`--exclude-expired`, and `--frontmonth`) to determine what (if anything) should be
removed from the result set. Exclusion filters are ORed, that is, securities are
excluded if they match any of the exclusion criteria.

Usage Guide:

* Master file: https://qrok.it/dl/qr/master-file

Examples
--------

Download NYSE and NASDAQ securities to file, using MICs to specify
the exchanges:

.. code-block:: bash

    quantrocket master get --exchanges XNYS XNAS -o securities.csv

Download NYSE and NASDAQ securities to file, using IBKR exchange codes
to specify the exchanges, and include all IBKR fields:

.. code-block:: bash

    quantrocket master get --exchanges NYSE NASDAQ -f 'ibkr*' -o securities.csv

Download a CSV of all ARCA ETFs and use it to create a universe called
"arca-etf":

.. code-block:: bash

    quantrocket master get --exchanges ARCA --sec-types ETF | quantrocket master universe "arca-etf" --infile -

Query the exchange and currency for all listings of AAPL and format for
terminal display:

.. code-block:: bash

    quantrocket master get --symbols AAPL --fields Exchange Currency | csvlook -I
    """
    sectype_choices = [
        "STK",
        "ETF",
        "FUT",
        "CASH",
        "IND",
        "OPT",
        "FOP",
        "BAG",
        "CFD"
    ]
    vendors = {
        "usstock": "US Stock",
        "sharadar": "Sharadar",
        "ibkr": "Interactive Brokers",
        "edi": "EDI",
        "alpaca": "Alpaca",
    }
    parser = _subparsers.add_parser(
        "get",
        help="query security details from the securities master database and download to file",
        epilog=examples,
        formatter_class=HelpFormatter)
    filters = parser.add_argument_group("filtering options")
    filters.add_argument(
        "-e", "--exchanges",
        nargs="*",
        metavar="EXCHANGE",
        help="limit to these exchanges. You can specify exchanges using the MIC or the "
        "vendor's exchange code.").completer = completers.exchange_calendar_completer
    filters.add_argument(
        "-t", "--sec-types",
        nargs="*",
        metavar="SEC_TYPE",
        choices=sectype_choices,
        help=f"limit to these security types. Possible choices: {', '.join(sectype_choices)}"
        ).completer = completers.sec_type_completer(sectype_choices)
    filters.add_argument(
        "-c", "--currencies",
        nargs="*",
        metavar="CURRENCY",
        help="limit to these currencies").completer = completers.currency_completer
    filters.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    filters.add_argument(
        "-s", "--symbols",
        nargs="*",
        metavar="SYMBOL",
        help="limit to these symbols").completer = completers.symbol_completer
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
        "--exclude-delisted",
        action="store_true",
        default=False,
        help="exclude delisted securities (default is to include them)")
    filters.add_argument(
        "--exclude-expired",
        action="store_true",
        default=False,
        help="exclude expired contracts (default is to include them)")
    filters.add_argument(
        "-m", "--frontmonth",
        action="store_true",
        default=False,
        help="exclude backmonth and expired futures contracts")
    filters.add_argument(
        "-v", "--vendors",
        nargs="*",
        metavar="VENDOR",
        choices=vendors.keys(),
        help=f"limit to these vendors. Possible choices: {', '.join(vendors.keys())}"
        ).completer = lambda *args, **kwargs: vendors
    outputs = parser.add_argument_group("output options")
    outputs.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="filepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv", "json"], outfile_prefix="securities")
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
        help='return specific fields. By default a core set of fields is '
        'returned, but additional vendor-specific fields are also available. '
        'To return non-core fields, you can reference them by name, or pass "*" '
        'to return all available fields. To return all fields for a specific '
        'vendor, pass the vendor prefix followed by "*", for example "edi*" '
        'for all EDI fields. Pass "?*" (or any invalid vendor prefix plus "*") '
        'to see available vendor prefixes. Pass "?" or any invalid fieldname '
        'to see all available fields.').completer = completers.master_fields_completer
    parser.set_defaults(func="quantrocket.master._cli_download_master_file")

    examples = """
List exchanges by security type and country as found on the IBKR website.

Notes
-----
Usage Guide:

* Interactive Brokers listings: https://qrok.it/dl/qr/master-ibkr

Examples
--------

List all exchanges:

.. code-block:: bash

    quantrocket master list-ibkr-exchanges

List stock exchanges in the Americas:

.. code-block:: bash

    quantrocket master list-ibkr-exchanges --regions americas --sec-types STK
    """
    sectype_choices = [
        "STK", "FUT", "CASH", "IND", "CFD"
    ]
    parser = _subparsers.add_parser(
        "list-ibkr-exchanges",
        help="list exchanges by security type and country as found on the IBKR website",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-r", "--regions",
        nargs="*",
        choices=["americas", "europe", "asia", "global"],
        metavar="REGION",
        help="limit to these regions. Possible choices: americas, europe, asia, global")
    parser.add_argument(
        "-t", "--sec-types",
        nargs="*",
        choices=sectype_choices,
        metavar="SEC_TYPE",
        help=f"limit to these security types. Possible choices: {', '.join(sectype_choices)}"
        ).completer = completers.sec_type_completer(sectype_choices)
    parser.set_defaults(func="quantrocket.master._cli_list_ibkr_exchanges")

    examples = """
Flag security details that have changed in IBKR's system since the time they
were last collected into the securities master database.

Diff can be run synchronously or asynchronously (asynchronous is the default
and is recommended if diffing more than a handful of securities).

Notes
-----
Usage Guide:

* Maintain listings: https://qrok.it/dl/qr/maintain-listings

Examples
--------

Asynchronously generate a diff for all securities in a universe called
"italy-stk" and log the results, if any, to flightlog:

.. code-block:: bash

    quantrocket master diff-ibkr -u "italy-stk"

Asynchronously generate a diff for all securities in a universe called
"italy-stk", looking only for sector or industry changes:

.. code-block:: bash

    quantrocket master diff-ibkr -u "italy-stk" --fields ibkr_Sector ibkr_Industry

Synchronously get a diff for specific securities by sid:

.. code-block:: bash

    quantrocket master diff-ibkr --sids FIBBG000LV0836 FIBBG000B9XRY4 --wait

Synchronously get a diff for specific securities without knowing their sids:

.. code-block:: bash

    quantrocket master get -e NASDAQ -t STK -s AAPL FB GOOG | quantrocket master diff-ibkr --wait --infile -

Asynchronously generate a diff for all securities in a universe called
"nasdaq-sml" and auto-delist any symbols that are no longer available from IBKR
or that are now associated with the PINK exchange:

.. code-block:: bash

    quantrocket master diff-ibkr -u "nasdaq-sml" --delist-missing --delist-exchanges PINK
    """
    parser = _subparsers.add_parser(
        "diff-ibkr",
        help="flag security details that have changed in IBKR's system since the time "
        "they were last collected into the securities master database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-u", "--universes",
        nargs="*",
        metavar="UNIVERSE",
        help="limit to these universes").completer = completers.universe_completer
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="limit to these sids").completer = completers.sid_completer
    parser.add_argument(
        "-n", "--infile",
        metavar="INFILE",
        dest="infilepath_or_buffer",
        help="limit to the sids in this CSV file (specify '-' to read file from stdin)").completer = completers.infile_completer(["csv"], allow_stdin=True)
    parser.add_argument(
        "-f", "--fields",
        nargs="*",
        metavar="FIELD",
        help="only diff these fields (field name should start with 'ibkr')"
        ).completer = completers.diff_ibkr_fields_completer
    parser.add_argument(
        "--delist-missing",
        action="store_true",
        default=False,
        help="auto-delist securities that are no longer available from IBKR")
    parser.add_argument(
        "--delist-exchanges",
        metavar="EXCHANGE",
        nargs="*",
        help="auto-delist securities that are associated with these exchanges")
    parser.add_argument(
        "-w", "--wait",
        action="store_true",
        default=False,
        help="run the diff synchronously and return the diff (otherwise run "
        "asynchronously and log the results, if any, to flightlog")
    parser.set_defaults(func="quantrocket.master._cli_diff_ibkr_securities")

    examples = """
Mark an IBKR security as delisted.

This does not remove any data but simply marks the security as delisted so
that data services won't attempt to collect data for the security and so
that the security can be optionally excluded from query results.

The security can be specified by sid or a combination of other
parameters (for example, symbol + exchange). As a precaution, the request
will fail if the parameters match more than one security.

Notes
-----
Usage Guide:

* Maintain listings: https://qrok.it/dl/qr/maintain-listings

Examples
--------

Delist a security by sid:

.. code-block:: bash

    quantrocket master delist-ibkr -i FIBBG1234567890

Delist a security by symbol + exchange:

.. code-block:: bash

    quantrocket master delist-ibkr -s ABC -e NYSE
    """
    sectype_choices = [
        "STK", "ETF", "FUT", "CASH", "IND", "CFD"
    ]
    parser = _subparsers.add_parser(
        "delist-ibkr",
        help="mark an IBKR security as delisted",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-i", "--sid",
        help="the sid of the security to be delisted").completer = completers.sid_completer
    parser.add_argument(
        "-s", "--symbol",
        help="the symbol to be delisted (if sid not provided)").completer = completers.symbol_completer
    parser.add_argument(
        "-e", "--exchange",
        help="the exchange of the security to be delisted (if needed to disambiguate)"
        ).completer = completers.ibkr_exchange_completer
    parser.add_argument(
        "-c", "--currency",
        help="the currency of the security to be delisted (if needed to disambiguate)"
        ).completer = completers.currency_completer
    parser.add_argument(
        "-t", "--sec-type",
        metavar="SEC_TYPE",
        choices=sectype_choices,
        help="the security type of the security to be delisted (if needed to disambiguate). "
        f"Possible choices: {', '.join(sectype_choices)}"
    ).completer = completers.sec_type_completer(sectype_choices)
    parser.set_defaults(func="quantrocket.master._cli_delist_ibkr_security")

    examples = """
List universes and their size.

Notes
-----
Usage Guide:

* Universes: https://qrok.it/dl/qr/universes

Examples
--------

.. code-block:: bash

    quantrocket master list-universes
    """
    parser = _subparsers.add_parser(
        "list-universes",
        help="list universes and their size",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.set_defaults(func="quantrocket.master._cli_list_universes")

    examples = """
Create a universe of securities.

Notes
-----
Usage Guide:

* Universes: https://qrok.it/dl/qr/universes

Examples
--------

Download a CSV of Italian stocks then upload it to create a universe called
"italy-stk":

.. code-block:: bash

    quantrocket master get --exchanges BVME --sec-types STK -f italy.csv
.. code-block:: bash

    quantrocket master universe "italy-stk" -f italy.csv

In one line, download a CSV of all ARCA ETFs and append to a universe called
"arca-etf":

.. code-block:: bash

    quantrocket master get --exchanges ARCA --sec-types ETF | quantrocket master universe "arca-etf" --append --infile -

Create a universe consisting of several existing universes:

.. code-block:: bash

    quantrocket master universe "asx" --from-universes "asx-sml" "asx-mid" "asx-lrg"

Copy a universe but exclude delisted securities:

.. code-block:: bash

    quantrocket master universe "hong-kong-active" --from-universes "hong-kong" --exclude-delisted
    """
    parser = _subparsers.add_parser(
        "universe",
        help="create a universe of securities",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        metavar="CODE",
        help="the code to assign to the universe (lowercase alphanumerics and hyphens only)"
        ).completer = completers.example_completer(["my-universe"])
    parser.add_argument(
        "-f", "--infile",
        metavar="INFILE",
        dest="infilepath_or_buffer",
        help="create the universe from the sids in this CSV file (specify '-' to read file "
        "from stdin)").completer = completers.infile_completer(["csv"], allow_stdin=True)
    parser.add_argument(
        "-i", "--sids",
        nargs="*",
        metavar="SID",
        help="create the universe from these sids").completer = completers.sid_completer
    parser.add_argument(
        "--from-universes",
        nargs="*",
        metavar="UNIVERSE",
        help="create the universe from these existing universes"
        ).completer = completers.universe_completer
    parser.add_argument(
        "--exclude-delisted",
        action="store_true",
        help="exclude delisted securities and expired contracts that would otherwise be included (default is to "
        "include them)")
    on_conflict_group = parser.add_mutually_exclusive_group()
    on_conflict_group.add_argument(
        "-a", "--append",
        action="store_true",
        help="append to universe if universe already exists")
    on_conflict_group.add_argument(
        "-r", "--replace",
        action="store_true",
        help="replace universe if universe already exists")
    parser.set_defaults(func="quantrocket.master._cli_create_universe")

    examples = """
Delete a universe.

The listings details of the member securities won't be deleted, only their
grouping as a universe.

Notes
-----
Usage Guide:

* Universes: https://qrok.it/dl/qr/universes

Examples
--------

Delete the universe called "italy-stk":

.. code-block:: bash

    quantrocket master delete-universe 'italy-stk'
    """
    parser = _subparsers.add_parser(
        "delete-universe",
        help="delete a universe",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "code",
        help="the universe code").completer = completers.universe_completer
    parser.set_defaults(func="quantrocket.master._cli_delete_universe")

    examples = """
Create an IBKR combo (aka spread), which is a composite instrument consisting
of two or more individual instruments (legs) that are traded as a single
instrument.

Each user-defined combo is stored in the securities master database with a
SecType of "BAG". The combo legs are stored in the ComboLegs field as a JSON
array. QuantRocket assigns a sid for the combo consisting of a prefix 'IC'
followed by an autoincrementing digit, for example: IC1, IC2, IC3, ...

If the combo already exists, its sid will be returned instead of creating a
duplicate record.

Notes
-----
Usage Guide:

* Combos: https://qrok.it/dl/qr/combos

Examples
--------

Create a spread from a JSON file:

.. code-block:: bash

    cat spread.json
    [["BUY", 1, QF12345],
     ["SELL", 1, QF23456]]

    quantrocket master create-ibkr-combo spread.json
    """
    parser = _subparsers.add_parser(
        "create-ibkr-combo",
        help="Create an IBKR combo (aka spread)",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "combo_filepath",
        metavar="PATH",
        help="a JSON file containing an array of the combo legs, where each "
        "leg is an array specifying action, ratio, and sid"
        ).completer = completers.infile_completer(["json"])
    parser.set_defaults(func="quantrocket.master._cli_create_ibkr_combo")

    examples = """
Upload a new rollover rules config, or return the current rollover rules.

Examples
--------

Upload a new rollover config (replaces current config):

.. code-block:: bash

    quantrocket master rollrules myrolloverrules.yml

Show current rollover config:

.. code-block:: bash

    quantrocket master rollrules
    """
    parser = _subparsers.add_parser(
        "rollrules",
        help="upload a new rollover rules config, or return the current rollover rules",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "filename",
        nargs="?",
        metavar="FILENAME",
        help="the rollover rules YAML config file to upload (if omitted, return the current config)"
        ).completer = completers.infile_completer(["yml"])
    parser.set_defaults(func="quantrocket.master._cli_load_or_show_rollrules")

    examples = """
Collect upcoming trading hours from IBKR for exchanges and save to securities
master database.

Notes
-----
Usage Guide:

* Trading Calendars: https://qrok.it/dl/qr/calendars

Examples
--------

Collect trading hours for ARCA:

.. code-block:: bash

    quantrocket master collect-ibkr-calendar -e ARCA
    """
    parser = _subparsers.add_parser(
        "collect-ibkr-calendar",
        help="collect upcoming trading hours from IBKR for exchanges and save to securities master database",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-e", "--exchanges",
        nargs="*",
        metavar="EXCHANGE",
        help="limit to these exchanges").completer = completers.ibkr_exchange_completer
    parser.set_defaults(func="quantrocket.master._cli_collect_ibkr_calendar")

    examples = """
Check whether exchanges are open or closed.

Notes
-----
Usage Guide:

* Trading Calendars: https://qrok.it/dl/qr/calendars

Examples
--------

Check whether NYSE is open or closed now:

.. code-block:: bash

    quantrocket master calendar NYSE

Check whether the Tokyo Stock Exchange was open or closed 5 hours ago:

.. code-block:: bash

    quantrocket master calendar TSEJ --ago 5h

Check whether CME will be open or closed in 30 minutes:

.. code-block:: bash

    quantrocket master calendar CME --in 30min
    """
    sectype_choices = ["STK", "FUT", "CASH", "OPT"]
    parser = _subparsers.add_parser(
        "calendar",
        help="check whether exchanges are open or closed",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "exchanges",
        metavar="EXCHANGE",
        nargs="+",
        help="the exchange(s) to check").completer = completers.exchange_calendar_completer
    parser.add_argument(
        "-t", "--sec-type",
        metavar="SEC_TYPE",
        choices=sectype_choices,
        help="the security type, if needed to disambiguate for exchanges that "
        f"trade multiple security types. Possible choices: {', '.join(sectype_choices)}"
        ).completer = completers.sec_type_completer(sectype_choices)
    timedelta_group = parser.add_mutually_exclusive_group()
    timedelta_group.add_argument(
        "-i", "--in",
        metavar="TIMEDELTA",
        dest="in_",
        help="check whether exchanges will be open or closed at this point in the "
        "future (use Pandas timedelta string, e.g. 2h or 30min or 1d)"
        ).completer = completers.timedelta_completer
    timedelta_group.add_argument(
        "-a", "--ago",
        metavar="TIMEDELTA",
        help="check whether exchanges were open or closed this long ago "
        "(use Pandas timedelta string, e.g. 2h or 30min or 1d)"
        ).completer = completers.timedelta_completer
    parser.add_argument(
        "-o", "--outside-rth",
        action="store_true",
        help="check extended hours calendar (default is to check regular "
        "trading hours calendar)")
    parser.set_defaults(func="quantrocket.master._cli_list_calendar_statuses")

    examples = """
Assert that one or more exchanges are open and exit non-zero if closed.

Intended to be used as a conditional for running other commands.

Notes
-----
Usage Guide:

* Trading Calendars: https://qrok.it/dl/qr/calendars

Examples
--------

Place Moonshot orders if NYSE is open now:

.. code-block:: bash

    quantrocket master isopen NYSE && quantrocket moonshot orders my-strategy | quantrocket blotter order -f -

Collect historical data for Australian stocks if the exchange was open 4 hours ago:

.. code-block:: bash

    quantrocket master isopen ASX --ago 4h && quantrocket history collect asx-stk-1d

Log a message if the London Stock Exchange will be open in 30 minutes:

.. code-block:: bash

    quantrocket master isopen LSE --in 30min && quantrocket flightlog log 'the market opens soon!'
    """
    sectype_choices = ["STK", "FUT", "CASH", "OPT"]
    parser = _subparsers.add_parser(
        "isopen",
        help="assert that one or more exchanges are open and exit non-zero if closed",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "exchanges",
        metavar="EXCHANGE",
        nargs="+",
        help="the exchange(s) to check").completer = completers.exchange_calendar_completer
    parser.add_argument(
        "-t", "--sec-type",
        metavar="SEC_TYPE",
        choices=sectype_choices,
        help="the security type, if needed to disambiguate for exchanges that "
        f"trade multiple security types. Possible choices: {', '.join(sectype_choices)}"
        ).completer = completers.sec_type_completer(sectype_choices)
    timedelta_group = parser.add_mutually_exclusive_group()
    timedelta_group.add_argument(
        "-i", "--in",
        metavar="TIMEDELTA",
        dest="in_",
        help="assert that exchanges will be open at this point in the "
        "future (use Pandas timedelta string, e.g. 2h or 30min or 1d)"
        ).completer = completers.timedelta_completer
    timedelta_group.add_argument(
        "-a", "--ago",
        metavar="TIMEDELTA",
        help="assert that exchanges were open this long ago "
        "(use Pandas timedelta string, e.g. 2h or 30min or 1d)"
        ).completer = completers.timedelta_completer
    sinceuntil_group = parser.add_mutually_exclusive_group()
    sinceuntil_group.add_argument(
        "-s", "--since",
        metavar="FREQ",
        help="assert that exchanges have been opened (as of --in or --ago if "
        "applicable) since at least this time (use Pandas frequency string, "
        "e.g. 'W' (week end), 'M' (month end), 'Q' (quarter end), 'A' (year end))"
        ).completer = completers.frequency_completer
    sinceuntil_group.add_argument(
        "-u", "--until",
        metavar="FREQ",
        help="assert that exchanges will be opened (as of --in or --ago if "
        "applicable) until at least this time (use Pandas frequency string, "
        "e.g. 'W' (week end), 'M' (month end), 'Q' (quarter end), 'A' (year end))"
        ).completer = completers.frequency_completer
    parser.add_argument(
        "-o", "--outside-rth",
        action="store_true",
        help="check extended hours calendar (default is to check regular "
        "trading hours calendar)")
    parser.set_defaults(func="quantrocket.master._cli_isopen")

    examples = """
Assert that one or more exchanges are closed and exit non-zero if open.

Intended to be used as a conditional for running other commands.

For --since/--until options, pass a Pandas frequency string, i.e. any string that
is a valid `freq` argument to `pd.date_range`. See:
https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#anchored-offsets

Notes
-----
Usage Guide:

* Trading Calendars: https://qrok.it/dl/qr/calendars

Examples
--------

Place Moonshot orders if the NYSE will be closed NYSE in 1 hour:

.. code-block:: bash

    quantrocket master isclosed NYSE --in 1h && quantrocket moonshot orders my-strategy | quantrocket blotter order -f -

Collect historical data for Australian stocks if the exchange is closed now but was
open 4 hours ago:

.. code-block:: bash

    quantrocket master isclosed ASX && quantrocket master isopen ASX --ago 4h && quantrocket history collect asx-stk-1d

Place Moonshot orders if the NYSE has been closed since month end:

.. code-block:: bash

    quantrocket master isclosed NYSE --since M && quantrocket moonshot orders monthly-rebalancing-strategy | quantrocket blotter order -f -

Place Moonshot orders if the NYSE will be closed in 1 hour and remain closed through quarter end:

.. code-block:: bash

    quantrocket master isclosed NYSE --in 1H --until Q && quantrocket moonshot orders end-of-quarter-strategy | quantrocket blotter order -f -
    """
    sectype_choices = ["STK", "FUT", "CASH", "OPT"]
    parser = _subparsers.add_parser(
        "isclosed",
        help="assert that one or more exchanges are closed and exit non-zero if open",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "exchanges",
        metavar="EXCHANGE",
        nargs="+",
        help="the exchange(s) to check").completer = completers.exchange_calendar_completer
    parser.add_argument(
        "-t", "--sec-type",
        metavar="SEC_TYPE",
        choices=sectype_choices,
        help="the security type, if needed to disambiguate for exchanges that "
        f"trade multiple security types. Possible choices: {', '.join(sectype_choices)}"
        ).completer = completers.sec_type_completer(sectype_choices)
    timedelta_group = parser.add_mutually_exclusive_group()
    timedelta_group.add_argument(
        "-i", "--in",
        metavar="TIMEDELTA",
        dest="in_",
        help="assert that exchanges will be closed at this point in the "
        "future (use Pandas timedelta string, e.g. 2h or 30min or 1d)"
        ).completer = completers.timedelta_completer
    timedelta_group.add_argument(
        "-a", "--ago",
        metavar="TIMEDELTA",
        help="assert that exchanges were closed this long ago "
        "(use Pandas timedelta string, e.g. 2h or 30min or 1d)"
        ).completer = completers.timedelta_completer
    sinceuntil_group = parser.add_mutually_exclusive_group()
    sinceuntil_group.add_argument(
        "-s", "--since",
        metavar="FREQ",
        help="assert that exchanges have been closed (as of --in or --ago if "
        "applicable) since at least this time (use Pandas frequency string, "
        "e.g. 'W' (week end), 'M' (month end), 'Q' (quarter end), 'A' (year end))"
        ).completer = completers.frequency_completer
    sinceuntil_group.add_argument(
        "-u", "--until",
        metavar="FREQ",
        help="assert that exchanges will be closed (as of --in or --ago if "
        "applicable) until at least this time (use Pandas frequency string, "
        "e.g. 'W' (week end), 'M' (month end), 'Q' (quarter end), 'A' (year end))"
        ).completer = completers.frequency_completer
    parser.add_argument(
        "-o", "--outside-rth",
        action="store_true",
        help="check extended hours calendar (default is to check regular "
        "trading hours calendar)")
    parser.set_defaults(func="quantrocket.master._cli_isclosed")

    examples = """
Round prices in a CSV file to valid tick sizes.

CSV should contain columns `Sid`, `Exchange`, and the columns to be rounded
(e.g. `LmtPrice`). Additional columns will be ignored and returned unchanged.

Notes
-----
Usage Guide:

* Tick sizes: https://qrok.it/dl/qr/tick-sizes

Examples
--------

Round the LmtPrice column in a CSV of orders and return a new CSV:

.. code-block:: bash

    quantrocket master ticksize -f orders.csv --round LmtPrice -o rounded_orders.csv

Round the StopPrice column in a CSV of orders and append the tick size as a
new column (called StopPriceTickSize):

.. code-block:: bash

    quantrocket master ticksize -f orders.csv -r StopPrice --append-ticksize -o rounded_orders.csv

Round the LmtPrice column in a CSV of Moonshot orders then place the orders:

.. code-block:: bash

    quantrocket moonshot orders umd-japan | quantrocket master ticksize -f - -r LmtPrice | quantrocket blotter order -f -
    """
    parser = _subparsers.add_parser(
        "ticksize",
        help="round prices in a CSV to valid tick sizes",
        epilog=examples,
        formatter_class=HelpFormatter)
    parser.add_argument(
        "-f", "--infile",
        required=True,
        metavar="INFILE",
        dest="infilepath_or_buffer",
        help="CSV file with prices to be rounded (specify '-' to read file from stdin)"
        ).completer = completers.infile_completer(["csv"], allow_stdin=True)
    parser.add_argument(
        "-r", "--round",
        nargs="+",
        required=True,
        metavar="FIELD",
        dest="round_fields",
        help="columns to be rounded").completer = completers.example_completer(["LmtPrice", "AuxPrice"])
    parser.add_argument(
        "-d", "--how",
        metavar="DIRECTION",
        choices=["up", "down", "nearest"],
        help="which direction to round to. Possible choices: up, down, nearest "
        "(default is 'nearest')")
    parser.add_argument(
        "-a", "--append-ticksize",
        action="store_true",
        help="append a column of tick sizes for each field to be rounded")
    parser.add_argument(
        "-o", "--outfile",
        metavar="OUTFILE",
        dest="outfilepath_or_buffer",
        help="filename to write the data to (default is stdout)").completer = completers.outfile_completer(
            ["csv"], outfile_prefix="rounded")
    parser.set_defaults(func="quantrocket.master._cli_round_to_tick_sizes")
