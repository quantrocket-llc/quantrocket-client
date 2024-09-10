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

def account_completer(prefix, parsed_args, **kwargs):
    from quantrocket.account import download_account_balances
    import io
    import requests
    import json
    f = io.StringIO()
    try:
        download_account_balances(f, latest=True, output="json")
    except requests.HTTPError:
        return []

    return {item["Account"]: "available accounts" for item in json.loads(f.getvalue())}

def universe_completer(prefix, parsed_args, **kwargs):
    from quantrocket.master import list_universes
    return {k: f"universe with {v} securities" for k,v in list_universes().items()}

def example_completer(examples, description="examples only"):
    if not isinstance(examples, (list, tuple)):
        examples = [examples]

    def _completer(prefix, parsed_args, **kwargs):
        return {example: description for example in examples}
    return _completer

def completer_from_dict(choices):
    def _completer(prefix, parsed_args, **kwargs):
        return choices
    return _completer

def sid_completer(prefix, parsed_args, **kwargs):
    return {
        "FIBBG000B9XRY4": "examples only",
        "FIBBG000BDTBL9": "examples only",
    }

def symbol_completer(prefix, parsed_args, **kwargs):
    return {
        "AAPL": "examples only",
        "SPY": "examples only",
    }

def history_db_completer(prefix, parsed_args, **kwargs):
    from quantrocket.history import list_databases
    return {db: f"{db} history database" for db in list_databases()}

def history_db_fields_completer(prefix, parsed_args, **kwargs):
    if not parsed_args.code:
        return {}
    from quantrocket.history import get_db_config
    fields = get_db_config(parsed_args.code)["fields"]
    return {fieldname: f"{fieldtype} fields in {parsed_args.code}"
            for fieldname, fieldtype in fields.items()}

def realtime_db_completer(prefix, parsed_args, **kwargs):
    from quantrocket.realtime import list_databases
    dbs = list_databases()
    completions = {}
    for tick_db, agg_dbs in dbs.items():
        desc = f"tick and aggregate dbs for {tick_db}"
        completions[tick_db] = desc
        for agg_db in agg_dbs:
            completions[agg_db] = desc
    return completions

def realtime_tick_db_completer(prefix, parsed_args, **kwargs):
    from quantrocket.realtime import list_databases
    return {db: "tick databases" for db in list_databases().keys()}

def realtime_db_fields_completer(prefix, parsed_args, **kwargs):
    if hasattr(parsed_args, "codes"):
        codes = parsed_args.codes
    else:
        codes = parsed_args.code

    if not codes:
        return {}

    if not isinstance(codes, (list, tuple)):
        codes = [codes]
    from quantrocket.realtime import get_db_config
    completions = {}
    for db in codes:
        fields = get_db_config(db)["fields"]
        for field in fields:
            completions[field] = f"{db} fields"
    return completions

def realtime_agg_db_fields_completer(prefix, parsed_args, **kwargs):
    if not parsed_args.tick_db_code:
        return {}
    aggs = ['Close', 'Open', 'High', 'Low', 'Mean', 'Sum', 'Count']
    from quantrocket.realtime import get_db_config
    fields = get_db_config(parsed_args.tick_db_code)["fields"]
    completions = {}
    for field in fields:
        completions[f"{field}:{','.join(aggs)}"] = field
    return completions

def bundle_completer(prefix, parsed_args, **kwargs):
    from quantrocket.zipline import list_bundles
    return list(list_bundles().keys())

def bundle_from_db_fields_completer(prefix, parsed_args, **kwargs):
    if not parsed_args.from_db:
        return {}
    zipline_fields = ['close', 'open', 'high', 'low', 'volume']
    from quantrocket.history import get_db_config as get_history_db_config
    from quantrocket.realtime import get_db_config as get_realtime_db_config
    completions = {}
    for db in parsed_args.from_db:
        try:
            fields = get_history_db_config(db)["fields"]
        except:
            try:
                fields = get_realtime_db_config(db)["fields"]
            except:
                pass
        for field in fields:
            for zipline_field in zipline_fields:
                completions[f"{zipline_field}:{field}"] = field
    return completions

def ibg_completer(prefix, parsed_args, **kwargs):
    from quantrocket.ibg import list_gateway_statuses
    return list(list_gateway_statuses().keys())

def start_date_completer(prefix, parsed_args, **kwargs):
    import datetime
    return [str(datetime.date.today() - datetime.timedelta(days=365))]

def end_date_completer(prefix, parsed_args, **kwargs):
    import datetime
    return [str(datetime.date.today())]

def timezone_completer(prefix, parsed_args, **kwargs):
    import zoneinfo
    return sorted(zoneinfo.available_timezones())

def order_ref_completer(prefix, parsed_args, **kwargs):
    return {
        "some-order-ref": "examples only",
    }

def directory_completer(prefix, parsed_args, **kwargs):
    """
    This completer is for directories, not files. Should be used
    when the user picks an existing directory.
    """
    from argcomplete.completers import DirectoriesCompleter
    dirs = list(DirectoriesCompleter()(prefix, **kwargs))
    dirs.sort()
    return dirs

def infile_completer(filetypes, allow_stdin=False):
    """
    Completer for input files. Files of the specified filetypes
    will be shown, along with an entry for stdin if applicable.
    Example items will be shown for any filetypes without an actual
    file.
    """
    def _completer(prefix, parsed_args, **kwargs):
        import os
        from argcomplete.completers import FilesCompleter
        all_files = FilesCompleter(directories=False)(prefix, **kwargs)
        # put most recent file first, then sort alphabetically
        all_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        all_files = all_files[0:1] + sorted(all_files[1:])
        infiles = {}
        if allow_stdin:
            infiles["-"] = "stdin"
        for file in all_files:
            if any([file.endswith("." + x) for x in filetypes]):
                infiles[file] = os.path.splitext(file)[1][1:]

        for filetype in filetypes:
            if filetype not in infiles.values():
                infiles["example." + filetype] = filetype + " (example only, this file does not exist)"
        return infiles
    return _completer

def outfile_completer(filetypes, outfile_prefix=None):
    """
    Completer for output files. Uses the outfile_prefix and
    filetypes to create a list of possible output file name
    suggestions. If the output has already been selected,
    limits the suggestions to that filetype. If a suggested
    file already exists, increments the filename to avoid a
    collision.
    """
    def _completer(prefix, parsed_args, **kwargs):
        import os
        filename = outfile_prefix or "_".join([
                parsed_args.command,
                parsed_args.subcommand,
                "results"])
        selected_output = getattr(parsed_args, "output", None)
        outfiles = {}
        for filetype in filetypes:
            if selected_output and filetype not in selected_output:
                continue
            file = filename + "." + filetype
            i = 1
            while os.path.exists(file):
                file = filename + str(i) + "." + filetype
                i += 1
            outfiles[file] = filetype

        return outfiles

    return _completer

def codeload_repo_completer(prefix, parsed_args, **kwargs):
        import requests
        all_repos = []
        url = "https://api.github.com/orgs/quantrocket-codeload/repos"
        while True:
            response = requests.get(
                url,
                params={"per_page": 100}
            )
            repos = response.json()
            all_repos.extend(repos)
            if "next" in response.links:
                url = response.links["next"]["url"]
            else:
                break

        all_repos.sort(key=lambda repo: repo["name"])
        return {repo["name"]: repo["description"] for repo in all_repos}

def bundle_data_frequency_completer(prefix, parsed_args, **kwargs):
    return {"daily": "daily prices", "minute": "minute prices"}

def sec_type_completer(sec_types=None):

    requested_sec_types = sec_types
    def _completer(prefix, parsed_args, **kwargs):
        all_sec_type_choices = {
            "STK": "stocks",
            "ETF": "ETFs",
            "FUT": "futures",
            "CASH": "currencies",
            "IND": "indices",
            "OPT": "options",
            "FOP": "futures options",
            "BAG": "combos",
            "CFD": "CFDs"
        }

        if requested_sec_types is None:
            return all_sec_type_choices

        sec_types = {sec_type: all_sec_type_choices[sec_type] for sec_type in requested_sec_types}
        return sec_types

    return _completer

def ibkr_country_completer(prefix, parsed_args, **kwargs):
    from quantrocket.master import list_ibkr_exchanges
    exchanges = list_ibkr_exchanges()

    completions = set()
    for _, countries in exchanges.items():
        for country in countries:
            completions.add(country)

    completions.add("FREE")
    return completions

def ibkr_exchange_completer(prefix, parsed_args, **kwargs):
    from quantrocket.master import list_ibkr_exchanges
    exchanges = list_ibkr_exchanges()

    completions = {}
    for _, countries in exchanges.items():
        for country, exchanges in countries.items():
            for exchange in exchanges:
                completions[exchange] = country

    return completions

def exchange_calendar_completer(prefix, parsed_args, **kwargs):
    try:
        from exchange_calendars import get_calendar_names
    except ImportError:
        return []
    return get_calendar_names()

def currency_completer(prefix, parsed_args, **kwargs):
    from quantrocket._cli.utils.completers.currencies import ECB_CURRENCIES
    return ECB_CURRENCIES

def timedelta_completer(prefix, parsed_args, **kwargs):
    return {s: "Pandas Timedelta string (examples only)" for s in [
        "5min", "2h", "30sec", "1d"
    ]}

def frequency_completer(prefix, parsed_args, **kwargs):
    return {s: "Pandas frequency (examples only)" for s in [
        "W", "M", "Q", "Y"
    ]}

def param_val_completer(prefix, parsed_args, **kwargs):
    return {"MY_PARAM:MY_VAL": "examples only"}

def moonshot_nlv_completer(prefix, parsed_args, **kwargs):
    return {s: "examples only" for s in ["USD:1000000", "EUR:1000000", "GBP:1000000"]}

def moonshot_segment_frequency_completer(prefix, parsed_args, **kwargs):
    return ["Y"]

def paramscan_param_completer(prefix, parsed_args, **kwargs):
    return {"MY_PARAM": "examples only"}

def paramscan_vals_completer(prefix, parsed_args, **kwargs):
    return {s: "examples only" for s in ["MY_VAL"]}

def backtest_num_workers_completer(prefix, parsed_args, **kwargs):
    return [str(i) for i in [2, 3, 4]]

def moonshot_strategy_completer(category):
    def _completer(prefix, parsed_args, **kwargs):
        from quantrocket.houston import houston
        return houston.get(f"/moonshot/strategies/{category}").json()

    return _completer

def moonshot_allocations_completer(prefix, parsed_args, **kwargs):
    if hasattr(parsed_args, "strategies"):
        strategies = parsed_args.strategies
    else:
        strategies = [parsed_args.strategy]
    if not strategies:
        return []
    allocation = round(1/len(strategies), 2)
    return {f"{s}:{allocation}": "strategy:pct_allocation" for s in strategies}

def zipline_strategy_completer(prefix, parsed_args, **kwargs):
    import os
    strategies = []
    zipline_dir = "/codeload/zipline"
    for f in os.listdir(zipline_dir):
        if not f.endswith(".py"):
            continue
        with open(zipline_dir + "/" + f) as f_obj:
            if "def initialize" not in f_obj.read():
                continue
        strategies.append(os.path.splitext(f)[0])
    return sorted(strategies, key=lambda s: s.lower())

def zipline_capital_base_completer(prefix, parsed_args, **kwargs):
    return {s: "examples only" for s in ["1e6", "500000"]}

def get_choices_from_type_hint(func, param):
    import typing
    args = typing.get_args(func.__annotations__[param])
    if not isinstance(args, tuple):
        args = (args,)
    completions = []
    for arg in args:
        if typing.get_origin(arg) is not typing.Literal:
            continue
        completions.extend(typing.get_args(arg))
    return completions

def master_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.master import download_master_file
    return get_choices_from_type_hint(download_master_file, "fields")

def diff_ibkr_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.master import diff_ibkr_securities
    return get_choices_from_type_hint(diff_ibkr_securities, "fields")

def account_balance_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.account import download_account_balances
    return get_choices_from_type_hint(download_account_balances, "fields")

def account_portfolio_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.account import download_account_portfolio
    return get_choices_from_type_hint(download_account_portfolio, "fields")

def order_status_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.blotter import download_order_statuses
    return get_choices_from_type_hint(download_order_statuses, "fields")

def sharadar_fundamentals_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.fundamental import download_sharadar_fundamentals
    return get_choices_from_type_hint(download_sharadar_fundamentals, "fields")

def sharadar_insiders_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.fundamental import download_sharadar_insiders
    return get_choices_from_type_hint(download_sharadar_insiders, "fields")

def sharadar_institutions_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.fundamental import download_sharadar_institutions
    return get_choices_from_type_hint(download_sharadar_institutions, "fields")

def ibkr_realtime_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.realtime import create_ibkr_tick_db
    return get_choices_from_type_hint(create_ibkr_tick_db, "fields")

def polygon_realtime_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.realtime import create_polygon_tick_db
    return get_choices_from_type_hint(create_polygon_tick_db, "fields")

def alpaca_realtime_fields_completer(prefix, parsed_args, **kwargs):
    from quantrocket.realtime import create_alpaca_tick_db
    return get_choices_from_type_hint(create_alpaca_tick_db, "fields")
