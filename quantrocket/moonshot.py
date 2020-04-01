# Copyright 2019 QuantRocket - All Rights Reserved
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

import sys
import os.path
import six
from zipfile import ZipFile
from quantrocket.houston import houston
from quantrocket.cli.utils.output import json_to_cli
from quantrocket.cli.utils.files import write_response_to_filepath_or_buffer
from quantrocket.cli.utils.parse import dict_strs_to_dict, dict_to_dict_strs
from quantrocket.utils.parse import _read_moonshot_or_pnl_csv

def backtest(strategies, start_date=None, end_date=None, segment=None, allocations=None,
             nlv=None, params=None, details=None, output="csv", filepath_or_buffer=None,
             no_cache=False):
    """
    Backtest one or more strategies.

    By default returns a CSV of backtest results but can also return a PDF tear sheet
    of performance charts.

    If testing multiple strategies, each column in the CSV represents a strategy.
    If testing a single strategy and `details=True`, each column in the CSV
    represents a security in the strategy universe.

    Parameters
    ----------
    strategies : list of str, required
        one or more strategy codes

    start_date : str (YYYY-MM-DD), optional
        the backtest start date (default is to use all available history)

    end_date : str (YYYY-MM-DD), optional
        the backtest end date (default is to use all available history)

    segment : str, optional
        backtest in date segments of this size, to reduce memory usage
        (use Pandas frequency string, e.g. 'A' for annual segments or 'Q'
        for quarterly segments)

    allocations : dict of CODE:FLOAT, optional
        the allocation for each strategy, passed as {code:allocation} (default
        allocation is 1.0 / number of strategies)

    nlv : dict of CURRENCY:NLV, optional
        the NLV (net liquidation value, i.e. account balance) to assume for
        the backtest, expressed in each currency represented in the backtest (pass
        as {currency:nlv})

    params : dict of PARAM:VALUE, optional
        one or more strategy params to set on the fly before backtesting
        (pass as {param:value})

    details : bool
        return detailed results for all securities instead of aggregating to
        strategy level (only supported for single-strategy backtests)

    output : str, required
        the output format (choices are csv or pdf)

    filepath_or_buffer : str, optional
        the location to write the results file (omit to write to stdout)

    no_cache : bool
        don't use cached files even if available. Using cached files speeds
        up backtests but may be undesirable if underlying data has changed.
        See http://qrok.it/h/mcache to learn more about caching in Moonshot.

    Returns
    -------
    None

    Examples
    --------
    Backtest several HML (High Minus Low) strategies from 2005-2015 and return a
    CSV of results:

    >>> backtest(["hml-us", "hml-eur", "hml-asia"],
                 start_date="2005-01-01",
                 end_date="2015-12-31",
                 filepath_or_buffer="hml_results.csv")

    Run a backtest in 1-year segments to reduce memory usage:

    >>> backtest("big-strategy",
                 start_date="2000-01-01",
                 end_date="2018-01-01",
                 segment="A",
                 filepath_or_buffer="results.csv")

    See Also
    --------
    read_moonshot_csv : load a Moonshot backtest CSV into a DataFrame
    """
    output = output or "csv"

    if output not in ("csv", "pdf"):
        raise ValueError("invalid output: {0} (choices are csv or pdf".format(output))

    _params = {}

    if strategies:
        _params["strategies"] = strategies
    if start_date:
        _params["start_date"] = start_date
    if end_date:
        _params["end_date"] = end_date
    if segment:
        _params["segment"] = segment
    if allocations:
        _params["allocations"] = dict_to_dict_strs(allocations)
    if nlv:
        _params["nlv"] = dict_to_dict_strs(nlv)
    if details:
        _params["details"] = details
    if params:
        _params["params"] = dict_to_dict_strs(params)
    if no_cache:
        _params["no_cache"] = no_cache

    response = houston.post("/moonshot/backtests.{0}".format(output),
                            params=_params, timeout=60*60*24)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_backtest(*args, **kwargs):
    allocations = kwargs.get("allocations", None)
    if allocations:
        kwargs["allocations"] = dict_strs_to_dict(*allocations)
    nlv = kwargs.get("nlv", None)
    if nlv:
        kwargs["nlv"] = dict_strs_to_dict(*nlv)
    params = kwargs.get("params", None)
    if params:
        kwargs["params"] = dict_strs_to_dict(*params)
    return json_to_cli(backtest, *args, **kwargs)

def read_moonshot_csv(filepath_or_buffer):
    """
    Load a Moonshot backtest CSV into a DataFrame.

    This is a light wrapper around pd.read_csv that handles setting index
    columns and casting to proper data types.

    Parameters
    ----------
    filepath_or_buffer : string or file-like, required
        path to CSV

    Returns
    -------
    DataFrame
        a multi-index (Field, Date[, Time]) DataFrame of backtest
        results, with sids or strategy codes as columns

    Examples
    --------
    >>> results = read_moonshot_csv("moonshot_backtest.csv")
    >>> returns = results.loc["Return"]
    """
    return _read_moonshot_or_pnl_csv(filepath_or_buffer)

def scan_parameters(strategies, start_date=None, end_date=None, segment=None,
                    param1=None, vals1=None, param2=None, vals2=None,
                    allocations=None, nlv=None, params=None, output="csv",
                    filepath_or_buffer=None, no_cache=False):
    """
    Run a parameter scan for one or more strategies.

    By default returns a CSV of scan results but can also return a PDF tear sheet.

    Parameters
    ----------
    strategies : list of str, required
        one or more strategy codes

    start_date : str (YYYY-MM-DD), optional
        the backtest start date (default is to use all available history)

    end_date : str (YYYY-MM-DD), optional
        the backtest end date (default is to use all available history)

    segment : str, optional
        backtest in date segments of this size, to reduce memory usage
        (use Pandas frequency string, e.g. 'A' for annual segments or 'Q'
        for quarterly segments)

    param1 : str, required
        the name of the parameter to test (a class attribute on the strategy)

    vals1 : list of int/float/str/tuple, required
        parameter values to test (values can be ints, floats, strings, False,
        True, None, 'default' (to test current param value), or lists of
        ints/floats/strings)

    param2 : str, optional
        name of a second parameter to test (for 2-D parameter scans)

    vals2 : list of int/float/str/tuple, optional
        values to test for parameter 2 (values can be ints, floats, strings,
        False, True, None, 'default' (to test current param value), or lists
        of ints/floats/strings)

    allocations : dict of CODE:FLOAT, optional
        the allocation for each strategy, passed as {code:allocation} (default
        allocation is 1.0 / number of strategies)

    nlv : dict of CURRENCY:NLV, optional
        the NLV (net liquidation value, i.e. account balance) to assume for
        the backtest, expressed in each currency represented in the backtest (pass
        as {currency:nlv})

    params : dict of PARAM:VALUE, optional
        one or more strategy params to set on the fly before backtesting
        (pass as {param:value})

    output : str, required
        the output format (choices are csv or pdf)

    filepath_or_buffer : str, optional
        the location to write the results file (omit to write to stdout)

    no_cache : bool
        don't use cached files even if available. Using cached files speeds
        up backtests but may be undesirable if underlying data has changed.
        See http://qrok.it/h/mcache to learn more about caching in Moonshot.

    Returns
    -------
    None

    Examples
    --------
    Run a parameter scan for several different moving averages on a strategy
    called trend-friend and return a CSV (which can be rendered with Moonchart):

    >>> scan_parameters("trend-friend",
                        param1="MAVG_WINDOW",
                        vals1=[20, 50, 100],
                        filepath_or_buffer="trend_friend_MAVG_WINDOW.csv")

    Run a 2-D parameter scan for multiple strategies and return a CSV:

    >>> scan_parameters(["strat1", "strat2", "strat3"],
                        param1="MIN_STD",
                        vals1=[1, 1.5, 2],
                        param2="STD_WINDOW",
                        vals2=[20, 50, 100, 200],
                        filepath_or_buffer="strategies_MIN_STD_and_STD_WINDOW.csv")

    Run a parameter scan in 1-year segments to reduce memory usage:

    >>> scan_parameters("big-strategy",
                        start_date="2000-01-01",
                        end_date="2018-01-01",
                        segment="A",
                        param1="MAVG_WINDOW",
                        vals1=[20, 50, 100],
                        filepath_or_buffer="big_strategy_MAVG_WINDOW.csv")
    """
    output = output or "csv"

    if output not in ("csv", "pdf"):
        raise ValueError("invalid output: {0} (choices are csv or pdf".format(output))

    _params = {}
    if strategies:
        _params["strategies"] = strategies
    if start_date:
        _params["start_date"] = start_date
    if end_date:
        _params["end_date"] = end_date
    if segment:
        _params["segment"] = segment
    if param1:
        _params["param1"] = param1
    if vals1:
        _params["vals1"] = [str(v) for v in vals1]
    if param2:
        _params["param2"] = param2
    if vals2:
        _params["vals2"] = [str(v) for v in vals2]
    if allocations:
        _params["allocations"] = dict_to_dict_strs(allocations)
    if nlv:
        _params["nlv"] = dict_to_dict_strs(nlv)
    if params:
        _params["params"] = dict_to_dict_strs(params)
    if no_cache:
        _params["no_cache"] = no_cache

    response = houston.post("/moonshot/paramscans.{0}".format(output), params=_params, timeout=60*60*24)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_scan_parameters(*args, **kwargs):
    allocations = kwargs.get("allocations", None)
    if allocations:
        kwargs["allocations"] = dict_strs_to_dict(*allocations)
    nlv = kwargs.get("nlv", None)
    if nlv:
        kwargs["nlv"] = dict_strs_to_dict(*nlv)
    params = kwargs.get("params", None)
    if params:
        kwargs["params"] = dict_strs_to_dict(*params)
    return json_to_cli(scan_parameters, *args, **kwargs)

def ml_walkforward(strategy, start_date, end_date, train, min_train=None, rolling_train=None,
                   model_filepath=None, force_nonincremental=None, segment=None,
                   allocation=None, nlv=None, params=None,
                   details=None, progress=False, filepath_or_buffer=None,
                   no_cache=False):
    """
    Run a walk-forward optimization of a machine learning strategy.

    The date range will be split into segments of `train` size. For each
    segment, the model will be trained with the data, then the trained model will
    be backtested on the following segment.

    By default, uses scikit-learn's StandardScaler+SGDRegressor. Also supports other
    scikit-learn models/pipelines and Keras models. To customize model, instantiate
    the model locally, serialize it to disk, and pass the path of the serialized
    model as `model_filepath`.

    Supports expanding walk-forward optimizations (the default), which use an anchored start date
    for model training, or rolling walk-forward optimizations (by specifying `rolling_train`),
    which use a rolling or non-anchored start date for model training.

    Returns a backtest results CSV and a dump of the machine learning model
    as of the end of the analysis.

    Parameters
    ----------
    strategy : str, required
        the strategy code

    start_date : str (YYYY-MM-DD), required
        the analysis start date (note that model training will start on this date
        but backtesting will not start until after the initial training period)

    end_date : str (YYYY-MM-DD), required
        the analysis end date

    train : str, required
        train model this frequently (use Pandas frequency string, e.g. 'A'
        for annual training or 'Q' for quarterly training)

    min_train : str, optional
        don't backtest until at least this much model training has occurred;
        defaults to the length of `train` if not specified (use Pandas frequency
        string, e.g. '5Y' for 5 years of initial training)

    rolling_train : str, optional
        train model with a rolling window of this length; if omitted, train
        model with an expanding window (use Pandas frequency string, e.g. '3Y' for
        a 3-year rolling training window)

    model_filepath : str, optional
        filepath of serialized model to use, filename must end in ".joblib" or
        ".pkl" (if omitted, default model is scikit-learn's StandardScaler+SGDRegressor)

    force_nonincremental : bool, optional
        force the model to be trained non-incrementally (i.e. load entire training
        data set into memory) even if it supports incremental learning. Must be True
        in order to perform a rolling (as opposed to expanding) walk-forward optimization
        with a model that supports incremental learning. Default False.

    segment : str, optional
        train and backtest in date segments of this size, to reduce memory usage;
        must be smaller than `train`/`min_train` or will have no effect (use Pandas frequency string,
        e.g. 'A' for annual segments or 'Q' for quarterly segments)

    allocation : float, optional
        the allocation for the strategy (default 1.0)

    nlv : dict of CURRENCY:NLV, optional
        the NLV (net liquidation value, i.e. account balance) to assume for
        the backtest, expressed in each currency represented in the backtest (pass
        as {currency:nlv})

    params : dict of PARAM:VALUE, optional
        one or more strategy params to set on the fly before backtesting
        (pass as {param:value})

    details : bool
        return detailed results for all securities instead of aggregating

    progress : bool
        log status and Sharpe ratios of each walk-forward segment during analysis
        (default False)

    filepath_or_buffer : str, optional
        the location to write the ZIP file to; or, if path ends with "*", the
        pattern to use for extracting the zipped files. For example, if the path is
        my_ml*, files will extracted to my_ml_results.csv and my_ml_trained_model.joblib.

    no_cache : bool
        don't use cached files even if available. Using cached files speeds
        up backtests but may be undesirable if underlying data has changed.
        See http://qrok.it/h/mcache to learn more about caching in Moonshot.

    Returns
    -------
    None

    Examples
    --------
    Run a walk-forward optimization using the default model and retrain the model
    annually, writing the backtest results and trained model to demo_ml_results.csv
    and demo_ml_trained_model.joblib, respectively:

    >>> ml_walkforward(
            "demo-ml",
            "2007-01-01",
            "2018-12-31",
            train="A",
            filepath_or_buffer="demo_ml*")

    Create a scikit-learn model, serialize it with joblib, and use it to
    run the walkforward backtest:

    >>> from sklearn.linear_model import SGDClassifier
    >>> import joblib
    >>> clf = SGDClassifier()
    >>> joblib.dump(clf, "my_model.joblib")
    >>> ml_walkforward(
            "demo-ml",
            "2007-01-01",
            "2018-12-31",
            train="A",
            model_filepath="my_model.joblib",
            filepath_or_buffer="demo_ml*")

    Run a walk-forward optimization using a custom model (serialized with joblib),
    retrain the model annually, don't perform backtesting until after 5 years
    of initial training, and further split the training and backtesting into
    quarterly segments to reduce memory usage:

    >>> ml_walkforward(
            "demo-ml",
            "2007-01-01",
            "2018-12-31",
            model_filepath="my_model.joblib",
            train="A",
            min_train="5Y",
            segment="Q",
            filepath_or_buffer="demo_ml*")

    Create a Keras model, serialize it, and use it to run the walkforward backtest:

    >>> from keras.models import Sequential
    >>> from keras.layers import Dense
    >>> model = Sequential()
    >>> # input_dim should match number of features in training data
    >>> model.add(Dense(units=4, activation='relu', input_dim=5))
    >>> # last layer should have a single unit
    >>> model.add(Dense(units=1, activation='softmax'))
    >>> model.compile(loss='sparse_categorical_crossentropy',
                      optimizer='sgd',
                      metrics=['accuracy'])
    >>> model.save('my_model.keras.h5')
    >>> ml_walkforward(
            "neuralnet-ml",
            "2007-01-01",
            "2018-12-31",
            train="A",
            model_filepath="my_model.keras.h5",
            filepath_or_buffer="neuralnet_ml*")
    """
    _params = {}

    _params["start_date"] = start_date
    _params["end_date"] = end_date
    _params["train"] = train
    if min_train:
        _params["min_train"] = min_train
    if rolling_train:
        _params["rolling_train"] = rolling_train
    if force_nonincremental:
        _params["force_nonincremental"] = force_nonincremental
    if segment:
        _params["segment"] = segment
    if allocation:
        _params["allocation"] = allocation
    if nlv:
        _params["nlv"] = dict_to_dict_strs(nlv)
    if details:
        _params["details"] = details
    if progress:
        _params["progress"] = progress
    if params:
        _params["params"] = dict_to_dict_strs(params)
    if no_cache:
        _params["no_cache"] = no_cache

    url = "/moonshot/ml/walkforward/{0}.zip".format(strategy)

    if model_filepath:
        # Send the filename as a hint how to open it
        _params["model_filename"] = os.path.basename(model_filepath)

        with open(model_filepath, "rb") as f:
            response = houston.post(url, data=f, params=_params, timeout=60*60*24)
    else:
        response = houston.post(url, params=_params, timeout=60*60*24)

    houston.raise_for_status_with_json(response)

    filepath_or_buffer = filepath_or_buffer or sys.stdout

    auto_extract = isinstance(filepath_or_buffer, six.string_types) and filepath_or_buffer.endswith("*")

    if auto_extract:
        base_filepath = filepath_or_buffer[:-1]
        zipfilepath = base_filepath + ".zip"

        write_response_to_filepath_or_buffer(zipfilepath, response)

        with ZipFile(zipfilepath, mode="r") as zfile:

            model_filename = [name for name in zfile.namelist() if "model" in name][0]
            model_filepath = base_filepath + "_" + model_filename
            csv_filepath = base_filepath + "_results.csv"

            with open(csv_filepath, "wb") as csvfile:
                csvfile.write(zfile.read("results.csv"))

            with open(model_filepath, "wb") as modelfile:
                modelfile.write(zfile.read(model_filename))

        os.remove(zipfilepath)

    else:
        write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_ml_walkforward(*args, **kwargs):
    nlv = kwargs.get("nlv", None)
    if nlv:
        kwargs["nlv"] = dict_strs_to_dict(*nlv)
    params = kwargs.get("params", None)
    if params:
        kwargs["params"] = dict_strs_to_dict(*params)
    return json_to_cli(ml_walkforward, *args, **kwargs)

def trade(strategies, accounts=None, review_date=None, output="csv", filepath_or_buffer=None):
    """
    Run one or more strategies and generate orders.

    Allocations are read from configuration (quantrocket.moonshot.allocations.yml).

    Parameters
    ----------
    strategies : list of str, required
        one or more strategy codes

    accounts : list of str, optional
        limit to these accounts

    review_date : str (YYYY-MM-DD), optional
        generate orders as if it were this date, rather than using today's date

    output : str, required
        the output format (choices are csv or json)

    filepath_or_buffer : str, optional
        the location to write the orders file (omit to write to stdout)

    Returns
    -------
    None
    """
    params = {}
    if strategies:
        params["strategies"] = strategies
    if accounts:
        params["accounts"] = accounts
    if review_date:
        params["review_date"] = review_date

    output = output or "csv"

    if output not in ("csv", "json"):
        raise ValueError("invalid output: {0} (choices are csv or json".format(output))

    response = houston.get("/moonshot/orders.{0}".format(output), params=params, timeout=60*5)

    houston.raise_for_status_with_json(response)

    # Don't write a null response to file
    if response.content[:4] == b"null":
        return

    filepath_or_buffer = filepath_or_buffer or sys.stdout
    write_response_to_filepath_or_buffer(filepath_or_buffer, response)

def _cli_trade(*args, **kwargs):
    return json_to_cli(trade, *args, **kwargs)
