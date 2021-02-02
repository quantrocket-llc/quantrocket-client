# Copyright 2017 QuantRocket - All Rights Reserved
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

import getpass
from quantrocket.houston import houston
from quantrocket.exceptions import DataInsertionError
from quantrocket.cli.utils.output import json_to_cli

def list_databases(services=None, codes=None, detail=False, expand=False):
    """
    List databases.

    Parameters
    ----------
    services : str, optional
        limit to these services

    codes: list of str, optional
        limit to these codes

    detail : bool
        return database statistics (default is to return a
        flat list of database names)

    expand : bool
        expand sharded databases to include individual shards
        (default is to list sharded databases as a single database)

    Returns
    -------
    dict
        dict of lists of databases (one key for PostgreSQL databases and one for
        SQLite databases)

    Examples
    --------
    Load database details in a pandas DataFrame:

    >>> from quantrocket.db import list_databases
    >>> import itertools
    >>> databases = list_databases(detail=True)
    >>> databases = pd.DataFrame.from_records(itertools.chain(databases["sqlite"], databases["postgres"]))
    """
    params = {}
    if services:
        params["services"] = services
    if codes:
        params["codes"] = codes
    if detail:
        params["detail"] = detail
    if expand:
        params["expand"] = expand

    response = houston.get("/db/databases", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_list_databases(*args, **kwargs):
    return json_to_cli(list_databases, *args, **kwargs)

def get_s3_config():
    """
    Return the current S3 configuration, if any.

    See http://qrok.it/h/dbs3 to learn more.

    Returns
    -------
    dict
        configuration details
    """
    response = houston.get("/db/s3config")
    houston.raise_for_status_with_json(response)
    # It's possible to get a 204 empty response
    if not response.content:
        return {}
    return response.json()

def set_s3_config(access_key_id=None, secret_access_key=None, bucket=None, region=None):
    """
    Set AWS S3 configuration for pushing and pulling databases to and from
    S3.

    See http://qrok.it/h/dbs3 to learn more.

    Credentials are encrypted at rest and never leave your deployment.

    Parameters
    ----------
    access_key_id : str, optional
        AWS access key ID

    secret_access_key : str, optional
        AWS secret access key (if omitted and access_key_id is provided,
        will be prompted for secret_access_key)

    bucket : str, optional
        the S3 bucket name to push to/pull from

    region : str, optional
        the AWS region in which to create the bucket (default us-east-1).
        Ignored if the bucket already exists.

    Returns
    -------
    dict
        status message
    """
    if access_key_id and not secret_access_key:
        secret_access_key = getpass.getpass(prompt="Enter AWS Secret Access Key: ")

    data = {}
    if access_key_id:
        data["access_key_id"] = access_key_id
    if secret_access_key:
        data["secret_access_key"] = secret_access_key
    if bucket:
        data["bucket"] = bucket
    if region:
        data["region"] = region

    response = houston.put("/db/s3config", data=data)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_get_or_set_s3_config(access_key_id=None, secret_access_key=None,
                                   bucket=None, region=None, *args, **kwargs):
    if access_key_id or secret_access_key or bucket or region:
        return json_to_cli(set_s3_config, access_key_id, secret_access_key, bucket, region, *args, **kwargs)
    else:
        return json_to_cli(get_s3_config, *args, **kwargs)

def s3_push_databases(services=None, codes=None):
    """
    Push database(s) to Amazon S3.

    See http://qrok.it/h/dbs3 to learn more.

    Parameters
    ----------
    serivces : list of str, optional
        limit to these services

    codes: list of str, optional
        limit to these codes

    Returns
    -------
    json
        status message
    """
    params = {}
    if services:
        params["services"] = services
    if codes:
        params["codes"] = codes
    response = houston.put("/db/s3", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_s3_push_databases(*args, **kwargs):
    return json_to_cli(s3_push_databases, *args, **kwargs)

def s3_pull_databases(services=None, codes=None, force=False):
    """
    Pull database(s) from Amazon S3.

    See http://qrok.it/h/dbs3 to learn more.

    Parameters
    ----------
    serivces : list of str, optional
        limit to these services

    codes: list of str, optional
        limit to these codes

    force: bool
        overwrite existing database if one exists (default is to
        fail if one exists)

    Returns
    -------
    json
        status message
    """
    params = {}
    if services:
        params["services"] = services
    if codes:
        params["codes"] = codes
    if force:
        params["force"] = force
    response = houston.get("/db/s3", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_s3_pull_databases(*args, **kwargs):
    return json_to_cli(s3_pull_databases, *args, **kwargs)

def optimize_databases(services=None, codes=None):
    """
    Optimize databases to improve performance.

    This runs the 'VACUUM' command, which defragments the database and
    reclaims disk space.

    Parameters
    ----------
    serivces : list of str, optional
        limit to these service

    codes: list of str, optional
        limit to these codes

    Returns
    -------
    json
        status message
    """
    params = {}
    if codes:
        params["codes"] = codes
    if services:
        params["services"] = services
    response = houston.post("/db/optimizations", params=params)
    houston.raise_for_status_with_json(response)
    return response.json()

def _cli_optimize_databases(*args, **kwargs):
    return json_to_cli(optimize_databases, *args, **kwargs)

def connect_sqlite(db_path):
    """
    Returns a connection to a SQLite database.

    Parameters
    ----------
    db_path : str, required
        full path to a SQLite database

    Returns
    -------
    sqlalchemy.engine.Engine
        database connection
    """
    try:
        from sqlalchemy import create_engine
    except ImportError:
        raise ValueError(
            "this function requires sqlalchemy and must be run in a QuantRocket container")

    conn = create_engine("sqlite:///{0}".format(db_path),
                         connect_args={"isolation_level": None})
    # Set some speed optimizations
    # Hand off writes to the OS and don't wait
    conn.execute("PRAGMA synchronous = 0")
    # Each page is ~1K; allow ~50MB
    conn.execute("PRAGMA cache_size = 50000")
    # Store temp tables in memory
    conn.execute("PRAGMA temp_store = 2")
    # Wait up to 10 seconds rather than instantly failing on SQLITE_BUSY
    conn.execute("PRAGMA busy_timeout = 10000")

    return conn

def _insert_into(df, table_name, conn, on_conflict):

    import time
    import subprocess
    import os

    temp_table_name = "temp_{0}".format(str(time.time()).replace(".", ""))

    # Get the db path from the engine object
    db_path = conn.url.database

    temp_file_name = "/tmp/sqlite_{}.csv".format(temp_table_name)

    # Cast booleans to ints or they will load into SQLite as strings
    df_bools = df.select_dtypes(['bool'])
    if not df_bools.empty:
        df.loc[:, df_bools.columns] = df_bools.astype(int)

    df.to_csv(temp_file_name, index=False)

    # Close connection to avoid Database Is Locked
    conn.dispose()

    from_cols = [f"NULLIF({col}, '')" for col in df.columns]

    queries = """
PRAGMA busy_timeout = 10000;
.bail on
.mode csv
.import {tempfile} {temptable}
INSERT OR {on_conflict} INTO {table} ({into_cols}) SELECT {from_cols} FROM {temptable};
DROP TABLE {temptable};
    """.format(
        table=table_name,
        on_conflict=on_conflict,
        into_cols=",".join(df.columns),
        from_cols=",".join(from_cols),
        tempfile=temp_file_name,
        temptable=temp_table_name)

    try:
        subprocess.check_output(
            ["sqlite3", db_path],
            input=bytes(queries.encode("utf-8")),
            stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise DataInsertionError(e.output)

    os.remove(temp_file_name)

def insert_or_fail(df, table_name, conn):
    """
    Insert a DataFrame into a SQLite database.

    In the case of a duplicate record insertion, the function
    will fail.

    Parameters
    ----------
    df : DataFrame, required
        the DataFrame to insert. All DataFrame columns must
        exist in the destination table. The DataFrame index
        will not be inserted.

    table_name : str, required
        the name of the table to insert the DataFrame into.
        The table must already exist in the database.

    conn : sqlalchemy.engine.Engine, required
        a connection object for the SQLite database

    Returns
    -------
    None

    Raises
    ------
    quantrocket.exceptions.DataInsertionError
        catch-all exception class for errors that occur when writing to the
        SQLite database
    """
    _insert_into(df, table_name, conn, "FAIL")

def insert_or_replace(df, table_name, conn):
    """
    Insert a DataFrame into a SQLite database.

    In the case of a duplicate record insertion, the incoming
    record will replace the existing record.

    Parameters
    ----------
    df : DataFrame, required
        the DataFrame to insert. All DataFrame columns must
        exist in the destination table. The DataFrame index
        will not be inserted.

    table_name : str, required
        the name of the table to insert the DataFrame into.
        The table must already exist in the database.

    conn : sqlalchemy.engine.Engine, required
        a connection object for the SQLite database

    Returns
    -------
    None

    Raises
    ------
    quantrocket.exceptions.DataInsertionError
        catch-all exception class for errors that occur when writing to the
        SQLite database
    """
    _insert_into(df, table_name, conn, "REPLACE")

def insert_or_ignore(df, table_name, conn):
    """
    Insert a DataFrame into a SQLite database.

    In the case of a duplicate record insertion, the incoming
    record will be ignored.

    Parameters
    ----------
    df : DataFrame, required
        the DataFrame to insert. All DataFrame columns must
        exist in the destination table. The DataFrame index
        will not be inserted.

    table_name : str, required
        the name of the table to insert the DataFrame into.
        The table must already exist in the database.

    conn : sqlalchemy.engine.Engine, required
        a connection object for the SQLite database

    Returns
    -------
    None

    Raises
    ------
    quantrocket.exceptions.DataInsertionError
        catch-all exception class for errors that occur when writing to the
        SQLite database
    """
    _insert_into(df, table_name, conn, "IGNORE")
