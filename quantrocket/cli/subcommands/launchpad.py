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

def add_subparser(subparsers):
    _parser = subparsers.add_parser("launchpad", description="QuantRocket IB Gateway service CLI", help="quantrocket launchpad -h")
    _subparsers = _parser.add_subparsers(title="subcommands", dest="subcommand")
    _subparsers.required = True

    parser = _subparsers.add_parser("status", help="list statuses and permissions of IB Gateway services")
    parser.add_argument("-e", "--exchanges", metavar="EXCHANGE", nargs="*", help="limit to IB Gateway services with market data permission for these exchanges")
    parser.add_argument("-t", "--sec-type", dest="sec_type", choices=["STK", "FUT"], help="limit to IB Gateway services with market data permission for these securitiy types (useful for disambiguating permissions for exchanges that trade multiple asset classes)")
    parser.add_argument("-s", "--status", choices=["running", "stopped", "error"], help="limit to IB Gateway services in this status")
    parser.add_argument("-g", "--gateways", metavar="SERVICE_NAME", nargs="*", help="limit to these IB Gateway services")
    parser.set_defaults(func="quantrocket.launchpad.list_gateway_statuses")

    parser = _subparsers.add_parser("start", help="start one or more IB Gateway services")
    parser.add_argument("-e", "--exchanges", metavar="EXCHANGE", nargs="*", help="limit to IB Gateway services with market data permission for these exchanges")
    parser.add_argument("-t", "--sec-type", dest="sec_type", choices=["STK", "FUT"], help="limit to IB Gateway services with market data permission for these securitiy types (useful for disambiguating permissions for exchanges that trade multiple asset classes)")
    parser.add_argument("-g", "--gateways", metavar="SERVICE_NAME", nargs="*", help="limit to these IB Gateway services")
    parser.set_defaults(func="quantrocket.launchpad.start_gateways")

    parser = _subparsers.add_parser("stop", help="stop one or more IB Gateway service")
    parser.add_argument("-e", "--exchanges", metavar="EXCHANGE", nargs="*", help="limit to IB Gateway services with market data permission for these exchanges")
    parser.add_argument("-t", "--sec-type", dest="sec_type", choices=["STK", "FUT"], help="limit to IB Gateway services with market data permission for these securitiy types (useful for disambiguating permissions for exchanges that trade multiple asset classes)")
    parser.add_argument("-g", "--gateways", metavar="SERVICE_NAME", nargs="*", help="limit to these IB Gateway services")
    parser.set_defaults(func="quantrocket.launchpad.stop_gateways")

    parser = _subparsers.add_parser("config", help="upload a new config, or return the current configuration")
    parser.add_argument("-f", "--filename", metavar="FILENAME", help="the config file to upload (if omitted, return the current config)")
    parser.set_defaults(func="quantrocket.launchpad._load_or_show_config")
