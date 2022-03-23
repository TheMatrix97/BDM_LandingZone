import argparse

from persist_landing import LandingLoadProcess
from temporal_load import TemporalLoadProcess


def build_arg_parser():
    parser = argparse.ArgumentParser(description='Landing Zone processes')
    parser.add_argument('--temporal-batch', dest='temporal_batch', action='store_true',
                        help='Run Temporal batch load')
    parser.add_argument('--datasource-folder', dest='datasource_folder', type=str,
                        help='Datasource base folder (required if --temporal-batch)')
    parser.add_argument('--persist-batch', dest='persist_batch', action='store_true',
                        help='Run Temporal to Persisted batch load')
    args = parser.parse_args()
    return args


def main():
    args = build_arg_parser()
    if args.temporal_batch:
        TemporalLoadProcess(args.datasource_folder).run_process()
    if args.persist_batch:
        LandingLoadProcess().run_process()


main()
