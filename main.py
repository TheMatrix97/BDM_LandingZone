import argparse

from temporal_load import batch_load_temporal


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
        batch_load_temporal(args.datasource_folder)
    if args.persist_batch:
        print('TODO')
        # batch load persist


main()
