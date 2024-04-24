import os

import argparse


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--google-sa-base64', required=True, type=str, help='Google service account linked to Terra workspace, base64 encoded')
    parser.add_argument('--portal-key', required=True, type=str)
    parser.add_argument('--portal-secret-key', required=True, type=str)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    print(args)
