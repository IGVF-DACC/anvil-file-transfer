import os

import argparse

from dataclasses import dataclass


context = {
    'sandbox': {
        'base_url': 'https://api.data.igvf.org',
        'source_workspace_id': '0f7ac85e-9aef-482c-9fb1-73c14877c2f8',
        'destination_workspace_id': '7d3c9ef1-99c2-4948-9811-fe79d626219f',
        'source_storage_resource_id': '33cc593d-bd19-4d1b-8bd4-7891a71293fb',
        'destination_storage_resource_id': 'd72ebb65-b7a9-4ebe-9815-3fe948715498',
    },
    'prod': {
        'base_url': 'https://api.sandbox.igvf.org',
        'source_workspace_id': '3201e576-2410-4dd8-9799-cb5e431333ae',
        'destination_workspace_id': 'b7c48e0a-02df-4e12-b026-4070c017359e',
        'source_storage_resource_id': '14be87fd-0130-4064-85b8-dceb8972ab11',
        'destination_storage_resource_id': '121eb709-b2e6-4d85-a76e-91fcb422654e',

    },
    'terra_api_url': 'https://workspace.dsde-prod.broadinstitute.org'
}


@dataclass
class TransferProps:
    google_sa_base64: str
    env: str
    portal_key: str
    portal_secret_key: str


def transfer(props: TransferProps):
    print(props)
    return


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--google-sa-base64', required=True, type=str, help='Google service account linked to Terra workspace, base64 encoded')
    parser.add_argument('--env', type=str, choices=['sandbox', 'prod'], help='Specify the environment: prod or sandbox.')
    parser.add_argument('--portal-key', required=True, type=str)
    parser.add_argument('--portal-secret-key', required=True, type=str)
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    props = TransferProps(
        google_sa_base64=args.google_sa_base64,
        env=args.env,
        portal_key=args.portal_key,
        portal_secret_key=args.portal_secret_key,
    )
    transfer(props)
