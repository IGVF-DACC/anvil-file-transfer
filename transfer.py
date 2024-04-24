import argparse

import base64

import json

import os

import requests # type: ignore

from google.auth.transport.requests import AuthorizedSession

from google.oauth2 import service_account

from azure.storage.blob import BlobClient

from dataclasses import dataclass

from typing import Optional
from typing import Tuple


@dataclass
class Context:
    portal_api_url: str
    source_workspace_id: str
    destination_workspace_id: str
    source_storage_resource_id: str
    destination_storage_resource_id: str


@dataclass
class TransferProps:
    google_service_account_credentials: dict[str, str]
    env: str
    portal_auth: Tuple[str, str]
    context: Context
    delete_source_files: bool
    terra_api_url: str = 'https://workspace.dsde-prod.broadinstitute.org'
    session: Optional[AuthorizedSession] = None
    source_sas_token: Optional[str] = None
    destination_sas_token: Optional[str] = None


@dataclass
class File:
    id_: str
    anvil_source_url: str
    anvil_destination_url: str
    upload_status: str


ENVIRONMENT = {
    'sandbox': Context(
        portal_api_url='https://api.sandbox.igvf.org',
        source_workspace_id='0f7ac85e-9aef-482c-9fb1-73c14877c2f8',
        destination_workspace_id='7d3c9ef1-99c2-4948-9811-fe79d626219f',
        source_storage_resource_id='33cc593d-bd19-4d1b-8bd4-7891a71293fb',
        destination_storage_resource_id='d72ebb65-b7a9-4ebe-9815-3fe948715498',
    ),
    'prod': Context(
        portal_api_url='https://api.data.igvf.org',
        source_workspace_id='3201e576-2410-4dd8-9799-cb5e431333ae',
        destination_workspace_id='b7c48e0a-02df-4e12-b026-4070c017359e',
        source_storage_resource_id='14be87fd-0130-4064-85b8-dceb8972ab11',
        destination_storage_resource_id='121eb709-b2e6-4d85-a76e-91fcb422654e',
    )
}


def init_session(props: TransferProps) -> None:
    service_account_credentials = service_account.Credentials.from_service_account_info(
        props.google_service_account_credentials,
        scopes=[
            'https://www.googleapis.com/auth/userinfo.profile',
            'https://www.googleapis.com/auth/userinfo.email'
        ]
    )
    props.session = AuthorizedSession(service_account_credentials)


def get_sas_token(terra_api_url, session, workspace_id, resource_id, expiration=28800) -> dict[str, str]:
    path = f'{terra_api_url}/api/workspaces/v1/{workspace_id}/resources/controlled/azure/storageContainer/{resource_id}/getSasToken?sasExpirationDuration={expiration}'
    return session.post(path).json()


def get_source_sas_token(props: TransferProps) -> dict[str, str]:
    return get_sas_token(
        props.terra_api_url,
        props.session,
        props.context.source_workspace_id,
        props.context.source_storage_resource_id,
    )


def get_destination_sas_token(props: TransferProps) -> dict[str, str]:
    return get_sas_token(
        props.terra_api_url,
        props.session,
        props.context.destination_workspace_id,
        props.context.destination_storage_resource_id,
    )


def init_sas_tokens(props: TransferProps) -> None:
    props.source_sas_token = get_source_sas_token(props)['token']
    props.destination_sas_token = get_destination_sas_token(props)['token']


def portal_is_indexing(props: TransferProps) -> bool:
    return requests.get(f'{props.context.portal_api_url}/indexer-info').json()['is_indexing']


def get_files_to_transfer(props: TransferProps) -> list[File]:
    path = f'{props.context.portal_api_url}/search/?type=File&audit.INTERNAL_ACTION.category=incorrect+anvil+workspace&field=@id&field=anvil_source_url&field=anvil_destination_url&field=upload_status&limit=all'
    files = requests.get(path, auth=props.portal_auth).json()['@graph']
    print(f'Got {len(files)} files to transfer')
    return [
        File(
            id_=item['@id'],
            anvil_source_url=item['anvil_source_url'],
            anvil_destination_url=item['anvil_destination_url'],
            upload_status=item['upload_status']
        )
        for item in files
    ]


def copy_file_from_source_to_destination(props, file_):
    print(f'Copying {file_.id_}')
    destination_blob = BlobClient.from_blob_url(
        f'{file_.anvil_destination_url}?{props.destination_sas_token}'
    )
    response = destination_blob.start_copy_from_url(
        f'{file_.anvil_source_url}?{props.source_sas_token}',
        requires_sync=True,
    )
    if response['copy_status'] != 'success':
        raise ValueError(f'Copying file {file_.id_} failed: {response}')


def file_exists(file_url_with_sas: str) -> bool:
    blob_client = BlobClient.from_blob_url(file_url_with_sas)
    return blob_client.exists()


def patch_upload_status_deposited(props: TransferProps, file_: File) -> None:
    response = requests.patch(f'{props.context.portal_api_url}{file_.id_}', json={'upload_status': 'deposited'}, auth=props.portal_auth)
    if response.status_code != 200:
        raise ValueError(f'Error patching upload_status: deposited on file {file_.id_} {response.json()}')


def delete_file(file_url_with_sas: str) -> None:
    blob_client = BlobClient.from_blob_url(file_url_with_sas)
    blob_client.delete_blob()


def maybe_delete_source_file(props: TransferProps, file_: File) -> None:
    if not props.delete_source_files:
        print('Not deleting source file')
        return
    print('Deleting source file {file_._id} {file_.anvil_source_url}')
    delete_file(f'{file_.anvil_source_url}?{props.source_sas_token}')


def transfer_files(props: TransferProps):
    files = get_files_to_transfer(props)
    for file_ in files:
        if not file_exists(f'{file_.anvil_source_url}?{props.source_sas_token}'):
            print(f'File {file_.id_} source URL {file_.anvil_source_url} does not exist. Has it been submitted yet? Skipping!')
            continue
        copy_file_from_source_to_destination(props, file_)
        patch_upload_status_deposited(props, file_)
        maybe_delete_source_file(props, file_)


def transfer(props: TransferProps):
    if portal_is_indexing(props):
        print('Portal is indexing, will try again later.')
        return
    else:
        print('Portal not indexing')
    transfer_files(props)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', type=str, choices=['sandbox', 'prod'], required=True, help='Specify the environment: prod or sandbox.')
    parser.add_argument('--google-service-account-credentials-base64', required=True, type=str, help='Google service account credentials linked to Terra workspace, base64 encoded')
    parser.add_argument('--portal-key', required=True, type=str, help='Portal key associated with Anvil Transfer service account user')
    parser.add_argument('--portal-secret-key', required=True, type=str, help='Portal secret key associated with Anvil Transfer service account user')
    parser.add_argument('--delete-source-files', action='store_true', help='Delete source files after copying')
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    props = TransferProps(
        google_service_account_credentials=json.loads(base64.b64decode(args.google_service_account_credentials_base64).decode('utf-8')),
        env=args.env,
        context=ENVIRONMENT[args.env],
        portal_auth=(args.portal_key, args.portal_secret_key),
        delete_source_files=args.delete_source_files,
    )
    init_session(props)
    init_sas_tokens(props)
    transfer(props)
