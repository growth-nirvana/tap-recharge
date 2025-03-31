""
"""
This module defines the stream classes and their individual sync logic.
"""

import datetime
from typing import Iterator

import singer
from singer import Transformer, utils, metrics, bookmarks

from tap_recharge.client import RechargeClient

LOGGER = singer.get_logger()

MAX_PAGE_LIMIT = 100


def get_recharge_bookmark(state: dict, tap_stream_id: str, default: str = None) -> str:
    return state.get('bookmarks', {}).get(tap_stream_id, default)


def write_recharge_bookmark(state: dict, tap_stream_id: str, value: str) -> dict:
    state = bookmarks.ensure_bookmark_path(state, ['bookmarks'])
    state['bookmarks'][tap_stream_id] = value
    return state


class BaseStream:
    tap_stream_id = None
    replication_method = None
    replication_key = None
    key_properties = []
    valid_replication_keys = []
    path = None
    params = {}
    parent = None
    data_key = None

    def __init__(self, client: RechargeClient):
        self.client = client

    def get_records(self, bookmark_datetime: datetime = None, is_parent: bool = False) -> list:
        raise NotImplementedError("Child classes of BaseStream require `get_records` implementation")

    def get_parent_data(self, bookmark_datetime: datetime = None) -> list:
        parent = self.parent(self.client)
        return parent.get_records(bookmark_datetime, is_parent=True)


class IncrementalStream(BaseStream):
    replication_method = 'INCREMENTAL'

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict,
             config: dict, transformer: Transformer) -> dict:
        start_date = get_recharge_bookmark(state, self.tap_stream_id, config['start_date'])
        bookmark_datetime = utils.strptime_to_utc(start_date)
        max_datetime = bookmark_datetime

        BATCH_SIZE = 100
        buffer = []

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config, bookmark_datetime):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                replication_value = transformed_record.get(self.replication_key)

                should_write = True
                if replication_value:
                    record_datetime = utils.strptime_to_utc(replication_value)
                    should_write = record_datetime >= bookmark_datetime
                    if should_write:
                        max_datetime = max(record_datetime, max_datetime)

                if should_write:
                    buffer.append(transformed_record)
                    counter.increment()

                if len(buffer) >= BATCH_SIZE:
                    for rec in buffer:
                        singer.write_record(self.tap_stream_id, rec)
                    buffer.clear()

            for rec in buffer:
                singer.write_record(self.tap_stream_id, rec)

        bookmark_date = utils.strftime(max_datetime)
        state = write_recharge_bookmark(state, self.tap_stream_id, bookmark_date)
        singer.write_state(state)
        return state


class FullTableStream(BaseStream):
    replication_method = 'FULL_TABLE'

    def sync(self, state: dict, stream_schema: dict, stream_metadata: dict,
             config: dict, transformer: Transformer) -> dict:
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(config):
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_record)
                counter.increment()
        singer.write_state(state)
        return state


class PageBasedPagingStream(IncrementalStream):
    def get_records(self, bookmark_datetime: datetime = None, is_parent: bool = False) -> Iterator[list]:
        page = 1
        self.params.update({'limit': MAX_PAGE_LIMIT, 'page': page})
        result_size = MAX_PAGE_LIMIT

        while result_size == MAX_PAGE_LIMIT:
            records = self.client.get(self.path, params=self.params)
            result_size = len(records.get(self.data_key))
            page += 1
            self.params.update({'page': page})
            yield from records.get(self.data_key)


class CursorPagingStream(IncrementalStream):
    def get_records(self, bookmark_datetime: datetime = None, is_parent: bool = False) -> Iterator[list]:
        self.params.update({'limit': MAX_PAGE_LIMIT})
        paging = True
        path = self.path
        url = None

        while paging:
            records = self.client.get(path, url=url, params=self.params)
            records_data = records.get(self.data_key)
            if records_data:
                yield from records_data
            next_cursor = records.get('next_cursor')
            if next_cursor:
                self.params = {'cursor': next_cursor, 'limit': MAX_PAGE_LIMIT}
            else:
                paging = False


class Addresses(CursorPagingStream):
    tap_stream_id = 'addresses'
    key_properties = ['id']
    path = 'addresses'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'addresses'


class Charges(CursorPagingStream):
    tap_stream_id = 'charges'
    key_properties = ['id']
    path = 'charges'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'charges'


class Collections(CursorPagingStream):
    tap_stream_id = 'collections'
    key_properties = ['id']
    path = 'collections'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'collections'


class Customers(CursorPagingStream):
    tap_stream_id = 'customers'
    key_properties = ['id']
    path = 'customers'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'customers'


class Discounts(CursorPagingStream):
    tap_stream_id = 'discounts'
    key_properties = ['id']
    path = 'discounts'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'discounts'


class MetafieldsStore(CursorPagingStream):
    tap_stream_id = 'metafields_store'
    key_properties = ['id']
    path = 'metafields'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc', 'owner_resource': 'store'}
    data_key = 'metafields'


class MetafieldsCustomer(CursorPagingStream):
    tap_stream_id = 'metafields_customer'
    key_properties = ['id']
    path = 'metafields'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc', 'owner_resource': 'customer'}
    data_key = 'metafields'


class MetafieldsSubscription(CursorPagingStream):
    tap_stream_id = 'metafields_subscription'
    key_properties = ['id']
    path = 'metafields'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc', 'owner_resource': 'subscription'}
    data_key = 'metafields'


class Onetimes(PageBasedPagingStream):
    tap_stream_id = 'onetimes'
    key_properties = ['id']
    path = 'onetimes'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'onetimes'


class Orders(CursorPagingStream):
    tap_stream_id = 'orders'
    key_properties = ['id']
    path = 'orders'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'orders'


class Products(CursorPagingStream):
    tap_stream_id = 'products'
    key_properties = ['id']
    path = 'products'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'products'


class Store(FullTableStream):
    tap_stream_id = 'store'
    key_properties = ['id']
    path = 'store'
    data_key = 'store'

    def get_records(self, bookmark_datetime: datetime = None, is_parent: bool = False) -> Iterator[list]:
        records = self.client.get(self.path)
        return [records.get(self.data_key)]


class Subscriptions(CursorPagingStream):
    tap_stream_id = 'subscriptions'
    key_properties = ['id']
    path = 'subscriptions'
    replication_key = 'updated_at'
    valid_replication_keys = ['updated_at']
    params = {'sort_by': f'{replication_key}-asc'}
    data_key = 'subscriptions'


STREAMS = {
    'addresses': Addresses,
    'charges': Charges,
    'collections': Collections,
    'customers': Customers,
    'discounts': Discounts,
    'metafields_store': MetafieldsStore,
    'metafields_customer': MetafieldsCustomer,
    'metafields_subscription': MetafieldsSubscription,
    'onetimes': Onetimes,
    'orders': Orders,
    'products': Products,
    'store': Store,
    'subscriptions': Subscriptions
}
