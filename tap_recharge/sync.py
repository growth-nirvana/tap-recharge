import singer
from singer import Transformer, Catalog, metadata

from tap_recharge.client import RechargeClient
from tap_recharge.streams import STREAMS

LOGGER = singer.get_logger()

def sync(client: RechargeClient, config: dict, state: dict, catalog: Catalog) -> dict:
    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client)
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info('Syncing stream: %s', tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key
            )

            state = stream_obj.sync(
                state,
                stream_schema,
                stream_metadata,
                config,
                transformer)

        state = singer.set_currently_syncing(state, None)
        singer.write_state(state)

    return state
