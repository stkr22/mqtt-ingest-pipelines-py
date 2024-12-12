import logging
import uuid
from datetime import datetime

import pytest

from mqtt_ingest_pipeline.assistant_data_transformer import CommsBridgeData, transform_comms_bridge_message


@pytest.fixture
def logger():
    return logging.getLogger("test")


def test_valid_comms_bridge_message_transformation(logger):
    # Given
    message_id = uuid.uuid4()
    topic = "/assistant/home/livingroom/message/response"
    payload = {"id": message_id, "room": "livingroom", "content": "Test message"}

    # When
    result = transform_comms_bridge_message(topic, payload, logger)

    # Then
    assert isinstance(result, CommsBridgeData)
    assert result.message_id == message_id
    assert result.room == "livingroom"
    assert result.topic == topic
    assert result.payload == payload
    assert isinstance(result.time, datetime)


def test_invalid_topic_length(logger):
    # Given
    topic = "/assistant/home/livingroom/message"  # Missing last part
    payload = {"id": uuid.uuid4(), "room": "livingroom", "content": "Test message"}

    # When
    result = transform_comms_bridge_message(topic, payload, logger)

    # Then
    assert result is None


def test_valid_string_uuid(logger):
    # Given
    message_id = str(uuid.uuid4())
    topic = "/assistant/home/livingroom/message/response"
    payload = {"id": message_id, "room": "livingroom", "content": "Test message"}

    # When
    result = transform_comms_bridge_message(topic, payload, logger)

    # Then
    assert isinstance(result, CommsBridgeData)
    assert str(result.message_id) == message_id


def test_invalid_uuid_format(logger):
    # Given
    topic = "/assistant/home/livingroom/message/response"
    payload = {"id": "not-a-uuid", "room": "livingroom", "content": "Test message"}

    # When
    result = transform_comms_bridge_message(topic, payload, logger)

    # Then
    assert result is None


def test_none_uuid(logger):
    # Given
    topic = "/assistant/home/livingroom/message/response"
    payload = {"id": None, "room": "livingroom", "content": "Test message"}

    # When
    result = transform_comms_bridge_message(topic, payload, logger)

    # Then
    assert result is None
