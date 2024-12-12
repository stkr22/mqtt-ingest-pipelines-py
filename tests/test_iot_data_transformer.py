import logging
from datetime import datetime

import pytest

from mqtt_ingest_pipeline.iot_data_transformer import IoTData, transform_iot_message


@pytest.fixture
def logger():
    return logging.getLogger("test")


def test_valid_iot_message_transformation(logger):
    # Given
    topic = "/home/livingroom/temperature/sensor1"
    payload = {"temperature": 21.5, "humidity": 45}

    # When
    result = transform_iot_message(topic, payload, logger)

    # Then
    assert isinstance(result, IoTData)
    assert result.room == "livingroom"
    assert result.device_type == "temperature"
    assert result.device_name == "sensor1"
    assert result.topic == topic
    assert result.payload == payload
    assert isinstance(result.time, datetime)


def test_invalid_topic_length(logger):
    # Given
    topic = "/home/livingroom/temperature"  # Missing device name
    payload = {"temperature": 21.5}

    # When
    result = transform_iot_message(topic, payload, logger)

    # Then
    assert result is None


def test_empty_topic(logger):
    # Given
    topic = ""
    payload = {"temperature": 21.5}

    # When
    result = transform_iot_message(topic, payload, logger)

    # Then
    assert result is None
