import logging
from datetime import datetime
from typing import Any

from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class IoTData(SQLModel, table=True):
    time: datetime = Field(primary_key=True)
    device_id: str = Field(primary_key=True)
    room: str = Field(index=True)
    device_type: str = Field(index=True)
    device_name: str = Field(index=True)
    topic: str
    payload: dict[str, Any] = Field(sa_type=JSONB)


def transform_iot_message(topic: str, payload: dict[str, Any], logger: logging.Logger) -> IoTData | None:
    parts = topic.strip("/").split("/")
    if len(parts) != 4:
        logger.warning("Topic not matching expected length: %s", topic)
        return None
    try:
        iot_data = IoTData(
            time=datetime.now(),
            device_id=topic,
            room=parts[1],
            device_type=parts[2],
            device_name=parts[3],
            topic=topic,
            payload=payload,
        )
        IoTData.model_validate(iot_data.model_dump())
        return iot_data

    except Exception as e:
        logger.error("Error storing message for topic %s: %s", topic, str(e))
    return None
