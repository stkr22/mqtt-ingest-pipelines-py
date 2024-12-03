import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

import aiomqtt
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession


class MQTTDataPipeline:
    def __init__(
        self,
        mqtt_client: aiomqtt.Client,
        db_engine: AsyncEngine,
        task_group: asyncio.TaskGroup,
        logger: logging.Logger,
    ):
        self.db_engine = db_engine
        self.logger = logger
        self.mqtt_client: aiomqtt.Client = mqtt_client
        self.task_group: asyncio.TaskGroup = task_group
        self.topic_to_transformer: dict[str, Callable[[str, dict[str, Any], logging.Logger], SQLModel | None]] = {}

    def register_transformer(
        self, topic: str, transformer: Callable[[str, dict[str, Any], logging.Logger], SQLModel | None]
    ) -> None:
        """Register a transformer function for a specific MQTT topic.

        Args:
            topic: The MQTT topic pattern to associate with the transformer
            transformer: A function that takes (topic, payload, logger) and returns an SQLModel
        """
        self.topic_to_transformer[topic] = transformer
        self.logger.info("Registered transformer for topic pattern: %s", topic)

    def get_transformer_for_topic(
        self, topic: aiomqtt.Topic
    ) -> Callable[[str, dict[str, Any], logging.Logger], SQLModel | None] | None:
        """Get the most specific transformer for a given topic using aiomqtt's topic matching.

        Args:
            topic: The aiomqtt.Topic object to find a transformer for
        """
        matching_transformers = [
            (registered_topic, transformer)
            for registered_topic, transformer in self.topic_to_transformer.items()
            if topic.matches(registered_topic)
        ]

        if not matching_transformers:
            return None

        # Return the most specific matching pattern
        return max(matching_transformers, key=lambda x: len(x[0]) - x[0].count("+") - x[0].count("#") * 10)[1]

    async def store_message(self, topic: aiomqtt.Topic, payload: dict[str, Any]) -> None:
        """Store a message using the appropriate transformer for its topic."""
        transformer = self.get_transformer_for_topic(topic)

        if transformer is None:
            self.logger.warning("No transformer registered for topic: %s", topic)
            return

        try:
            # Transform the data using the registered transformer
            model_instance = transformer(str(topic), payload, self.logger)

            if model_instance:
                # Store the transformed data
                async with AsyncSession(self.db_engine) as session:
                    session.add(model_instance)
                    await session.commit()

                self.logger.debug("Stored transformed message for topic: %s", topic)
            else:
                self.logger.warning("Error transforming message for topic %s", topic)

        except Exception as e:
            self.logger.error("Error storing message for topic %s: %s", topic, str(e))

    async def setup_mqtt_subscriptions(self) -> None:
        """Set up MQTT topic subscriptions based on registered transformers."""
        for topic in self.topic_to_transformer:
            await self.mqtt_client.subscribe(topic=topic, qos=1)
            self.logger.info("Subscribed to topic: %s", topic)

    async def listen_to_messages(self, client: aiomqtt.Client) -> None:
        """Listen for incoming MQTT messages and handle them appropriately."""
        async for message in client.messages:
            self.logger.debug("Received message on topic %s", message.topic)

            payload_str = self.decode_message_payload(message.payload)
            if payload_str is not None:
                try:
                    payload = json.loads(payload_str)
                    self.task_group.create_task(self.store_message(message.topic, payload))

                except json.JSONDecodeError:
                    self.logger.warning("Failed to parse message payload for topic %s: %s", message.topic, payload_str)
                except Exception as e:
                    self.logger.error("Error processing message for topic %s: %s", message.topic, str(e))

    def decode_message_payload(self, payload: Any) -> str | None:
        """Decode the message payload if it is a suitable type."""
        if isinstance(payload, (bytes | bytearray)):
            return payload.decode("utf-8")
        if isinstance(payload, str):
            return payload
        self.logger.warning("Unexpected payload type: %s", type(payload))
        return None
