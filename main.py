import json
import logging
from pprint import pformat

from quixstreams import Application
from requests_sse import EventSource


KAFKA_BROKER = "htpp://localhost:19092"
GITHUB_FIREHOSE = "https://github-firehose.libraries.io/events"


def main():
    logging.info("STARTING main function")
    app = Application(
        broker_address=KAFKA_BROKER,
        loglevel="DEBUG"
    )

    with (
        app.get_producer() as producer,
        EventSource(url=GITHUB_FIREHOSE, timeout=30) as event_source
    ):
        for event in event_source:
            value = json.loads(event.data)
            key = value["id"]
            logging.info(f"Got {pformat(value)}")


if __name__ == "__main__":
    try:
        logging.basicConfig(level="DEBUG")
        main()
    except KeyboardInterrupt:
        pass
