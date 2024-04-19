import json
import logging
import pathlib

from filip.clients.ngsi_v2.cb import ContextBrokerClient
from filip.models.base import FiwareHeader
from filip.models.ngsi_v2.subscriptions import \
    EntityPattern, \
    Mqtt, \
    Notification, \
    Subject, \
    Subscription


logger = logging.getLogger(__name__)


def create_subscriptions(
        mqtt_url: str,
        cb_url: str,
        fiware_header: dict,
        save_path: pathlib.Path = None
):
    fiware_header = FiwareHeader(**fiware_header)
    telegraf_mqtt_list = []
    with ContextBrokerClient(
            url=cb_url,
            fiware_header=fiware_header) as _http_client:
        entities = _http_client.get_entity_list()
    subscription_ids = []

    for entity in entities:
        entity_pattern = EntityPattern(**entity.model_dump())
        attrs = entity.get_attributes()
        for attr in attrs:
            telegraf_mqtt_list.append(f"{attr.name}_value")
        # Post new subscription
        topic = "/".join([
            "/fiware_to_influx",
            fiware_header.service.strip('/'),
            fiware_header.service_path.strip('/'),
            entity.id
        ])
        sub = Subscription(
            description="Subscription for fiware to influx-db management",
            subject=Subject(
                entities=[entity_pattern]
            ),
            notification=Notification(
                mqtt=Mqtt(url=mqtt_url,
                          topic=topic)
            ),
            throttling=1
        )
        logger.info("Posting subscription to topic '%s' with sub'=%s'",
                    topic, sub.model_dump_json())
        subscription_ids.append(
            _http_client.post_subscription(
                subscription=sub, update=True
            )
        )

    included_keys = json.dumps(list(set(telegraf_mqtt_list)), indent=2)

    with open(pathlib.Path(__file__).parent.joinpath("telegraf_conf.template"), "r") as file:
        conf_telegraf = file.read()
    conf_telegraf = conf_telegraf.replace(
        "included_keys = []",
        f"included_keys = {included_keys}"
    )
    logger.info("Your telegraf config:")
    logger.info(conf_telegraf)
    if save_path is not None:
        with open(save_path, "w+") as file:
            file.write(conf_telegraf)
        return save_path


if __name__ == '__main__':
    logging.basicConfig(level="INFO")
    create_subscriptions(
        mqtt_url=f"mqtt://134.130.56.157:1883",
        cb_url=f"http://134.130.56.157:1026",
        fiware_header={
            "service_path": "/HiL2",
            "service": "hil2"
        }
    )
