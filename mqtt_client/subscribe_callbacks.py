import subprocess

from loguru import logger


def default_subscribe_callback(mqttc, obj, msg):
    try:
        logger.info(f"Topic: {msg.topic}   Payload: {msg.payload}")
    except Exception as ex:
        logger.error(f"Error: {ex}")


def subscribe_callback_raw(mqttc, obj, msg):
    try:
        logger.info(msg.payload.decode("utf8"))
    except Exception as ex:
        logger.error(f"Error: {ex}")


def subscribe_callback_command(command):
    command = [command]

    def _(mqttc, obj, msg):
        command.append(msg.topic)
        command.append(msg.payload.decode("utf8"))
        logger.info(f"[COMMAND] {command}")

        response = subprocess.run(command, stdout=subprocess.PIPE)
        if response.stdout:
            logger.info(response.stdout)
        if response.stderr:
            logger.error(response.stderr)
        command.pop(1)
        command.pop(1)

    return _
