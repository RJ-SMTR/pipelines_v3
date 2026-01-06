# -*- coding: utf-8 -*-
import requests


def send_discord_message(
    message: str,
    webhook_url: str,
) -> None:
    """
    Sends a message to a Discord channel.
    """
    requests.post(
        webhook_url,
        data={"content": message},
    )


def format_send_discord_message(formatted_messages: list, webhook_url: str):
    """
    Format and send a message to discord

    Args:
        formatted_messages (list): The formatted messages
        webhook_url (str): The webhook url

    Returns:
        None
    """
    formatted_message = "".join(formatted_messages)
    print(formatted_message)
    msg_ext = len(formatted_message)
    max_len = 2000
    if msg_ext > max_len:
        print(
            f"** Message too long ({msg_ext} characters), will be split into multiple messages **"
        )
        # Split message into lines
        lines = formatted_message.split("\n")
        message_chunks = []
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > max_len:  # +1 for the newline character
                message_chunks.append(chunk)
                chunk = ""
            chunk += line + "\n"
        message_chunks.append(chunk)  # Append the last chunk
        for chunk in message_chunks:
            send_discord_message(
                message=chunk,
                webhook_url=webhook_url,
            )
    else:
        send_discord_message(
            message=formatted_message,
            webhook_url=webhook_url,
        )
