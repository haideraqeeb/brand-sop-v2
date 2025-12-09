import os
import logging
from typing import Optional
import smtplib
import ssl
from email.message import EmailMessage


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")


def send_email_with_attachment(
    to_email: str,
    subject: str,
    body: str,
    attachment_path: str,
    attachment_filename: Optional[str] = None,
) -> None:
    """
    Send an email with a single attachment using Gmail SMTP
    and an app password for a personal @gmail.com account.
    """
    if not to_email:
        raise ValueError("Recipient email address is required.")

    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        raise RuntimeError("GMAIL_ADDRESS or GMAIL_APP_PASSWORD is not set in environment.")

    if not os.path.exists(attachment_path):
        raise FileNotFoundError(f"Attachment not found: {attachment_path}")

    msg = EmailMessage()
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    filename = attachment_filename or os.path.basename(attachment_path)
    with open(attachment_path, "rb") as f:
        data = f.read()

    msg.add_attachment(
        data,
        maintype="application",
        subtype="octet-stream",
        filename=filename,
    )

    context = ssl.create_default_context()

    logger.info("Sending email with attachment to %s from %s", to_email, GMAIL_ADDRESS)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        server.send_message(msg)

    logger.info("Email sent successfully.")
