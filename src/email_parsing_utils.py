import csv
import email
import logging
import mailbox
import mimetypes
import os
import re

from datetime import timezone
from email.utils import parsedate_to_datetime
from email.policy import default

logger = logging.getLogger(__name__)
SUPPORTED_EXTENSIONS = ["eml", "mbox"]

from openrelik_worker_common.file_utils import create_output_file


# Utility functions for email parsing tasks
def santitize_filename(filename):
    """
    Sanitizes a filename by removing invalid characters.

    Args:
        filename (str): The original filename.

    Returns:
        str: A sanitized version of the filename.
    """
    # Replace invalid characters with underscores
    return re.sub(r'[<>:"/\\|?*]', '_', filename)


def convert_timestamp_to_utc(date_str):
    """
    Converts a date string to UTC datetime.

    Args:
        date_str (str): The date string from the email header.

    Returns:
        str: The UTC datetime as an ISO 8601 string, or None if
          conversion fails.
    """
    if not date_str:
        return None
    try:
        date = parsedate_to_datetime(date_str)
        if date and date.tzinfo:
            return date.astimezone(timezone.utc).isoformat()
        return date.isoformat()
    except Exception:
        return None


def extract_message_metadata(attachments, message):
    """
    Extracts metadata from an email message.

    Args:
        attachments (list): List of attachment filenames.
        message (object): The email message object.

    Returns:
        dict: A dictionary containing metadata for the email.
    """
    return {
        # Required Timesketch columns. timestamp, timestamp_desc,
        # message
        "Timestamp": convert_timestamp_to_utc(message.get("Date")),
        "Timestamp_desc": "Email received",
        "Message": "Email message",
        "To": message.get("To"),
        "From": message.get("From"),
        "Bcc": message.get("Bcc"),
        "Cc": message.get("Cc"),
        "Subject": message.get("Subject"),
        "Message-ID": message.get("Message-ID", ""),
        "Date": message.get("Date"),
        "Content-Type": message.get_content_type(),
        "Attachments": attachments,
        "User-Agent": message.get("User-Agent"),
        "Body": get_message_body(message),
    }


def get_message_body(message):
    """
    Extracts the body of an email message.

    Args:
        message (object): The email message object.

    Returns:
        str: The plain text body of the email.
    """
    if message.is_multipart():
        for part in message.walk():
            if part.get_content_type() == "text/plain":
                return part.get_payload(decode=True).decode(
                    part.get_content_charset() or "utf-8",
                    errors="replace"
                )
    else:
        return message.get_payload(decode=True).decode(
            message.get_content_charset() or "utf-8",
            errors="replace"
        )
    return ""


def write_dict_to_csv(message_dict, headers, output_file):
    """
    Writes out a dictionary to a CSV file.

    Args:
        headers (list): List of headers for the CSV file.
        message_dict (dict): Dict of parsed message data
        output_file (str): Path to the output CSV file.

    Returns:
        str: Path to the output CSV file.
    """
    with open(
        output_file.path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(message_dict)

    return output_file


def parse_eml_to_dict_and_extract_attachments(file_path, output_path):
    """
    Parses an EML file and generates a metadata dict for the message.

    Args:
        file_path (str): Path to the EML file.

    Returns:
        dict: A dictionary containing metadata for the email.
    """
    attachment_file_paths = []

    with open(file_path, "r", encoding="utf-8") as eml_file:
        msg = email.message_from_file(eml_file, policy=default)
        message_attachments = extract_message_attachments(
            message=msg,
            output_path=output_path
        )
        attachment_file_paths.extend(message_attachments)
        attachments = [
            part.get_filename()
            for part in msg.walk()
            if part.get_filename()
        ]

        email_metadata = extract_message_metadata(
            attachments=attachments,
            message=msg
        )

    return attachment_file_paths, email_metadata


def extract_message_attachments(message, output_path):
    """Extract attachments and inline content from an email message.

    Args:
        message (object): The email message object.
        output_path (str): The path to the output directory where
            attachments will be saved.
    """
    extracted_files = []

    if (
        isinstance(message, email.message.EmailMessage)
        or isinstance(message, mailbox.mboxMessage)
    ):
        message_id = santitize_filename(filename=message.get("Message-ID", ""))
        if not message_id:
            message_id = "unknown_message_" + str(abs(hash(str(message))))[:8]

        for part in message.walk():
            # Skip the main message container and alternative text/html
            # parts that don't have a filename
            if part.is_multipart():
                continue

            filename = part.get_filename()
            if filename:
                # Get the content disposition header and content type
                disposition = part.get("Content-Disposition")
                maintype = part.get_content_maintype()

                # Grab inline and attached files
                if (disposition and disposition.strip().lower().startswith(("attachment", "inline"))) or \
                   (not disposition and maintype != 'text' and filename):

                    base, ext = os.path.splitext(filename)
                    processed_base = base # 'filename_base'
                    processed_ext = ext[1:] # 'png'
                    # Add the message ID to the display name for uniqueness
                    attachment_file_display_name = f"{processed_base}.{message_id}"

                    try:
                        output_file = create_output_file(
                            output_path,
                            display_name=attachment_file_display_name,
                            extension=processed_ext,
                            data_type=processed_ext,
                        )
                        with open(output_file.path, 'wb') as out_f:
                            out_f.write(part.get_payload(decode=True))
                        logger.info(f"Saved file: {output_file.path} (Disposition: {disposition or 'implicit'})")

                        extracted_files.append(output_file.to_dict())

                    except Exception as e:
                        logger.error(f"Failed to save file {filename}: {e}")
                else:
                    logger.debug(f"Skipping part with filename '{filename}' (Disposition: {disposition}, Maintype: {maintype}).")
            else:
                pass 
    else:
        logger.warning("Unsupported message type for attachment extraction.")

    return extracted_files


def parse_mbox_to_dict_and_extract_attachments(file_path, output_path):
    """
    Parses an MBOX file and generate a metadata dict for each message. 
    Extract attachments and save them to the specified output path.

    Args:
        file_path (str): Path to the MBOX file.
        output_path (str): Path to the output directory for artifacts.

    Returns:
        list: A list of dictionaries containing metadata for each email.
        list: A list of output file dicts for the attachments.
    """
    attachment_file_paths = []
    messages_metadata = []

    try:
        mbox = mailbox.mbox(file_path)
        logger.info(f"Opened MBOX file: {file_path}")

        for message in mbox:
            attachment_filenames = []
            attachment_file_paths = []

            # Handle attachments and inline content while iterating
            # through messages.
            for part in message.walk():
                filename = part.get_filename()
                if filename:
                    message_content = extract_message_attachments(
                        message=message,
                        output_path=output_path
                    )
                    attachment_file_paths.extend(message_content)

            messages_metadata.append(
                extract_message_metadata(message=message,
                                         attachments=attachment_filenames)
            )

    except mailbox.NoSuchMailboxError:
        logger.error(f"Error: '{file_path}' is not a valid Mbox file format.")
    except Exception as e:
        logger.warning(
            f"An unexpected error occurred while processing the Mbox file: {e}")

    return attachment_file_paths, messages_metadata

