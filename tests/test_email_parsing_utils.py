from email.message import EmailMessage
from email.policy import default
from unittest.mock import MagicMock

import csv
import email
import mailbox
import os
import pytest
import sys
## Mock openrelik_worker_common
#sys.modules["openrelik_worker_common"] = MagicMock()
#sys.modules["openrelik_worker_common.file_utils"] = MagicMock()

from unittest.mock import MagicMock, patch

from src.email_parsing_utils import (
    santitize_filename, convert_timestamp_to_utc,
    create_output_file, get_message_body,
    extract_message_metadata, extract_message_attachments,
    parse_eml_to_dict_and_extract_attachments,
    parse_mbox_to_dict_and_extract_attachments,
    write_dict_to_csv)


# Message test metadata
MSG_TEST_METADATA= {
    # EML
    'attachment_message.eml': {
        'Subject': 'Important: Updated Company Policy Document',
        'From': 'HR Department <hr@example.com>',
        'To': 'Employee <employee@example.com>',
        'Date': 'Wed, 23 Oct 2024 10:34:04 +0300',
        'Message-ID': '<test-message-1234@example.com>',
        'Attachments': ['Company Policy Guidelines.txt'],
    },
    'inline_content_message.eml': {
        'Subject': 'Test Email with Inline Image',
        'From': 'Sender <sender@example.com>',
        'To': 'Recipient <recipient@example.com>',
        'Date': 'Wed, 23 Oct 2024 10:34:04 +0300',
        'Message-ID': '<test-message-1234@example.com>',
        'Attachments': ['myimage.png'],
    },
    'simple_message.eml': {
        'Subject': 'Test Email for EML Parsing',
        'From': 'Alice Example <alice@example.com>',
        'To': 'Bob Recipient <bob@example.com>',
        'Date': 'Tue, 17 Jun 2025 14:30:00 +0200',
        'Message-ID': '<test-message-1234@example.com>',
        'Attachments': [],
        'Body': 'This is a test email.\n',
    },

    # MBOX
    'attachment_message.mbox': {
        'Subject': 'Email with Attachment 1',
        'From': 'sender3@example.com',
        'To': 'recipient3@example.com',
        'Date': 'Thu, 1 Jan 2024 00:00:03 +0000',
        'Message-ID': '<msg3@example.com>',
        'Attachments': ['attachment1.txt'],
    },
    'inline_content_message.mbox': {
        'Subject': 'Holiday Greetings with Inline Image',
        'From': 'sender10@example.com',
        'To': 'recipient10@example.com',
        'Date': 'Thu, 1 Jan 2024 00:00:10 +0000',
        'Message-ID': '<msg10@example.com>',
        'Attachments': ['my_inline_image.png'],
    },
    'simple_message.mbox': {
        'Subject': 'Plain Text Email 1',
        'From': 'sender1@example.com',
        'To': 'recipient1@example.com',
        'Date': 'Thu, 1 Jan 2024 00:00:01 +0000',
        'Message-ID': '<msg1@example.com>',
        'Attachments': [],
    },
}
MSG_TEST_DATA_FILENAMES = list(MSG_TEST_METADATA.keys())


# Testing utility functions
def make_plain_email(body, charset="utf-8"):
    """Create a plain text EmailMessage."""
    msg = EmailMessage()
    msg.set_content(body, charset=charset)
    return msg


def make_multipart_email(text_body, html_body=None, charset="utf-8"):
    """Create a multipart EmailMessage with both text and HTML parts."""
    msg = EmailMessage()
    msg.set_content(text_body, charset=charset)
    if html_body:
        msg.add_alternative(html_body, subtype="html", charset=charset)
    return msg


def return_msg_from_file(file_path):
    """
    Return specified file as an EmailMessage object.
    """
    if file_path.endswith('.eml'):
        with open(file_path, "r", encoding="utf-8") as eml_file:
            return email.message_from_file(eml_file, policy=default)

    elif file_path.endswith('.mbox'):
        mbox = mailbox.mbox(file_path)
        for message in mbox:
            return message
        return None
    else:
        return None


# Utility functions for email parsing tasks
@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("normalfile.txt", "normalfile.txt"),
        ("file<name>.txt", "file_name_.txt"),
        ("file:name?.txt", "file_name_.txt"),
        ("file|with*bad/chars.txt", "file_with_bad_chars.txt"),
        ("file\"quote\".txt", "file_quote_.txt"),
        ("file\\slash/.txt", "file_slash_.txt"),
        ("<>:\"/\\|?*", "_________"),
        ("", ""),
        ("file.name.without.bad.chars", "file.name.without.bad.chars"),
    ]
)
def test_santitize_filename(input_str, expected):
    """Test that santitize_filename returns the expected sanitized filename."""
    assert santitize_filename(input_str) == expected

@pytest.mark.parametrize(
    "input_str,expected_prefix",
    [
        # with timezone
        ("Mon, 15 Jul 2024 10:30:00 -0400", "2024-07-15T14:30:00+00:00"),
        # UTC
        ("Mon, 15 Jul 2024 14:30:00 +0000", "2024-07-15T14:30:00+00:00"),
        # without timezone
        ("Mon, 15 Jul 2024 14:30:00", "2024-07-15T14:30:00"),
        # Empty string
        ("", None),
        # None input
        (None, None),
        # Invalid date
        ("not a date", None),
    ]
)
def test_convert_timestamp_to_utc(input_str, expected_prefix):
    """Test that convert_timestamp_to_utc returns the expected UTC timestamp."""
    result = convert_timestamp_to_utc(input_str)
    if expected_prefix is None:
        assert result is None
    else:
        assert result is not None, f"Expected a string starting with {expected_prefix}, got None"
        assert result.startswith(expected_prefix)

@pytest.mark.parametrize(
    "msg,expected",
    [
        (make_plain_email("Hello world!"), "Hello world!\n"),
        (make_plain_email("Привет мир!", charset="utf-8"), "Привет мир!\n"),
        (make_multipart_email("Plain part", "<b>HTML part</b>"), "Plain part\n"),
        (make_multipart_email(""), "\n"),
    ]
)
def test_get_message_body(msg, expected):
    """Test that get_message_body returns the correct text/plain part."""
    assert get_message_body(msg) == expected


def test_get_message_body_no_text_plain():
    """Test that get_message_body returns an empty string if no text/plain part exists."""
    msg = EmailMessage()
    msg.add_alternative("<b>HTML only</b>", subtype="html")
    assert get_message_body(msg) == ""


def test_write_dict_to_csv(tmp_path):
    """Test writing a list of dictionaries to a CSV file."""
    # Arrange
    headers = ["col1", "col2", "col3"]
    data = [
        {"col1": "a", "col2": "b", "col3": "c"},
        {"col1": "1", "col2": "2", "col3": "3"},
    ]

    # Act
    output_file = MagicMock()
    output_file.path = str(tmp_path / "test.csv")
    result = write_dict_to_csv(data, headers, output_file)

    # Assert
    assert result == output_file

    with open(output_file.path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert rows == [
            {"col1": "a", "col2": "b", "col3": "c"},
            {"col1": "1", "col2": "2", "col3": "3"},
        ]


@pytest.mark.parametrize("msg_filename", MSG_TEST_DATA_FILENAMES)
def test_extract_message_metadata(msg_filename):
    """Test extracting metadata from various types of EML/MBOX messages."""

    # Arrange
    msg_path = f'testdata/{msg_filename}'
    msg = return_msg_from_file(file_path=msg_path)
    attachments = MSG_TEST_METADATA[msg_filename]["Attachments"]

    # Act
    msg_metadata = extract_message_metadata(
        attachments=attachments,
        message=msg
    )

    # Assert
    assert msg_metadata["Subject"] == MSG_TEST_METADATA[msg_filename]["Subject"]
    assert msg_metadata["From"] == MSG_TEST_METADATA[msg_filename]["From"]
    assert msg_metadata["To"] == MSG_TEST_METADATA[msg_filename]["To"]
    assert msg_metadata["Date"] == MSG_TEST_METADATA[msg_filename]["Date"]
    assert msg_metadata["Message-ID"] == MSG_TEST_METADATA[msg_filename]["Message-ID"]   
    assert msg_metadata["Attachments"] == attachments


@pytest.mark.parametrize("msg_archive_filename", MSG_TEST_DATA_FILENAMES)
def test_parse_eml_mbox_to_dict_and_extract_attachments(tmp_path, msg_archive_filename):
    # Arrange
    mail_path = f'testdata/{msg_archive_filename}'
    output_dir = tmp_path
    expected_attachments = MSG_TEST_METADATA[msg_archive_filename]["Attachments"]

    # Act
    if msg_archive_filename.endswith('.eml'):
       attachments, email_metadata = parse_eml_to_dict_and_extract_attachments(
           file_path=mail_path, 
           output_path=str(output_dir))
    elif msg_archive_filename.endswith('.mbox'):
       attachments, email_metadata = parse_mbox_to_dict_and_extract_attachments(
           file_path=mail_path,
           output_path=str(output_dir))
       
    email_metadata_dict = email_metadata[0] if isinstance(email_metadata, list) else email_metadata
    msg_id = santitize_filename(email_metadata_dict.get("Message-ID", ""))

    # Build expected attachment display names due to santitize_filename() logic
    expected_display_names = [
        f"{os.path.splitext(fn)[0]}.{msg_id}.{os.path.splitext(fn)[1].lstrip('.')}"
        for fn in expected_attachments
    ]
    actual_display_names = [os.path.basename(a["display_name"]) for a in attachments]

    # Assert
    assert len(email_metadata) > 0

    # Verify attachments were written out
    for expected_name in expected_display_names:
        matching = [a for a in attachments if os.path.basename(a["display_name"]) == expected_name]
        assert matching, f"Attachment '{expected_name}' not found in {actual_display_names}"
        file_path = matching[0]["path"]

