"""OpenRelik Worker Email Parser Task"""

from src.email_parsing_utils import *

from openrelik_worker_common.file_utils import create_output_file
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery


# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-email-parser.tasks.command"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "openrelik-worker-email-parser",
    "description": "OpenRelik Worker Email Parser",
    # Configuration that will be rendered as a web for in the UI, and
    # any data entered by the user will be available to the task function
    # when executing (task_config).
    "task_config": [
        {
            "name": "openrelik_worker_email_parser_config",
            "label": "openrelik_worker_email_parser",
            "description": "OpenRelik Worker Email Parser Configuration",
            "type": "text",  # Types supported: text, textarea, checkbox
            "required": False,
        },
    ],
}


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def command(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Run <REPLACE_WITH_COMMAND> on input files.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task,
            if any.
        input_files: List of input file dictionaries (unused if 
            pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    input_files = get_input_files(pipe_result, input_files or [])
    output_files = []
    csv_headers = [
        "Timestamp", "Timestamp_desc", "Message", "To", "From", "Bcc",
        "Cc", "Subject", "Message-ID", "Date", "Content-Type",
        "Attachments", "User-Agent", "Body"]

    for input_file in input_files:
        input_extension = input_file.get("extension", "").lower()

        if input_extension not in SUPPORTED_EXTENSIONS:
            print('Skipping file with unsupported extension:',
                   input_file['extension'])
            continue

        output_file = create_output_file(
            output_path,
            display_name=input_file.get("display_name"),
            extension="csv",
            data_type="csv",
        )

        # MBOX
        if input_extension == "mbox":
            print(f"Processing MBOX file: {input_file.get('path')}")

            attachment_file_paths, mbox_dict = parse_mbox_to_dict_and_extract_attachments(
                file_path=input_file.get("path"),
                output_path=output_path)
            mbox_csv = write_dict_to_csv(
                message_dict=mbox_dict, headers=csv_headers,
                output_file=output_file)
            output_files.append(mbox_csv.to_dict())

            # Add attachments to output files
            if attachment_file_paths:
                output_files.extend(attachment_file_paths)

        # EML
        if input_extension == "eml":
            print(f"Processing EML file: {input_file.get('path')}")

            attachment_file_paths, eml_dict = parse_eml_to_dict_and_extract_attachments(
                file_path=input_file.get("path"),
                output_path=output_path)
            eml_csv = write_dict_to_csv(
                message_dict=[eml_dict],
                headers=csv_headers,
                output_file=output_file)
            output_files.append(eml_csv.to_dict())

            # Add attachments to output files
            if attachment_file_paths:
                output_files.extend(attachment_file_paths)

    if not output_files:
        raise RuntimeError(
            f"No compatible input files found. Supported extensions: {', '.join(SUPPORTED_EXTENSIONS)}."
        )

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        meta={},
    )
