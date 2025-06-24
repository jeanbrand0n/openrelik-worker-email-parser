# OpenRelik Email Parsing Worker
## Description
This OpenRelik worker handles the parsing of EML/MBOX email artifacts.
Message metadata (e.g. to, from, subject) is extracted to a `csv` file
and attached/inline content is extracted and keyed by message ID, to the 
workflow directory.

## Deploy
Add the below configuration to the OpenRelik docker-compose.yml file.

```
openrelik-worker-email-parser:
    container_name: openrelik-worker-email-parser
    image: ghcr.io/openrelik/openrelik-worker-email-parser:latest
    restart: always
    environment:
      - REDIS_URL=redis://openrelik-redis:6379
      - OPENRELIK_PYDEBUG=0
    volumes:
      - ./data:/usr/share/openrelik/data
    command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-email-parser"
    # ports:
      # - 5678:5678 # For debugging purposes.
```

## Test
```
pip install poetry
poetry install --with test --no-root
poetry run pytest --cov=. -v
```
