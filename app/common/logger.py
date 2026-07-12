# Logger for logging the events while running the application. 




import json

from datetime import datetime, UTC

import logging




LOG_FILE_PATH = "approvalflow.log"

logger = logging.getLogger("approvalflow")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.propagate = False

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler(
    LOG_FILE_PATH,
    encoding = "utf-8",
)
file_handler.setLevel(logging.INFO)

logger.addHandler(console_handler)
logger.addHandler(file_handler)




# Log a given event based on given details. 
def log_event(
    service: str, 
    event: str, 
    correlation_id: str | None = None, 
    tracking_id: str | None = None,
    level: str = "INFO",
    message: str | None = None,
    **fields,
) -> None:
    
    log_record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "level": level,
        "service": service,
        "event": event,
        "correlationId": correlation_id,
        "trackingId": tracking_id,
        "message": message,
    }

    for key, value in fields.items():
        log_record[key] = value

    clean_record = {}
    for key, value in log_record.items():
        if value is not None:
            clean_record[key] = value

    log_line = json.dumps(
        clean_record,
        ensure_ascii = False,
        default = str,
    )

    normalized_level = level.upper()

    if normalized_level == "ERROR":
        logger.error(log_line)
        
    elif normalized_level == "WARNING":
        logger.warning(log_line)
        
    else:
        logger.info(log_line)