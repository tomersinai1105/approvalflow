# Query API.




from fastapi import FastAPI, HTTPException

from app.common.logger import log_event

from app.microservices.query_service.query_service import (
    get_human_review_queue,
    get_dashboard_metrics,
)




# Start the API.
app = FastAPI()


# Health Check. 
@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "service": "query-service",
        "message": "Query service is running.",
    }




# GET request: "/human-review".
# Try to return a queue of all human-escalated items and log a corresponding message.  
# Raise an error if occurs. 
@app.get("/human-review")
def human_review_queue_endpoint():

    try:
        items = get_human_review_queue()

        log_event(
            service = "query-service",
            event = "human_review_queue_requested",
            correlation_id = "system",
            tracking_id = "system",
            message = "Human review queue was requested.",
            queueSize = len(items),
        )

        return {
            "items": items,
        }

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "query_human_review_error",
                "message": "query-service failed while building human review queue.",
                "error": str(error),
            },
        )




# GET request: "/dashboard".
# Try to return a dashboard  and log a corresponding message. 
# Raise an error if occurs. 
@app.get("/dashboard")
def dashboard_endpoint():

    try:
        
        dashboard = get_dashboard_metrics()

        log_event(
            service = "query-service",
            event = "dashboard_requested",
            correlation_id = "system",
            tracking_id = "system",
            message = "Dashboard metrics were requested.",
            throughput = dashboard.get("throughput"),
            autoApprovedCount = dashboard.get("autoApprovedCount"),
            humanReviewCount = dashboard.get("humanReviewCount"),
        )

        return dashboard

    except Exception as error:
        raise HTTPException(
            status_code = 500,
            detail = {
                "status": "query_dashboard_error",
                "message": "query-service failed while building dashboard metrics.",
                "error": str(error),
            },
        )