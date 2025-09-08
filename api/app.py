import os
import datetime
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
from src.logger import setup_logging # noqa: E402
from api.utils import get_datasets_list, fetch_single_dataset # noqa: E402


app = FastAPI()
logger = setup_logging()

origins = [
    "http://localhost:3000",
    "https://example-production-domain.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", tags=["Root"])
def root():
    try:
        logger.info("Root endpoint accessed")
        return {"message": "Welcome to the NYC Jobs Audit API. Please visit '/docs' for documentation on how to use this API."}
    except Exception as e:
        logger.error(f"Error accessing root endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/health", tags=["Health"])
def health():
    try: 
        logger.info("Health endpoint accessed")
        return {"status": "healthy",
                "timestamp": datetime.datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Error fetching health status: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/datasets", tags=["Datasets"])
def read_datasets_list():
    try:
        datasets = get_datasets_list()
        return datasets
    except Exception as e:
        logger.error(f"Error fetching datasets list: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/datasets/{dataset_id}", tags=["Datasets"])
async def read_dataset(
    dataset_id: int,
    offset: int = Query(0, ge=0, description="Pagination offset (default: 0)"),
    limit: int = Query(1000, ge=1, le=7500, description="Pagination limit (default: 1000, max: 7500)")
):
    try:
        dataset = fetch_single_dataset(dataset_id, offset, limit)
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        return dataset
    except Exception as e:
        logger.error(f"Error fetching dataset {dataset_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")