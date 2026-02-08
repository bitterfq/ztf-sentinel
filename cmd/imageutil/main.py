from fastapi import FastAPI
from fetch_stamps import fetch_and_save_all

app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "ok"}

@app.post("/fetch_stamps/{object_id}")
def fetch_stamps_endpoint(object_id: str, output_dir: str = "stamps"):
    try:
        images = fetch_and_save_all(object_id, output_dir)
        if images is None:
            return {
                "status": "error",
                "message": f"failed to fetch/save stamps for {object_id}",
                "images": []
            }
        return {"status": "success",
                "message": f"Stamps fetched and saved for {object_id}",
                "images": images
            }
    except Exception as e:
        return {"status": "error", "message": str(e), "images": []}