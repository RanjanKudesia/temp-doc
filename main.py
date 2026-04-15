"""Entry point for temp-doc service."""

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8006,
        reload=True,
        log_level="info",
    )
