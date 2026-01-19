import uvicorn
import rag_agent.app as app

def main() -> None:
    uvicorn.run(app, host="127.0.0.1", port=8000) #TODO: Use container runtime to run API process instead of calling uvicorn directly (hardcoded host and port)

if __name__ == "__main__":
    main()