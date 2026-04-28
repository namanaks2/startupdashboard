"""
FastAPI application — Smart Data Dashboard for Startups
"""
from __future__ import annotations

import io
from typing import Annotated

import pandas as pd
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr

import auth
import database as db
import data_processor as dp
from config import get_settings

settings = get_settings()

app = FastAPI(
    title="Smart Data Dashboard API",
    version="1.0.0",
    description="API for the Smart Data Dashboard for Startups",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.FRONTEND_URL, "http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

bearer_scheme = HTTPBearer(auto_error=False)


# ── Auth dependency ───────────────────────────────────────────────────────────

def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> dict:
    if not credentials:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    payload = auth.decode_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    user = db.get_user_by_id(payload.get("sub", ""))
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


# ── Pydantic models ───────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    name: str
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.post("/auth/register", response_model=TokenResponse, tags=["Auth"])
def register(body: RegisterRequest):
    if db.get_user_by_email(body.email):
        raise HTTPException(status_code=400, detail="Email already registered")
    hashed = auth.hash_password(body.password)
    user = db.create_user(body.email, hashed, body.name)
    token = auth.create_access_token({"sub": user["id"]})
    return TokenResponse(access_token=token, user={k: v for k, v in user.items() if k != "hashed_password"})


@app.post("/auth/login", response_model=TokenResponse, tags=["Auth"])
def login(body: LoginRequest):
    user = db.get_user_by_email(body.email)

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    
    if not auth.verify_password(body.password, user["hashed_password"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    token = auth.create_access_token({"sub": user["id"]})
    return TokenResponse(access_token=token, user={k: v for k, v in user.items() if k != "hashed_password"})


@app.get("/auth/me", tags=["Auth"])
def me(current_user: Annotated[dict, Depends(get_current_user)]):
    return {k: v for k, v in current_user.items() if k != "hashed_password"}


# ── Dataset routes ────────────────────────────────────────────────────────────

@app.get("/datasets", tags=["Datasets"])
def list_datasets(current_user: Annotated[dict, Depends(get_current_user)]):
    datasets = db.get_datasets_for_user(current_user["id"])
    return [
        {k: v for k, v in ds.items() if k != "raw_data"}
        for ds in datasets
    ]


@app.post("/datasets/upload", tags=["Datasets"])
async def upload_csv(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
):
    # Validate file type
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:  # 10 MB cap
        raise HTTPException(status_code=400, detail="File too large (max 10 MB)")

    try:
        df = dp.parse_csv(contents)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse CSV: {e}")

    if df.empty:
        raise HTTPException(status_code=422, detail="CSV is empty")

    kpis = dp.compute_kpis(df)
    charts = dp.generate_chart_data(df)
    insights = dp.generate_insights(df, kpis)
    forecast = dp.generate_forecast(df)

    dataset = db.save_dataset(
        current_user["id"],
        {
            "filename": file.filename,
            "row_count": len(df),
            "columns": df.columns.tolist(),
            "kpis": kpis,
            "charts": charts,
            "insights": insights,
            "forecast": forecast,
            "preview": dp.df_to_records(df.head(50)),
        },
    )

    return {
        "id": dataset["id"],
        "filename": dataset["filename"],
        "row_count": dataset["row_count"],
        "columns": dataset["columns"],
        "created_at": dataset["created_at"],
        "message": "Dataset uploaded and processed successfully",
    }


@app.get("/datasets/{dataset_id}", tags=["Datasets"])
def get_dataset(
    dataset_id: str,
    current_user: Annotated[dict, Depends(get_current_user)],
):
    dataset = db.get_dataset_by_id(current_user["id"], dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    ds = dict(dataset)
    ds.pop("raw_data", None)
    return ds


@app.get("/datasets/{dataset_id}/kpis", tags=["Analytics"])
def get_kpis(dataset_id: str, current_user: Annotated[dict, Depends(get_current_user)]):
    dataset = db.get_dataset_by_id(current_user["id"], dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset["kpis"]


@app.get("/datasets/{dataset_id}/charts", tags=["Analytics"])
def get_charts(dataset_id: str, current_user: Annotated[dict, Depends(get_current_user)]):
    dataset = db.get_dataset_by_id(current_user["id"], dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset["charts"]


@app.get("/datasets/{dataset_id}/insights", tags=["Analytics"])
def get_insights(dataset_id: str, current_user: Annotated[dict, Depends(get_current_user)]):
    dataset = db.get_dataset_by_id(current_user["id"], dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return {"insights": dataset["insights"]}


@app.get("/datasets/{dataset_id}/forecast", tags=["Analytics"])
def get_forecast(dataset_id: str, current_user: Annotated[dict, Depends(get_current_user)]):
    dataset = db.get_dataset_by_id(current_user["id"], dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset["forecast"]


@app.delete("/datasets/{dataset_id}", tags=["Datasets"])
def delete_dataset(dataset_id: str, current_user: Annotated[dict, Depends(get_current_user)]):
    success = db.delete_dataset(current_user["id"], dataset_id)
    if not success:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return {"message": "Dataset deleted"}


class ChatRequest(BaseModel):
    message: str

@app.post("/chat", tags=["Chat"])
def chat_endpoint(body: ChatRequest, current_user: Annotated[dict, Depends(get_current_user)]):
    message = body.message.lower()
    
    # Simple logic for the chatbot
    response = "I'm your SmartDash AI assistant. I can help you analyze your startup metrics."
    if "hello" in message or "hi" in message:
        response = f"Hello {current_user['name']}! How can I assist you with your data today?"
    elif "data" in message or "dataset" in message:
        datasets = db.get_datasets_for_user(current_user["id"])
        response = f"You currently have {len(datasets)} datasets uploaded. Would you like to view insights on them?"
    elif "forecast" in message:
        response = "I can help forecast your revenue and growth. Just upload a dataset and navigate to the Forecast panel!"
    elif "help" in message:
        response = "You can ask me about your data, how to use the dashboard, or specific metrics!"
    else:
        response = "That's an interesting question about your startup. I'm currently in beta, but I'll be able to answer this in detail soon!"
        
    return {"reply": response}


@app.get("/health")
def health():
    return {"status": "ok", "version": "1.0.0"}
