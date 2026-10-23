"""RAG-based incident copilot using Gemini + FAISS.

Indexes runbooks and recent alerts into FAISS, then uses Gemini to
generate incident summaries with suggested next actions. Also pulls
live Prometheus metrics for context — this is the agent-based workflow
described in the Visa JD.

Endpoint: POST /copilot/incident/{alert_id}
"""

import os
from pathlib import Path

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from config import settings
from consumer.db import async_session
from copilot.tools import fetch_recent_metrics, format_metrics_for_llm
from observability.logging import get_logger

logger = get_logger("copilot.rag")
copilot_router = APIRouter()

# Paths
RUNBOOKS_DIR = Path(__file__).parent.parent / "docs" / "runbooks"

# Lazy-loaded RAG components
_vectorstore = None
_llm = None


def _init_rag():
    """Initialize FAISS vectorstore and Gemini LLM (lazy, one-time)."""
    global _vectorstore, _llm

    if _vectorstore is not None:
        return

    try:
        from langchain.text_splitter import RecursiveCharacterTextSplitter
        from langchain_community.vectorstores import FAISS
        from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings

        # Load runbook documents
        documents = []
        for md_file in RUNBOOKS_DIR.glob("*.md"):
            content = md_file.read_text(encoding="utf-8")
            documents.append({
                "content": content,
                "source": md_file.name,
            })

        if not documents:
            logger.warning("no_runbooks_found", dir=str(RUNBOOKS_DIR))
            return

        # Split into chunks
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            separators=["\n## ", "\n### ", "\n\n", "\n", " "],
        )

        texts = []
        metadatas = []
        for doc in documents:
            chunks = splitter.split_text(doc["content"])
            for chunk in chunks:
                texts.append(chunk)
                metadatas.append({"source": doc["source"]})

        # Create FAISS index with Gemini embeddings
        api_key = settings.google_api_key
        if not api_key or api_key == "your-gemini-api-key-here":
            logger.warning("gemini_api_key_not_set", hint="Set GOOGLE_API_KEY in .env")
            return

        embeddings = GoogleGenerativeAIEmbeddings(
            model=settings.gemini_embedding_model,
            google_api_key=api_key,
        )

        _vectorstore = FAISS.from_texts(texts, embeddings, metadatas=metadatas)

        _llm = ChatGoogleGenerativeAI(
            model=settings.gemini_chat_model,
            google_api_key=api_key,
            temperature=0.3,
            max_output_tokens=2048,
        )

        logger.info(
            "rag_initialized",
            runbooks=len(documents),
            chunks=len(texts),
        )

    except Exception as e:
        logger.error("rag_init_failed", error=str(e))


async def _get_alert_context(alert_id: int) -> dict | None:
    """Fetch alert + transaction details from the database."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT a.id, a.transaction_id, a.alert_type, a.severity,
                       a.details, a.created_at,
                       t.amount, t.country, t.card_id, t.fraud_score,
                       t.fraud_reasons, t.degraded
                FROM alerts a
                LEFT JOIN transactions t ON a.transaction_id = t.transaction_id
                WHERE a.id = :alert_id
            """),
            {"alert_id": alert_id},
        )
        row = result.fetchone()

    if not row:
        return None

    return {
        "alert_id": row[0],
        "transaction_id": row[1],
        "alert_type": row[2],
        "severity": row[3],
        "details": row[4],
        "created_at": row[5].isoformat() if row[5] else None,
        "amount": float(row[6]) if row[6] else None,
        "country": row[7],
        "card_id": row[8],
        "fraud_score": float(row[9]) if row[9] else None,
        "fraud_reasons": row[10] or [],
        "degraded": row[11],
    }


@copilot_router.post("/incident/{alert_id}")
async def incident_copilot(alert_id: int):
    """RAG copilot for incident response.

    Agent workflow:
    1. Fetch alert context from database
    2. Retrieve relevant runbook sections from FAISS
    3. Fetch live Prometheus metrics (tool call)
    4. Generate incident summary + next actions with Gemini

    This is the agent-based workflow from the JD.
    """
    _init_rag()

    if _vectorstore is None or _llm is None:
        raise HTTPException(
            status_code=503,
            detail="RAG copilot not available. Set GOOGLE_API_KEY in .env",
        )

    # Step 1: Get alert context
    alert = await _get_alert_context(alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

    # Step 2: Retrieve relevant runbook sections
    query = f"Alert: {alert['alert_type']}, severity: {alert['severity']}, fraud score: {alert['fraud_score']}, degraded: {alert['degraded']}"
    relevant_docs = _vectorstore.similarity_search(query, k=4)
    runbook_context = "\n\n---\n\n".join([
        f"**Source: {doc.metadata.get('source', 'unknown')}**\n{doc.page_content}"
        for doc in relevant_docs
    ])

    # Step 3: Fetch live metrics (agent tool call)
    try:
        metrics = await fetch_recent_metrics(minutes=15)
        metrics_context = format_metrics_for_llm(metrics)
    except Exception as e:
        metrics_context = f"Metrics unavailable: {str(e)}"

    # Step 4: Generate incident summary with Gemini
    prompt = f"""You are an incident response copilot for a real-time fraud detection pipeline.
Analyze the following alert and provide a structured incident summary.

## Alert Details
- Alert ID: {alert['alert_id']}
- Transaction ID: {alert['transaction_id']}
- Type: {alert['alert_type']}
- Severity: {alert['severity']}
- Fraud Score: {alert['fraud_score']}
- Amount: ${alert['amount']}
- Country: {alert['country']}
- Card ID: {alert['card_id']}
- Fraud Reasons: {', '.join(alert['fraud_reasons']) if alert['fraud_reasons'] else 'None'}
- Degraded Mode: {alert['degraded']}
- Time: {alert['created_at']}

## Relevant Runbook Sections
{runbook_context}

## Live Metrics
{metrics_context}

## Instructions
Provide a structured response with:
1. **Incident Summary**: What happened, in 2-3 sentences
2. **Risk Assessment**: How severe is this, considering the fraud score and alert context
3. **Likely Cause**: Based on the fraud reasons and metrics
4. **Recommended Actions**: Ordered list of next steps, referencing specific runbook commands
5. **Monitoring**: What to watch to confirm resolution

Be specific, actionable, and reference exact commands from the runbooks.
"""

    try:
        response = await _llm.ainvoke(prompt)
        summary = response.content
    except Exception as e:
        logger.error("llm_generation_failed", error=str(e))
        summary = f"LLM generation failed: {str(e)}. Review the runbook sections below."

    return {
        "alert_id": alert_id,
        "alert": alert,
        "incident_summary": summary,
        "relevant_runbooks": [doc.metadata.get("source") for doc in relevant_docs],
        "metrics_snapshot": metrics_context,
    }


@copilot_router.get("/health")
async def copilot_health():
    """Check if the RAG copilot is initialized and ready."""
    _init_rag()
    return {
        "status": "ready" if _vectorstore is not None else "not_initialized",
        "vectorstore": "loaded" if _vectorstore else "not_loaded",
        "llm": "ready" if _llm else "not_ready",
        "api_key_set": bool(settings.google_api_key and settings.google_api_key != "your-gemini-api-key-here"),
    }
