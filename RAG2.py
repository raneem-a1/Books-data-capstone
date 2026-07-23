#!pip install chromadb sentence-transformers rank-bm25 numpy anthropic

import os
import re
import numpy as np
import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
from sentence_transformers import SentenceTransformer, CrossEncoder
from rank_bm25 import BM25Okapi
import anthropic

client_llm = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

#!wget -O Books.csv "https://gist.githubusercontent.com/jaidevd/23aef12e9bf56c618c41/raw/c05e98672b8d52fa0cb94aad80f75eb78342e5d4/books.csv"

import pandas as pd
books = pd.read_csv("data/Books.csv")
DOCUMENTS = []
for i, row in books.iterrows():
    DOCUMENTS.append({
        "id": f"book_{i}",
        "text": f"Title: {row['Title']}. Author: {row['Author']}. Genre: {row['Genre']}. Height: {row['Height']}. Publisher: {row['Publisher']}"
    })


def chunk_documents(docs: list[dict], chunk_size: int = 2) -> list[dict]:
    all_chunks = []
    for doc in docs:
        sentences = re.split(r"(?<=[.!?])\s+", doc["text"].strip())
        for i in range(0, len(sentences), max(1, chunk_size - 1)):
            chunk_text = " ".join(sentences[i : i + chunk_size])
            if not chunk_text.strip():
                continue
            all_chunks.append({
                "id":     f"{doc['id']}_chunk_{i:03d}",
                "text":   chunk_text,
                "doc_id": doc["id"],
            })
    return all_chunks


def build_vector_index(chunks: list[dict]) -> chromadb.Collection:
    print("\n📦 Building ChromaDB vector index...")
    ef = SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    client     = chromadb.Client()
    collection = client.get_or_create_collection(
        "rag_lab_knowledge_base", embedding_function=ef
    )
    collection.add(
        ids       = [c["id"]   for c in chunks],
        documents = [c["text"] for c in chunks],
        metadatas = [{"doc_id": c["doc_id"]} for c in chunks],
    )
    print(f"   ✅ {len(chunks)} chunks indexed (HNSW backend, all-MiniLM-L6-v2 embeddings)")
    return collection


class BM25Index:
    def __init__(self, chunks: list[dict]):
        tokenised  = [c["text"].lower().split() for c in chunks]
        self.bm25  = BM25Okapi(tokenised)
        self.chunks = chunks

    def search(self, query: str, top_k: int = 10) -> list[tuple[float, dict]]:
        scores = self.bm25.get_scores(query.lower().split())
        ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)
        return [(score, self.chunks[idx]) for idx, score in ranked[:top_k]]


def reciprocal_rank_fusion(
    vector_hits: list[dict],
    bm25_hits:   list[tuple[float, dict]],
    k:           int = 60,
    top_k:       int = 6,
) -> list[dict]:
    rrf_scores: dict[str, float] = {}
    id_to_chunk: dict[str, dict] = {}

    for rank, hit in enumerate(vector_hits):
        cid = hit["id"]
        rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (k + rank + 1)
        id_to_chunk[cid] = {"id": cid, "text": hit["document"]}

    for rank, (_, chunk) in enumerate(bm25_hits):
        cid = chunk["id"]
        rrf_scores[cid] = rrf_scores.get(cid, 0.0) + 1.0 / (k + rank + 1)
        id_to_chunk[cid] = chunk

    sorted_ids = sorted(rrf_scores, key=lambda x: rrf_scores[x], reverse=True)
    return [id_to_chunk[cid] for cid in sorted_ids[:top_k]]


def rerank(
    query:       str,
    candidates:  list[dict],
    model_name:  str = "cross-encoder/ms-marco-MiniLM-L-6-v2",
    top_k:       int = 3,
) -> list[dict]:
    print(f"  🎯 Cross-encoder reranking {len(candidates)} candidates...")
    model  = CrossEncoder(model_name)
    pairs  = [(query, c["text"]) for c in candidates]
    scores = model.predict(pairs)
    ranked = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
    return [doc for _, doc in ranked[:top_k]]


def build_rag_prompt(query: str, context_docs: list[dict]) -> str:
    context = "\n\n".join(
        f"[Source {i+1}]: {d['text']}" for i, d in enumerate(context_docs)
    )
    return (
        "You are a data engineering expert. Answer the question strictly based\n"
        "on the provided context. Do not add information not in the context.\n\n"
        f"CONTEXT:\n{context}\n\n"
        f"QUESTION: {query}\n\n"
        "ANSWER:"
    )


def generate_answer(prompt: str, model: str = "claude-sonnet-4-5") -> str:
    response = client_llm.messages.create(
        model=model,
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


def evaluate(
    query:          str,
    retrieved_docs: list[dict],
    embed_model:    SentenceTransformer,
    relevance_threshold: float = 0.30,
) -> dict:
    q_emb   = embed_model.encode(query, normalize_embeddings=True)
    scores  = [
        float(np.dot(q_emb, embed_model.encode(d["text"], normalize_embeddings=True)))
        for d in retrieved_docs
    ]
    relevant = sum(s > relevance_threshold for s in scores)
    return {
        "context_precision": round(relevant / len(scores), 3),
        "avg_similarity":    round(sum(scores) / len(scores), 3),
        "chunks_in_context": len(retrieved_docs),
    }


def main():
    print("=" * 65)
    print("  Day 3 Lab — Vector Databases and Advanced RAG Engineering")
    print("=" * 65)

    chunks = chunk_documents(DOCUMENTS, chunk_size=2)
    print(f"\n📄 {len(DOCUMENTS)} documents → {len(chunks)} chunks after splitting")

    collection  = build_vector_index(chunks)
    bm25_index  = BM25Index(chunks)
    embed_model = SentenceTransformer("all-MiniLM-L6-v2")

    queries = [
        "Who wrote Fundamentals of Wavelets?",
        "Which books are published by Wiley?",
        "Show me fiction books.",
        "Which books belong to the history category?"
    ]

    for query in queries:
        print(f"\n{'=' * 65}")
        print(f"QUERY: {query}")
        print("=" * 65)

        vec_results = collection.query(query_texts=[query], n_results=6)
        vec_hits = [
            {"id": vid, "document": vdoc}
            for vid, vdoc in zip(
                vec_results["ids"][0],
                vec_results["documents"][0],
            )
        ]
        print(f"\n  🔍 Vector search:   {len(vec_hits)} candidates")

        bm25_hits = bm25_index.search(query, top_k=6)
        print(f"  🔑 BM25 search:     {len(bm25_hits)} candidates")

        hybrid = reciprocal_rank_fusion(vec_hits, bm25_hits, top_k=6)
        print(f"  ⚡ RRF fusion:      {len(hybrid)} merged candidates")

        final_docs = rerank(query, hybrid, top_k=3)

        prompt = build_rag_prompt(query, final_docs)

        print("\n  📝 Top-3 chunks after reranking:")
        for i, doc in enumerate(final_docs, 1):
            print(f"    [{i}] {doc['text'][:110]}...")

        answer = generate_answer(prompt)
        print("\n  💬 Grounded answer:")
        print(f"    {answer}")

        metrics = evaluate(query, final_docs, embed_model)
        print(f"\n  📊 Retrieval metrics: {metrics}")
        if metrics["context_precision"] < 0.5:
            print("  ⚠️  Low precision — consider increasing ef or lowering chunk overlap")
        else:
            print("  ✅  Retrieval quality looks good")

    print("\n" + "=" * 65)
    print("Lab complete.")
    print("=" * 65)


main()
