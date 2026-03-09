PROJECT KNOWLEDGE BASE & AI CODING AGENT INSTRUCTIONS

Project Name: Viet-Contract Auditor (Hệ thống Kiểm toán Hợp đồng Pháp lý đa tác tử)
Core Technology: LightRAG (Graph-based RAG) + Multi-Agent Architecture
Target Domain: Vietnamese Legal System (Luật Dân sự, Luật Thương mại, Luật Lao động).

🎯 1. PROJECT OVERVIEW FOR AI AGENT

Your Role: You are an Expert Senior AI Engineer. Your task is to build this project step-by-step.
Project Goal: Build an action-oriented Legal-Tech system. The system takes a Vietnamese contract (PDF/Word) as input, uses LightRAG to retrieve relevant legal cross-references from a Knowledge Graph, passes the context through a Multi-Agent pipeline, and outputs an Audit Report highlighting legal traps and suggesting corrections.

🛠 2. TECH STACK & CONFIGURATION

Programming Language: Python 3.10+

Graph/RAG Core: LightRAG (Original repo logic, adapted for Vietnamese).

Multi-Agent Framework: LangGraph or CrewAI (Prefer LangGraph for explicit control flow).

LLM Backend (Generative & Agents): gpt-4o-mini (OpenAI API).

Embedding Model: text-embedding-3-small (OpenAI API).

Graph Database: NetworkX (for local graph processing) or Neo4j (if scalability is needed).

Vector Database: ChromaDB or FAISS or NanoVectorDB.

📂 3. DATA PROCESSING PIPELINE (CRITICAL RULES)

Rule 3.1: Dataset Segregation

Graph Knowledge Base (Train): Use strictly filtered legal texts (Luật Dân sự, Luật Thương mại, Luật Lao động) from Legal-Corpus-Zalo.

Evaluation Benchmark (Test): Use vietnamese-legal-qa and custom "Dummy Contracts" (Hợp đồng mồi). Do NOT index this dataset into the Knowledge Graph.

Rule 3.2: Semantic Chunking (DO NOT USE CHARACTER SPLITTING)

Vietnamese legal documents have strict hierarchies.

Action: Write a custom Python chunker using re (Regex).

Regex Target: Split strictly at Điều \d+\. (Article 1, Article 2...). Ensure one chunk contains a complete Article.

Parameters: Target chunk size: ~800-1200 tokens. Overlap: ~100-150 tokens.

Rule 3.3: Custom LightRAG Prompt Injection

The default LightRAG entity extraction prompt is in English.

Action: Override the default prompt with a Vietnamese legal-specific prompt.

Target Entities: [Chủ Thể], [Hành Vi], [Quyền Lợi], [Nghĩa Vụ], [Chế Tài/Mức Phạt], [Tài Sản].

Target Relations: [Bị cấm], [Được phép], [Quy định tại], [Xử phạt bằng].

🤖 4. MULTI-AGENT WORKFLOW ARCHITECTURE

Implement a state graph (e.g., using LangGraph StateGraph) with the following nodes:

Agent 1: Preprocessing & Router Agent

Input: User uploaded contract (PDF/Text).

Task: 1. Parse PDF to text.
2. Use LLM to classify contract type (e.g., "Hợp đồng Thuê nhà" -> Route to Civil Law).

Output: Clean text + Contract Domain Category.

Agent 2: Retrieval Agent (The LightRAG Engine)

Input: Contract text chunks + Domain Category.

Task: - Generate search queries based on contract clauses (e.g., "Giới hạn mức phạt vi phạm hợp đồng thương mại").

Trigger LightRAG Low-level Retrieval (for specific entities) and High-level Retrieval (for legal context/communities).

Output: A compiled Markdown string of Legal Ground Truth Context.

Agent 3: Audit & Reasoning Agent (The Core Logic)

Input: Contract text chunks + Legal Ground Truth Context.

Task: Chain-of-Thought reasoning. Cross-check every clause in the contract against the retrieved law. Identify Numeric Traps, Logical Traps, and Omissions.

Output: A structured JSON object containing:

[
  {
    "clause": "Trích dẫn câu sai trong hợp đồng",
    "violation": "Lý do sai luật",
    "reference_law": "Điều X, Luật Y",
    "suggested_fix": "Cách viết lại cho đúng"
  }
]


Agent 4: Generator Agent (Action Output)

Input: JSON object from Agent 3.

Task: Format the findings into a professional, formal Vietnamese Legal Audit Report.

Output: Final Markdown or .docx file.

📊 5. AUTOMATED BENCHMARKING PIPELINE (LLM-as-a-Judge)

You must create an evaluator.py script to run automated tests.

Test Data: A directory /test_contracts containing PDF files with known injected traps, paired with /ground_truth_json defining the exact traps.

Metrics to Calculate:

Trap Hit Rate (Recall): (Traps Found by Agent / Total Known Traps) * 100. (Target > 85%).

False Alarm Rate (False Positive): (Invalid Traps Flagged / Total Flags) * 100. (Target < 5%).

LLM Judge: Use a strict prompt asking gpt-4o to rate the "Correction Legal Validity" of the suggested_fix on a scale of 1-5.

🚀 6. IMPLEMENTATION PHASES FOR AI AGENT

Agent, please execute the project in this exact order:

[ ] Phase 1: Foundation. Setup virtual env, install dependencies (lightrag-hku, langgraph, openai, pymupdf).

[ ] Phase 2: Data Engineering. Build the SemanticChunker class using Regex for Vietnamese laws.

[ ] Phase 3: Graph Building. Initialize LightRAG, override the extraction prompt to Vietnamese, and index the Legal-Corpus-Zalo filtered data.

[ ] Phase 4: Multi-Agent Dev. Build the LangGraph pipeline (Router -> Retrieval -> Audit -> Generator).

[ ] Phase 5: CLI / Streamlit UI. Build a simple UI to upload a PDF and display the Audit Report.

[ ] Phase 6: Evaluation. Build the evaluator.py to run the Benchmark.

Note to Agent: When writing code, write modular, well-commented Python code. Ensure robust error handling (Try/Except) especially for API calls and JSON parsing. Output code strictly related to the current requested phase.