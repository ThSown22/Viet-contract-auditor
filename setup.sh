#!/usr/bin/env bash
# ============================================================================
# VIET-CONTRACT AUDITOR - Phase 1 & 2: Environment Setup Script (Linux/macOS)
# ============================================================================
# Mô tả: Script thiết lập môi trường ảo bằng 'uv' và cài đặt dependencies.
# Sử dụng: chmod +x setup.sh && ./setup.sh
# ============================================================================

set -e

echo "============================================"
echo " VIET-CONTRACT AUDITOR - Environment Setup"
echo "============================================"

# --- Bước 1: Kiểm tra 'uv' ---
echo ""
echo "[1/4] Kiem tra 'uv' package manager..."
if ! command -v uv &> /dev/null; then
    echo "  -> 'uv' chua duoc cai dat. Dang cai dat..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source "$HOME/.cargo/env" 2>/dev/null || true
else
    echo "  -> 'uv' da duoc cai dat: $(uv --version)"
fi

# --- Bước 2: Khởi tạo project ---
echo ""
echo "[2/4] Khoi tao Python project voi 'uv'..."
if [ ! -f "pyproject.toml" ]; then
    uv init --python 3.11
    echo "  -> Da khoi tao project Python 3.11"
else
    echo "  -> pyproject.toml da ton tai, bo qua."
fi

# --- Bước 3: Virtual environment ---
echo ""
echo "[3/4] Tao virtual environment..."
if [ ! -d ".venv" ]; then
    uv venv --python 3.11
    echo "  -> Da tao .venv"
else
    echo "  -> .venv da ton tai."
fi

# --- Bước 4: Cài dependencies ---
echo ""
echo "[4/4] Cai dat dependencies..."
uv add datasets spacy tqdm

echo ""
echo "============================================"
echo " SETUP HOAN TAT!"
echo "============================================"
echo "De chay pipeline: uv run python src/main.py"
