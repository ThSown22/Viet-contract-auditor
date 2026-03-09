#!/usr/bin/env pwsh
# ============================================================================
# VIET-CONTRACT AUDITOR - Phase 1 & 2: Environment Setup Script (Windows/PowerShell)
# ============================================================================
# Mô tả: Script thiết lập môi trường ảo bằng 'uv' và cài đặt dependencies.
# Sử dụng: .\setup.ps1
# ============================================================================

Write-Host "============================================" -ForegroundColor Cyan
Write-Host " VIET-CONTRACT AUDITOR - Environment Setup" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# --- Bước 1: Kiểm tra 'uv' đã cài đặt chưa ---
Write-Host "`n[1/4] Kiem tra 'uv' package manager..." -ForegroundColor Yellow
$uvCheck = Get-Command uv -ErrorAction SilentlyContinue
if (-not $uvCheck) {
    Write-Host "  -> 'uv' chua duoc cai dat. Dang cai dat..." -ForegroundColor Red
    Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression
    # Reload PATH
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
} else {
    Write-Host "  -> 'uv' da duoc cai dat: $(uv --version)" -ForegroundColor Green
}

# --- Bước 2: Khởi tạo project Python với uv ---
Write-Host "`n[2/4] Khoi tao Python project voi 'uv'..." -ForegroundColor Yellow
if (-not (Test-Path "pyproject.toml")) {
    uv init --python 3.11
    Write-Host "  -> Da khoi tao project Python 3.11" -ForegroundColor Green
} else {
    Write-Host "  -> pyproject.toml da ton tai, bo qua khoi tao." -ForegroundColor Green
}

# --- Bước 3: Tạo virtual environment ---
Write-Host "`n[3/4] Tao virtual environment..." -ForegroundColor Yellow
if (-not (Test-Path ".venv")) {
    uv venv --python 3.11
    Write-Host "  -> Da tao .venv" -ForegroundColor Green
} else {
    Write-Host "  -> .venv da ton tai." -ForegroundColor Green
}

# --- Bước 4: Cài đặt dependencies ---
Write-Host "`n[4/4] Cai dat dependencies..." -ForegroundColor Yellow

# Dependencies cho Phase 1 & 2:
# - datasets: Tải dataset từ HuggingFace
# - spacy: Xử lý ngôn ngữ tự nhiên (tokenizer, sentence splitting)
# - tqdm: Progress bar cho xử lý dữ liệu
# - regex: Regex nâng cao (hỗ trợ Unicode tốt hơn 're' built-in)
uv add datasets spacy tqdm

Write-Host "`n  -> Tai spaCy blank Vietnamese model (khong can download model lon)..." -ForegroundColor Yellow
# Sử dụng blank model 'vi' - không cần tải model nặng
# Model sẽ được tạo programmatically trong code

Write-Host "`n============================================" -ForegroundColor Green
Write-Host " SETUP HOAN TAT!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host "De chay pipeline:" -ForegroundColor White
Write-Host "  uv run python src/main.py" -ForegroundColor White
Write-Host ""
