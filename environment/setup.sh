#!/usr/bin/env bash
# =============================================================
# setup.sh — Environment setup script for Log Analytics Platform
# =============================================================
# Usage:
#   chmod +x environment/setup.sh
#   ./environment/setup.sh
# =============================================================

set -e  # Exit immediately on any error

echo "=================================================="
echo "  Log Analytics Platform — Environment Setup"
echo "=================================================="

# ---- 1. Check Python version ----
REQUIRED_PYTHON="3.10"
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)

echo ""
echo "[1/5] Checking Python version..."
if python3 -c "import sys; exit(0 if sys.version_info >= (3,10) else 1)"; then
    echo "  ✅  Python $PYTHON_VERSION found (>= $REQUIRED_PYTHON required)"
else
    echo "  ❌  Python $REQUIRED_PYTHON+ required. Found: $PYTHON_VERSION"
    echo "      Please install Python 3.10 or newer and re-run this script."
    exit 1
fi

# ---- 2. Create virtual environment ----
echo ""
echo "[2/5] Creating virtual environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "  ✅  Virtual environment created at .venv/"
else
    echo "  ℹ️   Virtual environment already exists — skipping creation"
fi

# Activate venv
source .venv/bin/activate
echo "  ✅  Virtual environment activated"

# ---- 3. Upgrade pip ----
echo ""
echo "[3/5] Upgrading pip..."
pip install --upgrade pip --quiet
echo "  ✅  pip upgraded"

# ---- 4. Install dependencies ----
echo ""
echo "[4/5] Installing dependencies from environment/requirements.txt..."
pip install -r environment/requirements.txt --quiet
echo "  ✅  All dependencies installed"

# ---- 5. Verify Dask and Ray ----
echo ""
echo "[5/5] Verifying Dask and Ray installations..."

python3 - <<'EOF'
import sys

# Verify Dask
try:
    import dask
    import dask.dataframe as dd
    print(f"  ✅  Dask {dask.__version__} — OK")
except ImportError as e:
    print(f"  ❌  Dask import failed: {e}")
    sys.exit(1)

# Verify Ray
try:
    import ray
    print(f"  ✅  Ray  {ray.__version__} — OK")
except ImportError as e:
    print(f"  ❌  Ray import failed: {e}")
    sys.exit(1)

# Verify PyYAML (for schema files)
try:
    import yaml
    print(f"  ✅  PyYAML — OK")
except ImportError as e:
    print(f"  ❌  PyYAML import failed: {e}")
    sys.exit(1)

print()
print("  All core dependencies verified successfully.")
EOF

# ---- Done ----
echo ""
echo "=================================================="
echo "  ✅  Setup complete!"
echo ""
echo "  To activate the environment in a new terminal:"
echo "    source .venv/bin/activate"
echo ""
echo "  To run the environment tests:"
echo "    pytest tests/test_environment.py -v"
echo "=================================================="