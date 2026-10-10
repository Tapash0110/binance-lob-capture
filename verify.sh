#!/usr/bin/env bash
set -e

cd /mnt/c/Users/tejas/Downloads/iRage

echo "========================================="
echo "Reviewer Checklist Verification"
echo "========================================="

echo "[1] Checking unit tests..."
./build/tests/unit_tests

echo "[2] Verifying CSV schema and column counts..."
for f in ./output/*_orderbook.csv; do
    cols=$(awk -F',' 'NR==2{print NF}' "$f")
    echo "  $f -> columns: $cols"
    if [ "$cols" -ne 26 ]; then
        echo "ERROR: Expected 26 columns, got $cols"
        exit 1
    fi
done

echo "[3] Checking headers for market data..."
for f in ./output/market_data_*[0-9].csv; do
    head -n 1 "$f"
done

echo "[4] Checking headers for orderbook..."
for f in ./output/*_orderbook.csv; do
    head -n 1 "$f"
    break
done

echo "[5] Checking secrets in source code..."
if git grep -iE "(api_key|secret|password)" -- src include 2>/dev/null; then
    echo "WARNING: Potential secret found in source code!"
    exit 1
else
    echo "  -> Clean! No secrets found in source code."
fi

echo "========================================="
echo "All Verification Checks Passed!"
echo "========================================="
