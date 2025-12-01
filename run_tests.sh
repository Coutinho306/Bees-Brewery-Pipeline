#!/bin/bash
# Test runner script

echo "========================================="
echo "Running BEES Breweries Pipeline Tests"
echo "========================================="
echo ""

# Check if containers are running
if ! docker-compose ps | grep -q "Up"; then
    echo "❌ Containers not running. Please start them first:"
    echo "   docker-compose up -d"
    exit 1
fi

# Run tests as airflow user
echo "Running tests inside Docker container..."
docker-compose exec -T -u airflow airflow-scheduler python -m pytest tests/ -v

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "✓ All tests passed!"
else
    echo ""
    echo "✗ Some tests failed (or pytest had issues)"
    exit 1
fi
