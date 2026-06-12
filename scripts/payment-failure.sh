#!/bin/bash
# Sets payment-service to fail 100% of transactions
curl -s -X POST http://localhost:8081/api/simulate/failure \
  -H "Content-Type: application/json" \
  -d '{"service":"payment","failureRate":1.0}'
echo ""
echo "Payment failure rate set to 100%"
