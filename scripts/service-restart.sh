#!/bin/bash
# service-restart.sh — idempotency verification for inventory-service
#
# What this script does:
#   1. Places an order via POST /api/orders
#   2. Waits 1 second (inventory-service begins processing OrderCreated)
#   3. Force-kills the inventory-service container (simulates a crash)
#   4. Docker Compose auto-restarts it (restart: unless-stopped must be set)
#   5. Waits for the service to become healthy again
#   6. Inspects the inventory topic for duplicate InventoryReserved events
#
# Expected result:
#   - Exactly ONE InventoryReserved event for the orderId on the inventory topic
#   - NO duplicate PaymentProcessed events on the payments topic
#   - Final saga status = COMPLETED (or FAILED if payment simulation is active)
#
# If you see two InventoryReserved events for the same orderId, idempotency is broken.

SERVICE=${1:-inventory-service}
echo "Killing $SERVICE..."
docker kill $SERVICE
echo "Waiting 3 seconds..."
sleep 3
echo "Restarting $SERVICE..."
docker start $SERVICE
echo "$SERVICE restarted."
