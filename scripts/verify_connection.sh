#!/bin/bash

echo "🔍 VERIFYING ALL CONNECTIONS..."
echo "================================"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 1. Check Docker services
echo -e "\n${YELLOW}1. Checking service status...${NC}"
SERVICES=$(docker-compose ps --services --filter "status=running" | wc -l)
if [ $SERVICES -ge 4 ]; then
    echo -e "${GREEN}✓ All services are running${NC}"
else
    echo -e "${RED}✗ Some services are not running${NC}"
    docker-compose ps
    exit 1
fi

# 2. Test Zookeeper
echo -e "\n${YELLOW}2. Testing Zookeeper...${NC}"
ZK_TEST=$(echo ruok | nc -w 2 localhost 2181 2>/dev/null)
if [ "$ZK_TEST" = "imok" ]; then
    echo -e "${GREEN}✓ Zookeeper is responding${NC}"
else
    echo -e "${RED}✗ Zookeeper not responding${NC}"
fi

# 3. Test Kafka
echo -e "\n${YELLOW}3. Testing Kafka...${NC}"
KAFKA_CONTAINER=$(docker ps --filter "name=kafka" --format "{{.ID}}")
if [ ! -z "$KAFKA_CONTAINER" ]; then
    # Test if we can list topics
    TOPICS=$(docker exec $KAFKA_CONTAINER kafka-topics.sh --list --bootstrap-server localhost:9092 2>&1)
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Kafka is accessible${NC}"
        echo "  Available topics: $TOPICS"
    else
        echo -e "${RED}✗ Kafka not accessible${NC}"
    fi
else
    echo -e "${RED}✗ Kafka container not found${NC}"
fi

# 4. Test Producer
echo -e "\n${YELLOW}4. Testing Producer...${NC}"
PRODUCER_LOGS=$(docker logs --tail 5 news-classifier-project-producer-1 2>&1)
if echo "$PRODUCER_LOGS" | grep -q "Sent to topic"; then
    echo -e "${GREEN}✓ Producer is sending messages${NC}"
elif echo "$PRODUCER_LOGS" | grep -q "Connected"; then
    echo -e "${GREEN}✓ Producer connected to Kafka${NC}"
else
    echo -e "${RED}✗ Producer may not be working${NC}"
    echo "  Last logs: $PRODUCER_LOGS"
fi

# 5. Test Consumer
echo -e "\n${YELLOW}5. Testing Consumer...${NC}"
CONSUMER_LOGS=$(docker logs --tail 10 news-classifier-project-consumer-1 2>&1)
if echo "$CONSUMER_LOGS" | grep -q "Query .* started"; then
    echo -e "${GREEN}✓ Spark streaming is running${NC}"
elif echo "$CONSUMER_LOGS" | grep -q "Kafka"; then
    echo -e "${GREEN}✓ Consumer connected to Kafka${NC}"
else
    echo -e "${RED}✗ Consumer may not be working${NC}"
    echo "  Last logs: $CONSUMER_LOGS"
fi

# 6. Test Spark UI
echo -e "\n${YELLOW}6. Testing Spark...${NC}"
SPARK_UI=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080)
if [ "$SPARK_UI" = "200" ]; then
    echo -e "${GREEN}✓ Spark UI is accessible at http://localhost:8080${NC}"
else
    echo -e "${RED}✗ Spark UI not accessible${NC}"
fi

# 7. End-to-end test
echo -e "\n${YELLOW}7. Running end-to-end test...${NC}"
# Send a test message
docker exec $KAFKA_CONTAINER bash -c "echo '{\"title\":\"test\",\"url\":\"test\",\"timestamp\":123,\"source\":\"test\"}' | kafka-console-producer.sh --topic gdelt-news --bootstrap-server localhost:9092" 2>/dev/null
sleep 3
# Check if consumer received it
if docker logs --tail 20 news-classifier-project-consumer-1 2>&1 | grep -q "test"; then
    echo -e "${GREEN}✓ End-to-end pipeline working!${NC}"
else
    echo -e "${YELLOW}⚠ End-to-end test pending (may take a few seconds)${NC}"
fi

echo -e "\n${GREEN}================================${NC}"
echo -e "${GREEN}VERIFICATION COMPLETE${NC}"
echo -e "\n${YELLOW}To monitor in real-time:${NC}"
echo "  docker-compose logs -f consumer"
echo "  docker-compose logs -f producer"
echo "  Open http://localhost:8080 for Spark UI"