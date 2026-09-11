#!/bin/bash
# Run this once to initialize the project

echo "Setting up GDELT News Classifier Project..."

# Create all directories
mkdir -p services/{spark/{jobs,conf},kafka/init,producer,consumer}
mkdir -p data/{models,checkpoints,raw,gold}
mkdir -p notebooks logs scripts

# Create empty files for team to fill
touch services/producer/news_producer.py
touch services/consumer/news_consumer.py
touch services/spark/jobs/news_classifier.py
touch notebooks/exploration.ipynb

echo "Directory structure created"
echo ""
echo "NEXT STEPS FOR YOUR TEAM:"
echo "1. Each member: git clone [your-repo-url]"
echo "2. Run: docker-compose up -d"
echo "3. Edit services/producer/news_producer.py - Add GDELT data fetching"
echo "4. Edit services/consumer/news_consumer.py - Add classification model"
echo "5. Test: docker-compose logs -f consumer"
echo ""
echo "Useful commands:"
echo "   docker-compose ps           # Check all services"
echo "   docker-compose logs -f      # Follow all logs"
echo "   docker-compose restart [service]  # Restart specific service"