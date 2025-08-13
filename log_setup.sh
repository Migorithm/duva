#!/bin/bash

echo "🔧 Fixed OpenTelemetry Setup"

# Stop existing services
echo "🛑 Stopping existing services..."
docker-compose down -v


echo "🚀 Starting fixed observability stack..."
docker-compose up -d

echo "⏳ Waiting for services to start..."
sleep 5



echo "🧪 Testing Collector Config..."
# Check if collector started without errors
if docker-compose logs otel-collector | grep -q "Everything is ready"; then
    echo "✅ Collector started successfully"
elif docker-compose logs otel-collector | grep -q "Error:"; then
    echo "❌ Collector has errors:"
    docker-compose logs --tail=10 otel-collector
    exit 1
else
    echo "⏳ Collector is starting..."
    sleep 10
fi

echo ""
echo "🔍 Testing endpoints..."

# Test collector
if curl -s -f -X POST -H "Content-Type: application/json" -d '{}' http://localhost:4318/v1/logs > /dev/null 2>&1; then
    echo "✅ Collector HTTP endpoint is working"
else
    echo "⚠️  Collector endpoint test failed, but it might still work"
fi

# Test other services
services=(
    "http://localhost:3000:Grafana"
    "http://localhost:3100/ready:Loki"
    "http://localhost:16686:Jaeger"
    "http://localhost:9090:Prometheus"
)

for service in "${services[@]}"; do
    url="${service%:*}"
    name="${service#*:}"
    
    if curl -s -f "$url" > /dev/null 2>&1; then
        echo "✅ $name is ready"
    else
        echo "⏳ $name is starting..."
    fi
done

echo ""
echo "🏗️  Building Rust application..."
cargo build

if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
    echo ""
    echo "🎯 Ready to run your applications:"
    echo "   ./target/debug/multi-process-logging user-service instance-1 &"
    echo "   ./target/debug/multi-process-logging order-service instance-1 &"
    echo ""
    echo "🌐 Access UIs:"
    echo "   - Grafana: http://localhost:3000 (admin/admin)"
    echo "   - Jaeger: http://localhost:16686"
    echo ""
    echo "📝 View logs in Grafana:"
    echo "   1. Go to Explore → Select Loki"
    echo "   2. Query: {service_name=\"user-service\"}"
    echo ""
    echo "🛑 To stop everything:"
    echo "   docker-compose down && pkill -f multi-process-logging"
else
    echo "❌ Build failed"
    exit 1
fi