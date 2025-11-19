import json
import psycopg2
from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime
import statistics

def create_tables(cur):
    """Create database schema for events and spread metrics"""
    
    # Original market events table (enhanced with spread fields)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS market_events (
            event_id VARCHAR(50) PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            event_type VARCHAR(30) NOT NULL,
            symbol VARCHAR(10) NOT NULL,
            side VARCHAR(10) NOT NULL,
            price NUMERIC(12, 4) NOT NULL,
            quantity INTEGER NOT NULL,
            order_id VARCHAR(50) NOT NULL,
            venue VARCHAR(20) NOT NULL,
            participant_type VARCHAR(30) NOT NULL,
            urgency_score NUMERIC(4, 3) NOT NULL,
            notional_value NUMERIC(15, 2) NOT NULL,
            
            -- NEW: Spread-related fields
            bid_price NUMERIC(12, 4) NOT NULL,
            ask_price NUMERIC(12, 4) NOT NULL,
            mid_price NUMERIC(12, 4) NOT NULL,
            quoted_spread_bps NUMERIC(8, 3) NOT NULL,
            effective_spread_bps NUMERIC(8, 3) NOT NULL,
            market_stress_level NUMERIC(4, 3) NOT NULL,
            
            CONSTRAINT check_side CHECK (side IN ('buy', 'sell')),
            CONSTRAINT check_event_type CHECK (event_type IN 
                ('limit_order', 'market_order', 'cancel', 'execution')),
            CONSTRAINT check_price_positive CHECK (price > 0),
            CONSTRAINT check_quantity_positive CHECK (quantity > 0),
            CONSTRAINT check_urgency_range CHECK (urgency_score >= 0 AND urgency_score <= 1)
        );
        """
    )
    
    # Indices for market events
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_symbol ON market_events(symbol);
        CREATE INDEX IF NOT EXISTS idx_timestamp ON market_events(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_venue ON market_events(venue);
        CREATE INDEX IF NOT EXISTS idx_event_type ON market_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_participant ON market_events(participant_type);
        CREATE INDEX IF NOT EXISTS idx_spread ON market_events(quoted_spread_bps);
        """
    )
    
    # NEW: Spread metrics table (from Flink aggregations)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS spread_metrics (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            symbol VARCHAR(10) NOT NULL,
            window_events_count INTEGER NOT NULL,
            
            -- Spread statistics
            avg_quoted_spread_bps NUMERIC(8, 3) NOT NULL,
            std_quoted_spread_bps NUMERIC(8, 3) NOT NULL,
            min_quoted_spread_bps NUMERIC(8, 3) NOT NULL,
            max_quoted_spread_bps NUMERIC(8, 3) NOT NULL,
            avg_effective_spread_bps NUMERIC(8, 3) NOT NULL,
            
            -- Volume metrics
            total_volume INTEGER NOT NULL,
            buy_volume INTEGER NOT NULL,
            sell_volume INTEGER NOT NULL,
            order_imbalance_ratio NUMERIC(5, 3) NOT NULL,
            
            -- Event counts
            limit_orders INTEGER NOT NULL,
            market_orders INTEGER NOT NULL,
            executions INTEGER NOT NULL,
            cancels INTEGER NOT NULL,
            quote_to_trade_ratio NUMERIC(8, 3) NOT NULL,
            
            -- Market conditions
            avg_market_stress NUMERIC(4, 3) NOT NULL,
            current_bid NUMERIC(12, 4) NOT NULL,
            current_ask NUMERIC(12, 4) NOT NULL,
            current_mid NUMERIC(12, 4) NOT NULL,
            
            -- Derived signals
            liquidity_score NUMERIC(10, 2) NOT NULL,
            spread_anomaly BOOLEAN NOT NULL,
            optimal_execution_window BOOLEAN NOT NULL
        );
        """
    )
    
    # Indices for spread metrics
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_spread_symbol ON spread_metrics(symbol);
        CREATE INDEX IF NOT EXISTS idx_spread_timestamp ON spread_metrics(timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_spread_anomaly ON spread_metrics(spread_anomaly);
        CREATE INDEX IF NOT EXISTS idx_optimal_window ON spread_metrics(optimal_execution_window);
        """
    )
    
    # Commit the table creation
    cur.connection.commit()
    
    print("[Consumer] ✓ Tables 'market_events' and 'spread_metrics' ready with indices.")


def consume_market_events():
    """Consumer for raw market events - also computes spread metrics"""
    
    # Windowing state
    windows = defaultdict(deque)
    window_size = 60  # seconds
    last_emit = defaultdict(float)
    emit_interval = 10  # emit every 10 seconds
    
    try:
        print("[Events Consumer] Connecting to Kafka at kafka:9092...")
        
        # Retry connection to Kafka
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                consumer = KafkaConsumer(
                    "market_events",
                    bootstrap_servers="kafka:9092",
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    group_id="market-events-consumer-group",
                )
                print("[Events Consumer] ✓ Connected to Kafka successfully!")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"[Events Consumer] ⏳ Waiting for Kafka... (attempt {attempt + 1}/{max_retries})")
                    import time
                    time.sleep(retry_delay)
                else:
                    raise

        print("[Events Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="postgres",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Events Consumer] ✓ Connected to PostgreSQL successfully!")

        # Create tables
        create_tables(cur)
        
        print("[Events Consumer] 🎧 Listening for market events...\n")

        message_count = 0
        total_notional = 0.0
        
        for message in consumer:
            try:
                event_data = message.value

                insert_query = """
                    INSERT INTO market_events (
                        event_id, timestamp, event_type, symbol, side, 
                        price, quantity, order_id, venue, participant_type, 
                        urgency_score, notional_value,
                        bid_price, ask_price, mid_price, 
                        quoted_spread_bps, effective_spread_bps, market_stress_level
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING;
                """
                
                cur.execute(
                    insert_query,
                    (
                        event_data["event_id"],
                        event_data["timestamp"],
                        event_data["event_type"],
                        event_data["symbol"],
                        event_data["side"],
                        event_data["price"],
                        event_data["quantity"],
                        event_data["order_id"],
                        event_data["venue"],
                        event_data["participant_type"],
                        event_data["urgency_score"],
                        event_data["notional_value"],
                        event_data["bid_price"],
                        event_data["ask_price"],
                        event_data["mid_price"],
                        event_data["quoted_spread_bps"],
                        event_data["effective_spread_bps"],
                        event_data["market_stress_level"],
                    ),
                )
                
                message_count += 1
                total_notional += event_data["notional_value"]
                
                # Add to windowing state
                symbol = event_data['symbol']
                timestamp = datetime.fromisoformat(event_data['timestamp'])
                windows[symbol].append({'timestamp': timestamp, 'event': event_data})
                
                # Remove old events
                cutoff = timestamp.timestamp() - window_size
                while windows[symbol] and windows[symbol][0]['timestamp'].timestamp() < cutoff:
                    windows[symbol].popleft()
                
                # Emit metrics if interval elapsed
                import time
                current_time = time.time()
                if current_time - last_emit[symbol] >= emit_interval and len(windows[symbol]) > 0:
                    metrics = compute_window_metrics(windows[symbol], symbol, cur)
                    last_emit[symbol] = current_time
                
                # Compact output
                if message_count % 10 == 0:  # Print every 10th event
                    print(f"[Events] ✓ #{message_count:04d} | "
                          f"Spread: {event_data['quoted_spread_bps']:>5.2f}bps | "
                          f"Stress: {event_data['market_stress_level']:.2f} | "
                          f"Total: ${total_notional:>12,.2f}")

            except Exception as e:
                print(f"[Events Consumer ERROR] Failed to process message: {e}")
                import traceback
                traceback.print_exc()
                continue

    except Exception as e:
        print(f"[Events Consumer ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


def compute_window_metrics(window, symbol, cur):
    """Compute windowed metrics and insert to DB"""
    events = [item['event'] for item in window]
    
    quoted_spreads = [e['quoted_spread_bps'] for e in events]
    effective_spreads = [e['effective_spread_bps'] for e in events if e['event_type'] == 'execution']
    
    buy_volume = sum(e['quantity'] for e in events if e['side'] == 'buy')
    sell_volume = sum(e['quantity'] for e in events if e['side'] == 'sell')
    total_volume = buy_volume + sell_volume
    
    limit_orders = len([e for e in events if e['event_type'] == 'limit_order'])
    market_orders = len([e for e in events if e['event_type'] == 'market_order'])
    executions = len([e for e in events if e['event_type'] == 'execution'])
    cancels = len([e for e in events if e['event_type'] == 'cancel'])
    
    avg_quoted_spread = statistics.mean(quoted_spreads) if quoted_spreads else 0
    std_quoted_spread = statistics.stdev(quoted_spreads) if len(quoted_spreads) > 1 else 0
    min_quoted_spread = min(quoted_spreads) if quoted_spreads else 0
    max_quoted_spread = max(quoted_spreads) if quoted_spreads else 0
    avg_effective_spread = statistics.mean(effective_spreads) if effective_spreads else 0
    
    order_imbalance_ratio = buy_volume / total_volume if total_volume > 0 else 0.5
    quote_to_trade_ratio = (limit_orders + cancels) / executions if executions > 0 else 0
    avg_stress = statistics.mean([e['market_stress_level'] for e in events])
    
    latest = events[-1]
    liquidity_score = 1000 / avg_quoted_spread if avg_quoted_spread > 0 else 0
    spread_anomaly = max_quoted_spread > (2 * avg_quoted_spread) if avg_quoted_spread > 0 else False
    is_optimal = avg_quoted_spread < 5.0 and std_quoted_spread < 2.0 and 0.4 < order_imbalance_ratio < 0.6
    
    # Insert to database
    insert_query = """
        INSERT INTO spread_metrics (
            timestamp, symbol, window_events_count,
            avg_quoted_spread_bps, std_quoted_spread_bps, 
            min_quoted_spread_bps, max_quoted_spread_bps, 
            avg_effective_spread_bps,
            total_volume, buy_volume, sell_volume, order_imbalance_ratio,
            limit_orders, market_orders, executions, cancels, quote_to_trade_ratio,
            avg_market_stress, current_bid, current_ask, current_mid,
            liquidity_score, spread_anomaly, optimal_execution_window
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    cur.execute(insert_query, (
        datetime.now(), symbol, len(events),
        round(avg_quoted_spread, 3), round(std_quoted_spread, 3),
        round(min_quoted_spread, 3), round(max_quoted_spread, 3),
        round(avg_effective_spread, 3),
        total_volume, buy_volume, sell_volume, round(order_imbalance_ratio, 3),
        limit_orders, market_orders, executions, cancels, round(quote_to_trade_ratio, 3),
        round(avg_stress, 3), latest['bid_price'], latest['ask_price'], latest['mid_price'],
        round(liquidity_score, 2), spread_anomaly, is_optimal
    ))
    
    alert = "⚠️ ANOMALY" if spread_anomaly else "✅ NORMAL"
    optimal = "🎯 OPTIMAL" if is_optimal else ""
    print(f"[Metrics] {symbol:6s} | Spread: {avg_quoted_spread:>6.2f}±{std_quoted_spread:>5.2f}bps | "
          f"Liquidity: {liquidity_score:>6.2f} | {alert} {optimal}")


if __name__ == "__main__":
    print("=" * 80)
    print("MARKET EVENTS CONSUMER WITH INLINE SPREAD ANALYTICS")
    print("=" * 80)
    consume_market_events()