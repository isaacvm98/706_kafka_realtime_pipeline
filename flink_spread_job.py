"""
Apache Flink Stream Processing Job
Real-Time Spread Analytics with Windowed Aggregations

This job consumes market events from Kafka, computes windowed spread metrics,
and produces aggregated features for spread management and anomaly detection.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaRecordSerializationSchema, KafkaSink, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration
import json
from datetime import datetime
import statistics

class SpreadMetricsAggregator(KeyedProcessFunction):
    """
    Windowed aggregation of spread metrics per symbol
    Computes 1-minute tumbling window statistics
    """
    
    def __init__(self, window_size_seconds=60):
        self.window_size_ms = window_size_seconds * 1000
        self.window_data_descriptor = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state for windowed data"""
        self.window_data_descriptor = ValueStateDescriptor(
            "window_data",
            Types.PICKLED_BYTE_ARRAY()
        )
        
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        """Process each event and aggregate within windows"""
        
        # Get or initialize window state
        window_state = ctx.get_state(self.window_data_descriptor)
        current_data = window_state.value()
        
        if current_data is None:
            current_data = {
                'events': [],
                'window_start': ctx.timestamp()
            }
        
        # Add current event to window
        current_data['events'].append({
            'timestamp': value['timestamp'],
            'symbol': value['symbol'],
            'event_type': value['event_type'],
            'quoted_spread_bps': value['quoted_spread_bps'],
            'effective_spread_bps': value['effective_spread_bps'],
            'market_stress_level': value['market_stress_level'],
            'side': value['side'],
            'quantity': value['quantity'],
            'notional_value': value['notional_value'],
            'bid_price': value['bid_price'],
            'ask_price': value['ask_price'],
            'mid_price': value['mid_price']
        })
        
        # Update state
        window_state.update(current_data)
        
        # Check if window should close
        window_end = current_data['window_start'] + self.window_size_ms
        
        if ctx.timestamp() >= window_end:
            # Compute window aggregations
            metrics = self.compute_window_metrics(current_data['events'], value['symbol'])
            
            # Emit aggregated metrics
            yield metrics
            
            # Reset window
            window_state.clear()
    
    def compute_window_metrics(self, events, symbol):
        """Compute aggregated metrics for the window"""
        
        if not events:
            return None
        
        # Extract spreads
        quoted_spreads = [e['quoted_spread_bps'] for e in events]
        effective_spreads = [e['effective_spread_bps'] for e in events if e['event_type'] == 'execution']
        
        # Order flow metrics
        buy_volume = sum(e['quantity'] for e in events if e['side'] == 'buy')
        sell_volume = sum(e['quantity'] for e in events if e['side'] == 'sell')
        total_volume = buy_volume + sell_volume
        
        # Event type counts
        limit_orders = len([e for e in events if e['event_type'] == 'limit_order'])
        market_orders = len([e for e in events if e['event_type'] == 'market_order'])
        executions = len([e for e in events if e['event_type'] == 'execution'])
        cancels = len([e for e in events if e['event_type'] == 'cancel'])
        
        # Calculate metrics
        avg_quoted_spread = statistics.mean(quoted_spreads) if quoted_spreads else 0
        std_quoted_spread = statistics.stdev(quoted_spreads) if len(quoted_spreads) > 1 else 0
        min_quoted_spread = min(quoted_spreads) if quoted_spreads else 0
        max_quoted_spread = max(quoted_spreads) if quoted_spreads else 0
        
        avg_effective_spread = statistics.mean(effective_spreads) if effective_spreads else 0
        
        order_imbalance_ratio = buy_volume / total_volume if total_volume > 0 else 0.5
        quote_to_trade_ratio = (limit_orders + cancels) / executions if executions > 0 else 0
        
        avg_stress = statistics.mean([e['market_stress_level'] for e in events])
        
        # Latest prices
        latest_event = events[-1]
        current_bid = latest_event['bid_price']
        current_ask = latest_event['ask_price']
        current_mid = latest_event['mid_price']
        
        # Liquidity score (inverse of spread)
        liquidity_score = 1000 / avg_quoted_spread if avg_quoted_spread > 0 else 0
        
        # Anomaly detection: spread > 2 * recent average
        spread_anomaly = max_quoted_spread > (2 * avg_quoted_spread) if avg_quoted_spread > 0 else False
        
        # Optimal execution signal
        is_optimal = (
            avg_quoted_spread < 5.0 and  # Tight spread
            std_quoted_spread < 2.0 and   # Low volatility
            order_imbalance_ratio > 0.4 and order_imbalance_ratio < 0.6  # Balanced flow
        )
        
        return {
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'window_events_count': len(events),
            
            # Spread metrics
            'avg_quoted_spread_bps': round(avg_quoted_spread, 3),
            'std_quoted_spread_bps': round(std_quoted_spread, 3),
            'min_quoted_spread_bps': round(min_quoted_spread, 3),
            'max_quoted_spread_bps': round(max_quoted_spread, 3),
            'avg_effective_spread_bps': round(avg_effective_spread, 3),
            
            # Volume metrics
            'total_volume': total_volume,
            'buy_volume': buy_volume,
            'sell_volume': sell_volume,
            'order_imbalance_ratio': round(order_imbalance_ratio, 3),
            
            # Event counts
            'limit_orders': limit_orders,
            'market_orders': market_orders,
            'executions': executions,
            'cancels': cancels,
            'quote_to_trade_ratio': round(quote_to_trade_ratio, 3),
            
            # Market conditions
            'avg_market_stress': round(avg_stress, 3),
            'current_bid': current_bid,
            'current_ask': current_ask,
            'current_mid': current_mid,
            
            # Derived signals
            'liquidity_score': round(liquidity_score, 2),
            'spread_anomaly': spread_anomaly,
            'optimal_execution_window': is_optimal
        }


class EventParser(MapFunction):
    """Parse JSON events from Kafka"""
    
    def map(self, value):
        try:
            return json.loads(value)
        except:
            return None


class MetricsSerializer(MapFunction):
    """Serialize metrics back to JSON for Kafka"""
    
    def map(self, value):
        if value is None:
            return None
        return json.dumps(value)


def create_flink_job():
    """
    Create and configure the Flink streaming job
    """
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Add Kafka connector JARs
    import sys
    jar_paths = []
    i = 0
    while i < len(sys.argv):
        if sys.argv[i] == '--jarfile' and i + 1 < len(sys.argv):
            jar_paths.append(sys.argv[i + 1])
            i += 2
        else:
            i += 1
    
    if jar_paths:
        for jar_path in jar_paths:
            env.add_jars(f"file://{jar_path}")
            print(f"[Flink] ✓ Added JAR: {jar_path}")
    
    # Set parallelism
    env.set_parallelism(2)
    
    # Enable checkpointing for fault tolerance (every 60 seconds)
    env.enable_checkpointing(60000)
    
    # Configure Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("market_events") \
        .set_group_id("flink-spread-analytics") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Create data stream from Kafka
    events_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
        "Kafka Source"
    )
    
    # Parse JSON events
    parsed_stream = events_stream.map(EventParser(), output_type=Types.PICKLED_BYTE_ARRAY())
    
    # Filter out None values
    filtered_stream = parsed_stream.filter(lambda x: x is not None)
    
    # Key by symbol for parallel processing
    keyed_stream = filtered_stream.key_by(lambda x: x['symbol'])
    
    # Apply windowed aggregation
    aggregated_stream = keyed_stream.process(
        SpreadMetricsAggregator(window_size_seconds=60),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )
    
    # Filter out None results
    valid_metrics = aggregated_stream.filter(lambda x: x is not None)
    
    # Print to console - the consumer will read from market_events and compute metrics
    valid_metrics.map(
        lambda x: f"[Flink Window] {x['symbol']:6s} | "
                 f"Avg Spread: {x['avg_quoted_spread_bps']:>6.2f}±{x['std_quoted_spread_bps']:>5.2f}bps | "
                 f"Liquidity: {x['liquidity_score']:>6.2f} | "
                 f"Events: {x['window_events_count']:>4d} | "
                 f"{'⚠️ ANOMALY' if x['spread_anomaly'] else '✅ NORMAL'} "
                 f"{'🎯 OPTIMAL' if x['optimal_execution_window'] else ''}",
        output_type=Types.STRING()
    ).print()
    
    return env


if __name__ == "__main__":
    print("=" * 80)
    print("APACHE FLINK - REAL-TIME SPREAD ANALYTICS")
    print("=" * 80)
    print("Windowed Aggregations: 1-minute tumbling windows")
    print("Input Topic: market_events")
    print("Output Topic: spread_metrics")
    print("=" * 80)
    
    # Create and execute job
    env = create_flink_job()
    
    # Execute the job
    env.execute("Real-Time Spread Management - Windowed Analytics")