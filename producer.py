import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import numpy as np

fake = Faker()

# S&P 500 stocks with realistic price ranges and spread characteristics
STOCK_UNIVERSE = {
    # High liquidity (tight spreads): 1-3 bps
    "AAPL": {"price_range": (170, 195), "base_spread_bps": 1.5, "volatility": 0.25},
    "MSFT": {"price_range": (360, 390), "base_spread_bps": 1.2, "volatility": 0.22},
    "GOOGL": {"price_range": (130, 145), "base_spread_bps": 2.0, "volatility": 0.28},
    "AMZN": {"price_range": (145, 165), "base_spread_bps": 1.8, "volatility": 0.32},
    
    # Medium liquidity (moderate spreads): 3-8 bps
    "NVDA": {"price_range": (480, 520), "base_spread_bps": 5.0, "volatility": 0.45},
    "TSLA": {"price_range": (210, 250), "base_spread_bps": 8.0, "volatility": 0.60},
    "META": {"price_range": (320, 360), "base_spread_bps": 3.5, "volatility": 0.35},
    "JPM": {"price_range": (145, 165), "base_spread_bps": 3.0, "volatility": 0.20},
    
    # Lower liquidity (wider spreads): 8-15 bps
    "V": {"price_range": (255, 280), "base_spread_bps": 2.5, "volatility": 0.18},
    "WMT": {"price_range": (58, 65), "base_spread_bps": 4.0, "volatility": 0.15},
    "DIS": {"price_range": (90, 110), "base_spread_bps": 6.0, "volatility": 0.25},
    "NFLX": {"price_range": (450, 550), "base_spread_bps": 10.0, "volatility": 0.40},
}

VENUES = ["NYSE", "NASDAQ", "BATS", "IEX", "ARCA"]
EVENT_TYPES = ["limit_order", "market_order", "cancel", "execution"]
PARTICIPANT_TYPES = ["retail", "institutional", "hft", "market_maker"]

class MarketSimulator:
    """Simulates realistic stock prices and bid-ask spreads"""
    
    def __init__(self):
        # Initialize mid prices at midpoint of ranges
        self.mid_prices = {
            symbol: np.mean(config["price_range"]) 
            for symbol, config in STOCK_UNIVERSE.items()
        }
        
        # Initialize spreads at base levels
        self.current_spreads_bps = {
            symbol: config["base_spread_bps"]
            for symbol, config in STOCK_UNIVERSE.items()
        }
        
        # Market regime affects all spreads (stress = wider spreads)
        self.market_stress_level = 0.0  # 0 = calm, 1 = crisis
        
        # Track last update time
        self.last_update = {symbol: datetime.now() for symbol in STOCK_UNIVERSE.keys()}
        
    def update_market_stress(self):
        """
        Model market stress level using mean-reverting process
        High stress = wider spreads across all symbols
        """
        # Ornstein-Uhlenbeck process for stress
        dt = 1/252/390  # 1 minute
        mean_reversion_speed = 2.0
        stress_vol = 0.3
        
        mean_stress = 0.2  # Normal market has 20% stress
        
        self.market_stress_level += mean_reversion_speed * (mean_stress - self.market_stress_level) * dt
        self.market_stress_level += stress_vol * np.sqrt(dt) * np.random.normal(0, 1)
        
        # Keep stress between 0 and 1
        self.market_stress_level = np.clip(self.market_stress_level, 0.0, 1.0)
    
    def update_spread(self, symbol):
        """
        Update spread based on:
        1. Base spread (stock characteristic)
        2. Market stress (systemic factor)
        3. Random noise (microstructure)
        """
        config = STOCK_UNIVERSE[symbol]
        base_spread = config["base_spread_bps"]
        
        # Stress multiplier (1.0 to 5.0x base spread)
        stress_multiplier = 1.0 + 4.0 * self.market_stress_level
        
        # Random noise (±20% of base)
        noise = np.random.normal(1.0, 0.2)
        
        # Calculate new spread
        new_spread = base_spread * stress_multiplier * noise
        
        # Smooth update (don't jump too much)
        alpha = 0.3  # smoothing factor
        self.current_spreads_bps[symbol] = (
            alpha * new_spread + (1 - alpha) * self.current_spreads_bps[symbol]
        )
        
        # Ensure minimum spread (0.5 bps)
        self.current_spreads_bps[symbol] = max(self.current_spreads_bps[symbol], 0.5)
        
        return self.current_spreads_bps[symbol]
    
    def update_price(self, symbol):
        """
        Update mid price using Geometric Brownian Motion
        """
        config = STOCK_UNIVERSE[symbol]
        dt = 1 / (252 * 390)  # 1 minute in trading year
        
        drift = 0.0
        vol = config["volatility"]
        
        shock = np.random.normal(0, 1)
        price_change = self.mid_prices[symbol] * (drift * dt + vol * np.sqrt(dt) * shock)
        
        self.mid_prices[symbol] += price_change
        
        # Keep within bounds
        min_price, max_price = config["price_range"]
        self.mid_prices[symbol] = np.clip(self.mid_prices[symbol], min_price, max_price)
        
        self.last_update[symbol] = datetime.now()
        
        return round(self.mid_prices[symbol], 2)
    
    def get_bid_ask(self, symbol):
        """
        Calculate bid and ask prices from mid and spread
        """
        mid_price = self.mid_prices[symbol]
        spread_bps = self.current_spreads_bps[symbol]
        
        # Convert basis points to dollars
        half_spread = (spread_bps / 10000) * mid_price / 2
        
        bid = round(mid_price - half_spread, 2)
        ask = round(mid_price + half_spread, 2)
        
        return bid, ask, mid_price

# Initialize market simulator
simulator = MarketSimulator()

def generate_market_event():
    """Generate realistic market event with bid-ask spreads"""
    
    # Update market stress periodically
    if random.random() < 0.1:  # 10% chance per event
        simulator.update_market_stress()
    
    # Select random symbol
    symbol = random.choice(list(STOCK_UNIVERSE.keys()))
    
    # Update price and spread
    mid_price = simulator.update_price(symbol)
    spread_bps = simulator.update_spread(symbol)
    bid, ask, mid = simulator.get_bid_ask(symbol)
    
    # Event type distribution
    event_type = random.choices(
        EVENT_TYPES, 
        weights=[0.45, 0.25, 0.15, 0.15]
    )[0]
    
    # Side
    side = random.choices(["buy", "sell"], weights=[0.52, 0.48])[0]
    
    # Venue
    venue = random.choice(VENUES)
    
    # Participant type
    participant = random.choices(
        PARTICIPANT_TYPES,
        weights=[0.40, 0.35, 0.15, 0.10]
    )[0]
    
    # Price logic based on order type and side
    if event_type == "limit_order":
        if side == "buy":
            # Buy limit orders at or below bid
            price = round(bid - random.uniform(0, 0.10), 2)
        else:
            # Sell limit orders at or above ask
            price = round(ask + random.uniform(0, 0.10), 2)
    elif event_type == "market_order":
        # Market orders cross spread
        price = ask if side == "buy" else bid
    elif event_type == "execution":
        # Executions happen within spread
        price = round(random.uniform(bid, ask), 2)
    else:  # cancel
        # Cancels reference limit prices
        if side == "buy":
            price = round(bid - random.uniform(0, 0.05), 2)
        else:
            price = round(ask + random.uniform(0, 0.05), 2)
    
    # Ensure price is positive
    price = max(price, 0.01)
    
    # Quantity based on participant type
    if participant == "retail":
        quantity = random.choice([1, 5, 10, 25, 50, 100, 200])
    elif participant == "hft":
        quantity = random.choice([100, 200, 500, 1000, 2000])
    elif participant == "market_maker":
        quantity = random.choice([500, 1000, 2000, 5000])
    else:  # institutional
        quantity = random.choice([1000, 2500, 5000, 10000, 25000])
    
    # Urgency score
    if event_type == "market_order":
        urgency = random.uniform(0.6, 1.0)
    elif participant == "hft":
        urgency = random.uniform(0.5, 0.9)
    else:
        urgency = random.uniform(0.1, 0.5)
    
    # Calculate notional value
    notional_value = price * quantity
    
    # Calculate effective spread for executions
    if event_type == "execution":
        if side == "buy":
            effective_spread_bps = ((price - mid) / mid) * 10000
        else:
            effective_spread_bps = ((mid - price) / mid) * 10000
    else:
        effective_spread_bps = 0.0
    
    return {
        "event_id": str(uuid.uuid4())[:12],
        "timestamp": datetime.now().isoformat(),
        "event_type": event_type,
        "symbol": symbol,
        "side": side,
        "price": price,
        "quantity": quantity,
        "order_id": f"ORD{random.randint(100000, 999999)}",
        "venue": venue,
        "participant_type": participant,
        "urgency_score": round(urgency, 3),
        "notional_value": round(notional_value, 2),
        
        # NEW: Spread-related fields
        "bid_price": bid,
        "ask_price": ask,
        "mid_price": round(mid, 2),
        "quoted_spread_bps": round(spread_bps, 2),
        "effective_spread_bps": round(effective_spread_bps, 2),
        "market_stress_level": round(simulator.market_stress_level, 3)
    }

def run_producer():
    """Kafka producer that sends market events with spread data"""
    try:
        print("[Market Feed] Connecting to Kafka at kafka:9092...")
        
        # Retry connection to Kafka
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers="kafka:9092",
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    request_timeout_ms=30000,
                    max_block_ms=60000,
                    retries=5,
                )
                print("[Market Feed] ✓ Connected to Kafka successfully!")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"[Market Feed] ⏳ Waiting for Kafka... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    raise
        
        print("[Market Feed] 📡 Starting real-time order flow with spread data...\n")

        count = 0
        while True:
            event = generate_market_event()
            
            # Format output
            side_emoji = "🟢" if event['side'] == "buy" else "🔴"
            spread_indicator = "📏" if event['quoted_spread_bps'] > 5 else "📐"
            
            print(f"[Event #{count:04d}] {side_emoji} {event['symbol']:6s} "
                  f"{event['side'].upper():4s} {event['quantity']:>6,d}@${event['price']:>7.2f} "
                  f"| {event['event_type']:12s} | {event['venue']:6s} "
                  f"| {spread_indicator} Spread: {event['quoted_spread_bps']:>5.2f}bps "
                  f"| Bid: ${event['bid_price']:>7.2f} Ask: ${event['ask_price']:>7.2f} "
                  f"| Stress: {event['market_stress_level']:.2f}")

            future = producer.send("market_events", value=event)
            record_metadata = future.get(timeout=10)
            
            producer.flush()
            count += 1

            # Realistic inter-arrival time
            sleep_time = random.uniform(0.05, 0.5)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Market Feed ERROR] {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    print("=" * 100)
    print("REAL-TIME MARKET MICROSTRUCTURE SIMULATOR WITH BID-ASK SPREADS")
    print("=" * 100)
    run_producer()