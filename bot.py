#!/usr/bin/env python3
"""
Crypto Arbitrage Detection Bot - Railway Compatible
No pandas/numpy required
"""

import asyncio
import json
import csv
import time
import logging
import aiohttp
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import os
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration - Environment variables"""
    
    # Load environment variables
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    
    # Telegram Configuration
    TELEGRAM_CONFIG = {
        'bot_token': TELEGRAM_BOT_TOKEN,
        'chat_id': TELEGRAM_CHAT_ID,
        'alert_threshold': float(os.getenv('ALERT_THRESHOLD', '0.5')),
        'cooldown_seconds': int(os.getenv('COOLDOWN_SECONDS', '60')),
        'send_summary': os.getenv('SEND_SUMMARY', 'True').lower() == 'true',
        'summary_interval': int(os.getenv('SUMMARY_INTERVAL', '10')),
        'enabled': bool(TELEGRAM_BOT_TOKEN)
    }
    
    # CEX Configuration - Public endpoints only
    CEX_ENDPOINTS = {
        'binance': 'https://api.binance.com/api/v3/ticker/bookTicker',
        'bybit': 'https://api.bybit.com/v5/market/tickers?category=spot',
        'mexc': 'https://api.mexc.com/api/v3/ticker/bookTicker',
        'gateio': 'https://api.gateio.ws/api/v4/spot/tickers',
        'kucoin': 'https://api.kucoin.com/api/v1/market/allTickers',
    }
    
    # Scanner Configuration
    SCANNER_CONFIG = {
        'scan_interval': int(os.getenv('SCAN_INTERVAL', '20')),
        'simulation_balance': float(os.getenv('SIMULATION_BALANCE', '1000')),
        'min_liquidity': float(os.getenv('MIN_LIQUIDITY', '10000')),
        'max_spread_percentage': float(os.getenv('MAX_SPREAD_PERCENTAGE', '5')),
        'max_concurrent_requests': int(os.getenv('MAX_CONCURRENT_REQUESTS', '5')),
        'timeout_seconds': int(os.getenv('TIMEOUT_SECONDS', '10'))
    }
    
    # Profit Calculation
    PROFIT_CONFIG = {
        'taker_fee_percentage': 0.1,
        'maker_fee_percentage': 0.1,
        'dex_fee_percentage': 0.3,
        'withdrawal_fee_usd': 5,
        'min_profit_usd': 1,
        'slippage_percentage': 0.5
    }

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class MarketData:
    """Market data structure"""
    exchange: str
    symbol: str
    base: str
    quote: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    timestamp: int
    
    @property
    def spread_percentage(self):
        if self.ask > 0:
            return ((self.ask - self.bid) / self.ask) * 100
        return 0

# ============================================================================
# CEX SCANNER
# ============================================================================

class CEXScanner:
    """CEX Scanner using public endpoints only"""
    
    def __init__(self):
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_exchange(self, exchange: str, url: str) -> Dict[str, MarketData]:
        """Fetch data from a single exchange"""
        markets = {}
        
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if exchange == 'binance':
                        for ticker in data:
                            symbol = ticker['symbol']
                            if symbol.endswith('USDT'):
                                base = symbol.replace('USDT', '')
                                markets[f"{base}/USDT"] = MarketData(
                                    exchange='binance',
                                    symbol=f"{base}/USDT",
                                    base=base,
                                    quote='USDT',
                                    bid=float(ticker['bidPrice']),
                                    ask=float(ticker['askPrice']),
                                    bid_size=float(ticker['bidQty']),
                                    ask_size=float(ticker['askQty']),
                                    timestamp=int(time.time() * 1000)
                                )
                    
                    elif exchange == 'bybit':
                        for ticker in data['result']['list']:
                            symbol = ticker['symbol']
                            if symbol.endswith('USDT'):
                                base = symbol.replace('USDT', '')
                                markets[f"{base}/USDT"] = MarketData(
                                    exchange='bybit',
                                    symbol=f"{base}/USDT",
                                    base=base,
                                    quote='USDT',
                                    bid=float(ticker['bid1Price']),
                                    ask=float(ticker['ask1Price']),
                                    bid_size=float(ticker['bid1Size']),
                                    ask_size=float(ticker['ask1Size']),
                                    timestamp=int(time.time() * 1000)
                                )
                    
                    elif exchange == 'mexc':
                        for ticker in data:
                            symbol = ticker['symbol']
                            if symbol.endswith('_USDT'):
                                base = symbol.replace('_USDT', '')
                                markets[f"{base}/USDT"] = MarketData(
                                    exchange='mexc',
                                    symbol=f"{base}/USDT",
                                    base=base,
                                    quote='USDT',
                                    bid=float(ticker['bidPrice']),
                                    ask=float(ticker['askPrice']),
                                    bid_size=float(ticker['bidSize']),
                                    ask_size=float(ticker['askSize']),
                                    timestamp=int(time.time() * 1000)
                                )
                    
                    elif exchange == 'gateio':
                        for ticker in data:
                            currency_pair = ticker['currency_pair']
                            if currency_pair.endswith('_USDT'):
                                base = currency_pair.replace('_USDT', '')
                                markets[f"{base}/USDT"] = MarketData(
                                    exchange='gateio',
                                    symbol=f"{base}/USDT",
                                    base=base,
                                    quote='USDT',
                                    bid=float(ticker['lowest_ask']),
                                    ask=float(ticker['highest_bid']),
                                    bid_size=0,
                                    ask_size=0,
                                    timestamp=int(time.time() * 1000)
                                )
                    
                    elif exchange == 'kucoin':
                        for ticker in data['data']['ticker']:
                            symbol = ticker['symbol']
                            if symbol.endswith('-USDT'):
                                base = symbol.replace('-USDT', '')
                                markets[f"{base}/USDT"] = MarketData(
                                    exchange='kucoin',
                                    symbol=f"{base}/USDT",
                                    base=base,
                                    quote='USDT',
                                    bid=float(ticker['buy']),
                                    ask=float(ticker['sell']),
                                    bid_size=0,
                                    ask_size=0,
                                    timestamp=int(time.time() * 1000)
                                )
        
        except Exception as e:
            logging.error(f"Error fetching {exchange}: {e}")
        
        return markets
    
    async def fetch_all_exchanges(self) -> Dict[str, Dict[str, MarketData]]:
        """Fetch data from all exchanges concurrently"""
        tasks = []
        for exchange, url in Config.CEX_ENDPOINTS.items():
            tasks.append(self.fetch_exchange(exchange, url))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_markets = {}
        exchanges = list(Config.CEX_ENDPOINTS.keys())
        
        for i, result in enumerate(results):
            exchange = exchanges[i]
            if not isinstance(result, Exception) and result:
                all_markets[exchange] = result
        
        logging.info(f"Fetched data from {len(all_markets)} exchanges")
        return all_markets
    
    def find_cex_arbitrage(self, all_markets: Dict[str, Dict[str, MarketData]]) -> List[Dict]:
        """Find arbitrage between exchanges"""
        opportunities = []
        
        # Create symbol map across exchanges
        symbol_map = {}
        for exchange, markets in all_markets.items():
            for symbol, market in markets.items():
                if symbol not in symbol_map:
                    symbol_map[symbol] = []
                symbol_map[symbol].append((exchange, market))
        
        # Check each symbol for arbitrage
        for symbol, markets in symbol_map.items():
            if len(markets) < 2:
                continue
            
            # Find best bid and ask
            best_bid = max(markets, key=lambda x: x[1].bid)
            best_ask = min(markets, key=lambda x: x[1].ask)
            
            if best_bid[0] == best_ask[0]:
                continue
            
            buy_price = best_ask[1].ask
            sell_price = best_bid[1].bid
            
            if buy_price <= 0 or sell_price <= 0:
                continue
            
            # Calculate profit
            profit_before_fees = (sell_price - buy_price) / buy_price * 100
            fees = Config.PROFIT_CONFIG['taker_fee_percentage'] * 2
            profit_after_fees = profit_before_fees - fees
            
            if profit_after_fees > Config.TELEGRAM_CONFIG['alert_threshold']:
                # Calculate USD profit
                amount = Config.SCANNER_CONFIG['simulation_balance']
                tokens = amount / buy_price
                revenue = tokens * sell_price
                fees_usd = amount * Config.PROFIT_CONFIG['taker_fee_percentage'] / 100
                fees_usd += revenue * Config.PROFIT_CONFIG['taker_fee_percentage'] / 100
                profit_usd = revenue - amount - fees_usd
                
                if profit_usd > Config.PROFIT_CONFIG['min_profit_usd']:
                    opportunity = {
                        'type': 'CEX_ARBITRAGE',
                        'symbol': symbol,
                        'buy_exchange': best_ask[0],
                        'sell_exchange': best_bid[0],
                        'buy_price': round(buy_price, 6),
                        'sell_price': round(sell_price, 6),
                        'profit_percentage': round(profit_after_fees, 4),
                        'profit_usd': round(profit_usd, 2),
                        'volume': min(best_ask[1].ask_size, best_bid[1].bid_size),
                        'timestamp': datetime.now().isoformat()
                    }
                    opportunities.append(opportunity)
        
        return sorted(opportunities, key=lambda x: x['profit_percentage'], reverse=True)

# ============================================================================
# TELEGRAM ALERTER
# ============================================================================

class TelegramAlerter:
    """Telegram alerts"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.bot = None
        self.alerted = set()
        self.scans_since_summary = 0
        
        if not config.get('enabled', True):
            logging.info("Telegram alerts disabled")
            return
            
        if not config.get('bot_token') or not config.get('chat_id'):
            logging.warning("Telegram credentials not configured")
            return
        
        try:
            from telegram import Bot
            self.bot = Bot(token=config['bot_token'])
            logging.info("Telegram bot initialized")
            asyncio.create_task(self.send_startup_message())
        except ImportError:
            logging.warning("python-telegram-bot not installed")
        except Exception as e:
            logging.error(f"Telegram setup failed: {e}")
    
    async def send_startup_message(self):
        """Send startup message"""
        try:
            if self.bot:
                message = f"""
🤖 <b>Arbitrage Bot Started on Railway!</b>

⚙️ <b>Configuration:</b>
   • Scan interval: {Config.SCANNER_CONFIG['scan_interval']}s
   • Min profit: {self.config['alert_threshold']}%
   • Sim balance: ${Config.SCANNER_CONFIG['simulation_balance']}
   
📡 <b>Monitoring CEXs:</b>
   • Binance, Bybit, MEXC, Gate.io, KuCoin
   
✅ Bot is now running!
"""
                await self.bot.send_message(
                    chat_id=self.config['chat_id'],
                    text=message,
                    parse_mode='HTML'
                )
        except:
            pass
    
    async def send_alert(self, opportunity: Dict) -> bool:
        """Send alert"""
        if not self.bot:
            return False
        
        opp_id = f"{opportunity['type']}_{opportunity.get('symbol', '')}_{datetime.now().strftime('%H')}"
        
        if opp_id in self.alerted:
            return False
        
        try:
            message = self.format_message(opportunity)
            await self.bot.send_message(
                chat_id=self.config['chat_id'],
                text=message,
                parse_mode='HTML'
            )
            self.alerted.add(opp_id)
            logging.info(f"Telegram alert sent")
            return True
        except Exception as e:
            logging.error(f"Failed to send alert: {e}")
            return False
    
    def format_message(self, opportunity: Dict) -> str:
        """Format alert message"""
        if opportunity['type'] == 'CEX_ARBITRAGE':
            return f"""
🚀 <b>ARBITRAGE FOUND!</b>

📊 <b>Pair:</b> {opportunity['symbol']}
🔼 <b>Buy on:</b> {opportunity['buy_exchange'].upper()}
💵 <b>Price:</b> ${opportunity['buy_price']}
🔽 <b>Sell on:</b> {opportunity['sell_exchange'].upper()}
💰 <b>Price:</b> ${opportunity['sell_price']}
📈 <b>Profit:</b> {opportunity['profit_percentage']:.2f}%
💸 <b>USD Profit:</b> ${opportunity['profit_usd']:.2f}
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
"""
        return ""
    
    async def send_summary(self, opportunities: List[Dict], scan_duration: float):
        """Send scan summary"""
        if not self.bot:
            return
        
        self.scans_since_summary += 1
        
        if self.scans_since_summary < self.config.get('summary_interval', 10):
            return
        
        try:
            if not opportunities:
                message = f"""
📊 <b>Scan Summary</b>

No opportunities found in {self.scans_since_summary} scans.
⏱️ <b>Last scan:</b> {scan_duration:.1f}s
🔄 <b>Next scan in:</b> {Config.SCANNER_CONFIG['scan_interval']}s
"""
            else:
                message = f"""
📊 <b>Scan Summary</b>

🎯 <b>Opportunities found:</b> {len(opportunities)}
🏆 <b>Top profit:</b> {opportunities[0]['profit_percentage']:.2f}%
⏱️ <b>Scan duration:</b> {scan_duration:.1f}s
🔄 <b>Next scan in:</b> {Config.SCANNER_CONFIG['scan_interval']}s
"""
            
            await self.bot.send_message(
                chat_id=self.config['chat_id'],
                text=message,
                parse_mode='HTML'
            )
            
            self.scans_since_summary = 0
            
        except Exception as e:
            logging.error(f"Failed to send summary: {e}")

# ============================================================================
# DATA LOGGER (No pandas)
# ============================================================================

class DataLogger:
    """Log opportunities without pandas"""
    
    def __init__(self, log_dir: str = "arb_logs"):
        self.log_dir = log_dir
        self.csv_file = os.path.join(log_dir, "opportunities.csv")
        
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Create CSV with headers
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'type', 'symbol', 'buy_exchange', 'sell_exchange',
                    'buy_price', 'sell_price', 'profit_percentage', 'profit_usd',
                    'details'
                ])
    
    def log_opportunity(self, opportunity: Dict):
        """Log opportunity to CSV"""
        try:
            with open(self.csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    opportunity['timestamp'],
                    opportunity['type'],
                    opportunity.get('symbol', ''),
                    opportunity.get('buy_exchange', ''),
                    opportunity.get('sell_exchange', ''),
                    opportunity.get('buy_price', 0),
                    opportunity.get('sell_price', 0),
                    opportunity.get('profit_percentage', 0),
                    opportunity.get('profit_usd', 0),
                    json.dumps(opportunity)
                ])
            
            logging.info(f"Logged {opportunity['type']}: {opportunity.get('profit_percentage', 0):.2f}%")
        except Exception as e:
            logging.error(f"Failed to log: {e}")
    
    def get_recent_opportunities(self, hours: int = 1) -> List[Dict]:
        """Get recent opportunities from CSV"""
        opportunities = []
        
        try:
            with open(self.csv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        # Parse timestamp
                        row_time = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
                        cutoff = datetime.now() - timedelta(hours=hours)
                        
                        if row_time >= cutoff:
                            # Parse details JSON
                            details = json.loads(row['details'])
                            opportunities.append(details)
                    except:
                        continue
        except FileNotFoundError:
            pass
        
        return opportunities

# ============================================================================
# MAIN ARBITRAGE BOT
# ============================================================================

class ArbitrageBot:
    """Main arbitrage detection bot"""
    
    def __init__(self):
        self.running = False
        self.scan_count = 0
        self.total_opportunities = 0
        
        self.alerter = TelegramAlerter(Config.TELEGRAM_CONFIG)
        self.logger = DataLogger()
        
        logging.info("Arbitrage Bot initialized")
    
    async def run_scan(self):
        """Run a single scan"""
        start_time = time.time()
        
        try:
            async with CEXScanner() as scanner:
                # Fetch data from exchanges
                logging.info(f"Scan #{self.scan_count + 1}: Fetching CEX data...")
                cex_data = await scanner.fetch_all_exchanges()
                
                # Find arbitrage opportunities
                opportunities = scanner.find_cex_arbitrage(cex_data)
                
                scan_duration = time.time() - start_time
                
                if opportunities:
                    # Log and alert top opportunities
                    for opp in opportunities[:3]:  # Top 3
                        self.logger.log_opportunity(opp)
                        await self.alerter.send_alert(opp)
                    
                    self.total_opportunities += len(opportunities)
                    
                    # Print results
                    self.print_results(opportunities, scan_duration)
                    
                    # Send summary
                    await self.alerter.send_summary(opportunities, scan_duration)
                    
                else:
                    logging.info(f"Scan complete in {scan_duration:.1f}s - No opportunities")
                    await self.alerter.send_summary([], scan_duration)
                
                return opportunities
                
        except Exception as e:
            logging.error(f"Scan failed: {e}")
            return []
    
    def print_results(self, opportunities: List[Dict], duration: float):
        """Print results to console"""
        print(f"\n{'='*60}")
        print(f"📊 Scan #{self.scan_count + 1} completed in {duration:.1f}s")
        print(f"📈 Found {len(opportunities)} opportunities")
        
        if opportunities:
            print(f"\n🏆 TOP OPPORTUNITIES:")
            for i, opp in enumerate(opportunities[:3], 1):
                print(f"{i}. {opp['symbol']}")
                print(f"   Buy: {opp['buy_exchange']} @ ${opp['buy_price']}")
                print(f"   Sell: {opp['sell_exchange']} @ ${opp['sell_price']}")
                print(f"   Profit: {opp['profit_percentage']:.2f}% (${opp['profit_usd']:.2f})")
                print()
        
        print(f"{'='*60}")
    
    async def run(self):
        """Main loop"""
        self.running = True
        
        print(f"""
╔══════════════════════════════════════════════════════════╗
║       CRYPTO ARBITRAGE BOT - RAILWAY EDITION            ║
║              No API Keys Required                        ║
╚══════════════════════════════════════════════════════════╝
        """)
        
        print(f"⚙️  Configuration:")
        print(f"   Scan interval: {Config.SCANNER_CONFIG['scan_interval']}s")
        print(f"   Min profit: {Config.TELEGRAM_CONFIG['alert_threshold']}%")
        print(f"   Telegram: {'✅ ENABLED' if Config.TELEGRAM_CONFIG['enabled'] else '❌ DISABLED'}")
        print(f"\n📡 Scanning: Binance, Bybit, MEXC, Gate.io, KuCoin")
        print(f"\n{'='*60}")
        print("🚀 Bot started! Press Ctrl+C to stop.")
        print(f"{'='*60}\n")
        
        try:
            while self.running:
                self.scan_count += 1
                await self.run_scan()
                
                # Wait for next scan
                await asyncio.sleep(Config.SCANNER_CONFIG['scan_interval'])
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping bot...")
        except Exception as e:
            logging.error(f"Bot error: {e}")
        finally:
            self.running = False
            await self.shutdown()
    
    async def shutdown(self):
        """Shutdown bot"""
        print(f"\n{'='*60}")
        print("📊 FINAL STATISTICS")
        print(f"   Total scans: {self.scan_count}")
        print(f"   Total opportunities: {self.total_opportunities}")
        print(f"   Logs saved to: {self.logger.log_dir}")
        print(f"{'='*60}")
        print("✅ Bot stopped successfully!")

# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main entry point"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Check environment variables
    if not Config.TELEGRAM_BOT_TOKEN or not Config.TELEGRAM_CHAT_ID:
        print(f"\n{'='*60}")
        print("⚠️  TELEGRAM CREDENTIALS NOT SET")
        print(f"{'='*60}")
        print("Set these environment variables in Railway:")
        print("   TELEGRAM_BOT_TOKEN=your_bot_token")
        print("   TELEGRAM_CHAT_ID=your_chat_id")
        print(f"\nThe bot will run but Telegram alerts won't work.")
        print(f"{'='*60}")
    
    # Check requirements
    try:
        import aiohttp
    except ImportError:
        print("❌ aiohttp not installed")
        print("💡 Install with: pip install aiohttp")
        return
    
    try:
        import telegram
    except ImportError:
        print("⚠️  python-telegram-bot not installed")
        print("💡 Install with: pip install python-telegram-bot")
        print("⚠️  Telegram alerts will not work")
    
    # Run bot
    bot = ArbitrageBot()
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")

if __name__ == "__main__":
    main()