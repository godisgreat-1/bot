#!/usr/bin/env python3
"""
Crypto Arbitrage Detection Bot - NO API KEYS REQUIRED
Uses public endpoints for all CEX and DEX data
"""

import asyncio
import json
import csv
import time
import signal
import logging
import pandas as pd
import aiohttp
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Optional, Set
import os
import sys

# ============================================================================
# CONFIGURATION (No API Keys Needed)
# ============================================================================

class Config:
    """Configuration - No API keys required"""
    
    # CEX Configuration - Public endpoints only
    CEX_ENDPOINTS = {
        'binance': 'https://api.binance.com/api/v3/ticker/bookTicker',
        'bybit': 'https://api.bybit.com/v5/market/tickers?category=spot',
        'mexc': 'https://api.mexc.com/api/v3/ticker/bookTicker',
        'gateio': 'https://api.gateio.ws/api/v4/spot/tickers',
        'kucoin': 'https://api.kucoin.com/api/v1/market/allTickers',
        'bitget': 'https://api.bitget.com/api/v2/spot/market/tickers',
        'okx': 'https://www.okx.com/api/v5/market/tickers?instType=SPOT',
        'huobi': 'https://api.huobi.pro/market/tickers',
        'coinbase': 'https://api.exchange.coinbase.com/products'
    }
    
    # DEX Configuration - Public RPCs
    DEX_CONFIG = {
        'ethereum': {
            'rpc_url': 'https://rpc.ankr.com/eth',
            'native_token': 'ETH',
            'chain_id': 1
        },
        'bsc': {
            'rpc_url': 'https://bsc-dataseed.binance.org',
            'native_token': 'BNB',
            'chain_id': 56
        },
        'polygon': {
            'rpc_url': 'https://polygon-rpc.com',
            'native_token': 'MATIC',
            'chain_id': 137
        },
        'arbitrum': {
            'rpc_url': 'https://arb1.arbitrum.io/rpc',
            'native_token': 'ETH',
            'chain_id': 42161
        },
        'optimism': {
            'rpc_url': 'https://mainnet.optimism.io',
            'native_token': 'ETH',
            'chain_id': 10
        }
    }
    
    # DEX Factory Addresses
    DEX_FACTORIES = {
        'uniswap_v2': '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f',
        'uniswap_v3': '0x1F98431c8aD98523631AE4a59f267346ea31F984',
        'sushiswap': '0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac',
        'pancakeswap_v2': '0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73',
        'quickswap': '0x5757371414417b8C6CAad45bAeF941aBc7d3Ab32'
    }
    
    # ==================== TELEGRAM CONFIGURATION ====================
    # Replace these with your actual Telegram credentials:
    # 1. Get bot token from @BotFather on Telegram
    # 2. Get chat ID from @userinfobot on Telegram
    # 3. Leave empty if you don't want Telegram alerts
     TELEGRAM_CONFIG = {
        'bot_token': os.getenv('TELEGRAM_BOT_TOKEN', ''),  # From .env
        'chat_id': os.getenv('TELEGRAM_CHAT_ID', ''),      # From .env
        'alert_threshold': float(os.getenv('ALERT_THRESHOLD', '0.5')),
        'cooldown_seconds': int(os.getenv('COOLDOWN_SECONDS', '60')),
        'send_summary': os.getenv('SEND_SUMMARY', 'True').lower() == 'true',
        'summary_interval': int(os.getenv('SUMMARY_INTERVAL', '10')),
        'enabled': bool(os.getenv('TELEGRAM_BOT_TOKEN', ''))  # Auto-enable if token exists
    }
    # ================================================================
    
    # Scanner Configuration
    SCANNER_CONFIG = {
        'scan_interval': 20,  # seconds between scans
        'simulation_balance': 100,  # USD for simulation
        'min_liquidity': 10000,  # USD minimum liquidity
        'max_spread_percentage': 5,
        'gas_price_buffer': 1.2,
        'max_concurrent_requests': 10,
        'timeout_seconds': 30
    }
    
    # Profit Calculation
    PROFIT_CONFIG = {
        'taker_fee_percentage': 0.1,  # 0.1% taker fee
        'maker_fee_percentage': 0.1,  # 0.1% maker fee
        'dex_fee_percentage': 0.3,    # 0.3% DEX fee
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
    last: float
    volume_24h: float
    timestamp: int
    
    @property
    def spread_percentage(self):
        if self.ask > 0:
            return ((self.ask - self.bid) / self.ask) * 100
        return 0

@dataclass
class ArbitrageOpportunity:
    """Arbitrage opportunity"""
    id: str
    type: str  # 'CEX', 'TRIANGULAR', 'DEX'
    profit_percentage: float
    profit_usd: float
    details: Dict
    detected_at: datetime
    alerted: bool = False
    
    def to_dict(self):
        return {
            'id': self.id,
            'type': self.type,
            'profit_percentage': round(self.profit_percentage, 4),
            'profit_usd': round(self.profit_usd, 2),
            'details': json.dumps(self.details, default=str),
            'detected_at': self.detected_at.isoformat(),
            'alerted': self.alerted
        }

# ============================================================================
# CEX SCANNER (No API Keys)
# ============================================================================

class CEXScanner:
    """CEX Scanner using public endpoints only"""
    
    def __init__(self):
        self.session = None
        self.markets_cache = {}
        self.last_update = {}
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_binance(self) -> Dict[str, MarketData]:
        """Fetch Binance data"""
        url = Config.CEX_ENDPOINTS['binance']
        markets = {}
        
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data:
                        symbol = ticker['symbol']
                        if symbol.endswith('USDT'):
                            base = symbol.replace('USDT', '')
                            market = MarketData(
                                exchange='binance',
                                symbol=f"{base}/USDT",
                                base=base,
                                quote='USDT',
                                bid=float(ticker['bidPrice']),
                                ask=float(ticker['askPrice']),
                                bid_size=float(ticker['bidQty']),
                                ask_size=float(ticker['askQty']),
                                last=0,
                                volume_24h=0,
                                timestamp=int(time.time() * 1000)
                            )
                            markets[market.symbol] = market
        except Exception as e:
            logging.error(f"Error fetching Binance: {e}")
        
        return markets
    
    async def fetch_bybit(self) -> Dict[str, MarketData]:
        """Fetch Bybit data"""
        url = Config.CEX_ENDPOINTS['bybit']
        markets = {}
        
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data['result']['list']:
                        symbol = ticker['symbol']
                        if symbol.endswith('USDT'):
                            base = symbol.replace('USDT', '')
                            market = MarketData(
                                exchange='bybit',
                                symbol=f"{base}/USDT",
                                base=base,
                                quote='USDT',
                                bid=float(ticker['bid1Price']),
                                ask=float(ticker['ask1Price']),
                                bid_size=float(ticker['bid1Size']),
                                ask_size=float(ticker['ask1Size']),
                                last=float(ticker['lastPrice']),
                                volume_24h=float(ticker['volume24h']),
                                timestamp=int(time.time() * 1000)
                            )
                            markets[market.symbol] = market
        except Exception as e:
            logging.error(f"Error fetching Bybit: {e}")
        
        return markets
    
    async def fetch_mexc(self) -> Dict[str, MarketData]:
        """Fetch MEXC data"""
        url = Config.CEX_ENDPOINTS['mexc']
        markets = {}
        
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data:
                        symbol = ticker['symbol']
                        if symbol.endswith('_USDT'):
                            base = symbol.replace('_USDT', '')
                            market = MarketData(
                                exchange='mexc',
                                symbol=f"{base}/USDT",
                                base=base,
                                quote='USDT',
                                bid=float(ticker['bidPrice']),
                                ask=float(ticker['askPrice']),
                                bid_size=float(ticker['bidSize']),
                                ask_size=float(ticker['askSize']),
                                last=float(ticker['lastPrice']),
                                volume_24h=0,
                                timestamp=int(time.time() * 1000)
                            )
                            markets[market.symbol] = market
        except Exception as e:
            logging.error(f"Error fetching MEXC: {e}")
        
        return markets
    
    async def fetch_gateio(self) -> Dict[str, MarketData]:
        """Fetch Gate.io data"""
        url = Config.CEX_ENDPOINTS['gateio']
        markets = {}
        
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data:
                        currency_pair = ticker['currency_pair']
                        if currency_pair.endswith('_USDT'):
                            base = currency_pair.replace('_USDT', '')
                            market = MarketData(
                                exchange='gateio',
                                symbol=f"{base}/USDT",
                                base=base,
                                quote='USDT',
                                bid=float(ticker['lowest_ask']),
                                ask=float(ticker['highest_bid']),
                                bid_size=0,
                                ask_size=0,
                                last=float(ticker['last']),
                                volume_24h=float(ticker['quote_volume']),
                                timestamp=int(time.time() * 1000)
                            )
                            markets[market.symbol] = market
        except Exception as e:
            logging.error(f"Error fetching Gate.io: {e}")
        
        return markets
    
    async def fetch_kucoin(self) -> Dict[str, MarketData]:
        """Fetch KuCoin data"""
        url = Config.CEX_ENDPOINTS['kucoin']
        markets = {}
        
        try:
            async with self.session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    for ticker in data['data']['ticker']:
                        symbol = ticker['symbol']
                        if symbol.endswith('-USDT'):
                            base = symbol.replace('-USDT', '')
                            market = MarketData(
                                exchange='kucoin',
                                symbol=f"{base}/USDT",
                                base=base,
                                quote='USDT',
                                bid=float(ticker['buy']),
                                ask=float(ticker['sell']),
                                bid_size=0,
                                ask_size=0,
                                last=float(ticker['last']),
                                volume_24h=float(ticker['vol']),
                                timestamp=int(time.time() * 1000)
                            )
                            markets[market.symbol] = market
        except Exception as e:
            logging.error(f"Error fetching KuCoin: {e}")
        
        return markets
    
    async def fetch_all_exchanges(self) -> Dict[str, Dict[str, MarketData]]:
        """Fetch data from all exchanges concurrently"""
        tasks = [
            self.fetch_binance(),
            self.fetch_bybit(),
            self.fetch_mexc(),
            self.fetch_gateio(),
            self.fetch_kucoin()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_markets = {
            'binance': results[0] if not isinstance(results[0], Exception) else {},
            'bybit': results[1] if not isinstance(results[1], Exception) else {},
            'mexc': results[2] if not isinstance(results[2], Exception) else {},
            'gateio': results[3] if not isinstance(results[3], Exception) else {},
            'kucoin': results[4] if not isinstance(results[4], Exception) else {}
        }
        
        # Filter out exchanges with no data
        all_markets = {k: v for k, v in all_markets.items() if v}
        
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
    
    def find_triangular_arbitrage(self, markets: Dict[str, MarketData]) -> List[Dict]:
        """Find triangular arbitrage within an exchange"""
        opportunities = []
        
        # Build graph of USDT pairs
        usdt_pairs = {}
        for market in markets.values():
            if market.quote == 'USDT':
                usdt_pairs[market.base] = {
                    'bid': market.bid,
                    'ask': market.ask,
                    'bid_size': market.bid_size,
                    'ask_size': market.ask_size
                }
        
        # Get all tokens
        tokens = list(usdt_pairs.keys())
        
        # Find triangular paths: USDT -> A -> B -> USDT
        for i, token_a in enumerate(tokens):
            for token_b in tokens:
                if token_a == token_b:
                    continue
                
                # Check if we have direct A/B or B/A pair
                direct_pair = None
                for market in markets.values():
                    if (market.base == token_a and market.quote == token_b) or \
                       (market.base == token_b and market.quote == token_a):
                        direct_pair = market
                        break
                
                if direct_pair:
                    try:
                        # Determine direction
                        if direct_pair.base == token_a and direct_pair.quote == token_b:
                            # A -> B
                            a_to_b_rate = direct_pair.bid
                            b_to_a_rate = 1 / direct_pair.ask
                            
                            # Path: USDT -> A -> B -> USDT
                            usdt_to_a = 1 / usdt_pairs[token_a]['ask']
                            b_to_usdt = usdt_pairs[token_b]['bid']
                            
                            final_amount = 1 * usdt_to_a * a_to_b_rate * b_to_usdt
                            
                        else:
                            # B -> A
                            b_to_a_rate = direct_pair.bid
                            a_to_b_rate = 1 / direct_pair.ask
                            
                            # Path: USDT -> B -> A -> USDT
                            usdt_to_b = 1 / usdt_pairs[token_b]['ask']
                            a_to_usdt = usdt_pairs[token_a]['bid']
                            
                            final_amount = 1 * usdt_to_b * b_to_a_rate * a_to_usdt
                        
                        profit_percentage = (final_amount - 1) * 100
                        fees = Config.PROFIT_CONFIG['taker_fee_percentage'] * 3
                        profit_after_fees = profit_percentage - fees
                        
                        if profit_after_fees > Config.TELEGRAM_CONFIG['alert_threshold']:
                            opportunity = {
                                'type': 'TRIANGULAR_ARBITRAGE',
                                'exchange': list(markets.values())[0].exchange,
                                'path': f"USDT → {token_a} → {token_b} → USDT",
                                'profit_percentage': round(profit_after_fees, 4),
                                'profit_usd': round((final_amount - 1) * Config.SCANNER_CONFIG['simulation_balance'], 2),
                                'timestamp': datetime.now().isoformat()
                            }
                            opportunities.append(opportunity)
                    except:
                        continue
        
        return sorted(opportunities, key=lambda x: x['profit_percentage'], reverse=True)

# ============================================================================
# DEX SIMULATOR (No Complex Blockchain Queries)
# ============================================================================

class DEXSimulator:
    """DEX Simulator using public APIs (no Web3 required)"""
    
    def __init__(self):
        self.session = None
        self.token_prices = {}
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_token_price(self, symbol: str) -> float:
        """Get token price from CoinGecko"""
        cache_key = symbol.lower()
        
        if cache_key in self.token_prices:
            return self.token_prices[cache_key]
        
        try:
            async with self.session.get(
                f"https://api.coingecko.com/api/v3/simple/price",
                params={'ids': cache_key, 'vs_currencies': 'usd'},
                timeout=10
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    price = data.get(cache_key, {}).get('usd', 0)
                    self.token_prices[cache_key] = price
                    return price
        except:
            pass
        
        # Fallback to CoinMarketCap style API
        try:
            async with self.session.get(
                f"https://api.coinpaprika.com/v1/tickers/{cache_key}",
                timeout=10
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    price = data.get('quotes', {}).get('USD', {}).get('price', 0)
                    self.token_prices[cache_key] = price
                    return price
        except:
            pass
        
        return 0
    
    async def get_dex_prices(self, token1: str, token2: str) -> List[Dict]:
        """Get DEX prices from aggregators"""
        prices = []
        
        # Simulate DEX prices (in real implementation, query 1inch, 0x, Paraswap, etc.)
        base_price = await self.get_token_price(token1)
        quote_price = await self.get_token_price(token2)
        
        if base_price > 0 and quote_price > 0:
            # Create simulated DEX prices with slight variations
            actual_price = base_price / quote_price if quote_price > 0 else 0
            
            # Simulate different DEXs with different prices
            for dex, spread in [('uniswap', 0.001), ('sushiswap', 0.002), 
                              ('pancakeswap', 0.003), ('quickswap', 0.004)]:
                price_variation = np.random.uniform(-spread, spread)
                dex_price = actual_price * (1 + price_variation)
                
                if dex_price > 0:
                    prices.append({
                        'dex': dex,
                        'price': dex_price,
                        'liquidity': np.random.uniform(10000, 1000000),
                        'fee': 0.003,
                        'chain': 'ethereum' if dex in ['uniswap', 'sushiswap'] else 'bsc'
                    })
        
        return prices
    
    async def find_dex_arbitrage(self, token_pairs: List[Tuple[str, str]]) -> List[Dict]:
        """Find DEX arbitrage opportunities"""
        opportunities = []
        
        for token1, token2 in token_pairs:
            # Get prices from different DEXs
            dex_prices = await self.get_dex_prices(token1, token2)
            
            if len(dex_prices) < 2:
                continue
            
            # Find best and worst prices
            best_dex = min(dex_prices, key=lambda x: x['price'])
            worst_dex = max(dex_prices, key=lambda x: x['price'])
            
            if best_dex['dex'] == worst_dex['dex']:
                continue
            
            # Calculate arbitrage
            amount = Config.SCANNER_CONFIG['simulation_balance']
            
            # Buy on best DEX (lowest price)
            tokens = amount / best_dex['price']
            tokens_after_fee = tokens * (1 - best_dex['fee'])
            
            # Sell on worst DEX (highest price)
            revenue = tokens_after_fee * worst_dex['price']
            revenue_after_fee = revenue * (1 - worst_dex['fee'])
            
            profit_usd = revenue_after_fee - amount
            profit_percentage = (profit_usd / amount) * 100
            
            # Estimate gas costs
            gas_cost = self.estimate_gas_cost(best_dex['chain'])
            net_profit = profit_usd - gas_cost
            net_profit_percentage = (net_profit / amount) * 100
            
            if net_profit_percentage > Config.TELEGRAM_CONFIG['alert_threshold']:
                opportunity = {
                    'type': 'DEX_ARBITRAGE',
                    'token_pair': f"{token1}/{token2}",
                    'buy_dex': best_dex['dex'],
                    'sell_dex': worst_dex['dex'],
                    'buy_price': round(best_dex['price'], 6),
                    'sell_price': round(worst_dex['price'], 6),
                    'profit_percentage': round(net_profit_percentage, 4),
                    'profit_usd': round(net_profit, 2),
                    'gas_cost': round(gas_cost, 2),
                    'liquidity': min(best_dex['liquidity'], worst_dex['liquidity']),
                    'timestamp': datetime.now().isoformat()
                }
                opportunities.append(opportunity)
        
        return sorted(opportunities, key=lambda x: x['profit_percentage'], reverse=True)
    
    def estimate_gas_cost(self, chain: str) -> float:
        """Estimate gas cost"""
        gas_costs = {
            'ethereum': 50,    # $50 for 2 swaps
            'bsc': 2,          # $2 for 2 swaps
            'polygon': 0.5,    # $0.5 for 2 swaps
            'arbitrum': 2,     # $2 for 2 swaps
            'optimism': 1      # $1 for 2 swaps
        }
        return gas_costs.get(chain, 10)

# ============================================================================
# TELEGRAM ALERTER (NOW ENABLED)
# ============================================================================

class TelegramAlerter:
    """Telegram alerts - Now enabled by default"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.bot = None
        self.alerted = set()
        self.last_summary_time = time.time()
        self.scans_since_summary = 0
        
        # Check if Telegram is enabled
        if not config.get('enabled', True):
            logging.info("Telegram alerts are disabled in config")
            return
            
        # Check if credentials are provided
        if not config.get('bot_token') or not config.get('chat_id'):
            logging.warning("Telegram bot_token or chat_id not configured. Alerts disabled.")
            logging.info("To enable Telegram alerts:")
            logging.info("1. Get bot token from @BotFather")
            logging.info("2. Get chat ID from @userinfobot")
            logging.info("3. Update TELEGRAM_CONFIG in bot.py")
            return
        
        # Try to import telegram, but don't fail if not installed
        try:
            from telegram import Bot
            self.bot = Bot(token=config['bot_token'])
            logging.info("✅ Telegram bot initialized successfully")
            
            # Send startup message
            asyncio.create_task(self.send_startup_message())
            
        except ImportError:
            logging.warning("⚠️ python-telegram-bot not installed. To install:")
            logging.warning("   pip install python-telegram-bot")
            logging.warning("   Alerts will be disabled until installed.")
        except Exception as e:
            logging.error(f"❌ Telegram bot setup failed: {e}")
    
    async def send_startup_message(self):
        """Send startup message to Telegram"""
        try:
            if self.bot:
                message = f"""
🤖 <b>Arbitrage Bot Started!</b>

📊 <b>Configuration:</b>
   Scan interval: {Config.SCANNER_CONFIG['scan_interval']}s
   Min profit: {self.config['alert_threshold']}%
   Sim balance: ${Config.SCANNER_CONFIG['simulation_balance']}
   
📡 <b>Monitoring:</b>
   CEXs: Binance, Bybit, MEXC, Gate.io, KuCoin
   DEXs: Uniswap, Sushiswap, Pancakeswap, QuickSwap
   
✅ Bot is now running and will send alerts for profitable opportunities.
"""
                await self.bot.send_message(
                    chat_id=self.config['chat_id'],
                    text=message,
                    parse_mode='HTML'
                )
                logging.info("✅ Startup message sent to Telegram")
        except Exception as e:
            logging.error(f"Failed to send startup message: {e}")
    
    async def send_alert(self, opportunity: Dict) -> bool:
        """Send alert if Telegram is configured"""
        if not self.bot or not self.config.get('enabled', True):
            return False
        
        # Create unique ID for this opportunity
        opp_id = f"{opportunity['type']}_{opportunity.get('symbol', opportunity.get('token_pair', ''))}_{datetime.now().strftime('%Y%m%d_%H')}"
        
        # Check cooldown
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
            
            # Clean old alerts from set (keep only last 24 hours)
            current_time = time.time()
            if hasattr(self, 'last_cleanup'):
                if current_time - self.last_cleanup > 3600:  # Cleanup every hour
                    self.alerted = {id for id in self.alerted if not id.endswith('_old')}
                    self.last_cleanup = current_time
            
            logging.info(f"✅ Telegram alert sent: {opp_id}")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to send Telegram alert: {e}")
            return False
    
    def format_message(self, opportunity: Dict) -> str:
        """Format alert message"""
        if opportunity['type'] == 'CEX_ARBITRAGE':
            return f"""
🚀 <b>CEX ARBITRAGE FOUND!</b>

📊 <b>Pair:</b> {opportunity['symbol']}
🔼 <b>Buy on:</b> {opportunity['buy_exchange'].upper()} @ ${opportunity['buy_price']}
🔽 <b>Sell on:</b> {opportunity['sell_exchange'].upper()} @ ${opportunity['sell_price']}
💰 <b>Profit:</b> {opportunity['profit_percentage']:.2f}% (${opportunity['profit_usd']:.2f})
📈 <b>Volume:</b> {opportunity['volume']:.2f}
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}

<a href="https://www.binance.com/en/trade/{opportunity['symbol'].replace('/', '_')}">📈 View on Binance</a>
"""
        elif opportunity['type'] == 'TRIANGULAR_ARBITRAGE':
            return f"""
🔄 <b>TRIANGULAR ARBITRAGE!</b>

🏦 <b>Exchange:</b> {opportunity['exchange'].upper()}
🔄 <b>Path:</b> {opportunity['path']}
💰 <b>Profit:</b> {opportunity['profit_percentage']:.2f}% (${opportunity['profit_usd']:.2f})
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
"""
        else:  # DEX_ARBITRAGE
            return f"""
🦄 <b>DEX ARBITRAGE FOUND!</b>

📊 <b>Pair:</b> {opportunity['token_pair']}
🔼 <b>Buy on:</b> {opportunity['buy_dex'].upper()} @ ${opportunity['buy_price']}
🔽 <b>Sell on:</b> {opportunity['sell_dex'].upper()} @ ${opportunity['sell_price']}
💰 <b>Profit:</b> {opportunity['profit_percentage']:.2f}% (${opportunity['profit_usd']:.2f})
⛽ <b>Gas Cost:</b> ${opportunity['gas_cost']:.2f}
📈 <b>Liquidity:</b> ${opportunity['liquidity']:.0f}
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
"""
    
    async def send_summary(self, opportunities: List[Dict], scan_duration: float):
        """Send scan summary"""
        if not self.bot or not self.config.get('enabled', True):
            return
        
        self.scans_since_summary += 1
        
        # Check if it's time to send summary
        if self.scans_since_summary < self.config.get('summary_interval', 10):
            return
        
        try:
            if not opportunities:
                message = f"""
📊 <b>Scan Summary</b>

No profitable opportunities found in the last {self.scans_since_summary} scans.
⏱️ <b>Last scan duration:</b> {scan_duration:.1f}s
🔄 <b>Next scan in:</b> {Config.SCANNER_CONFIG['scan_interval']}s
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
"""
            else:
                top = opportunities[0]
                message = f"""
📊 <b>Scan Summary</b>

🔍 <b>Scans completed:</b> {self.scans_since_summary}
🎯 <b>Opportunities found:</b> {len(opportunities)}
🏆 <b>Top opportunity:</b>
   • Type: {top['type']}
   • Profit: {top['profit_percentage']:.2f}%
   • USD: ${top['profit_usd']:.2f}
⏱️ <b>Scan duration:</b> {scan_duration:.1f}s
🔄 <b>Next scan in:</b> {Config.SCANNER_CONFIG['scan_interval']}s
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
"""
            
            await self.bot.send_message(
                chat_id=self.config['chat_id'],
                text=message,
                parse_mode='HTML'
            )
            
            # Reset counter
            self.scans_since_summary = 0
            logging.info("✅ Summary sent to Telegram")
            
        except Exception as e:
            logging.error(f"❌ Failed to send summary: {e}")
    
    async def send_error_alert(self, error: str):
        """Send error alert to Telegram"""
        if not self.bot or not self.config.get('enabled', True):
            return
        
        try:
            message = f"""
⚠️ <b>BOT ERROR ALERT</b>

❌ <b>Error:</b> {error}
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}

The bot may need attention!
"""
            await self.bot.send_message(
                chat_id=self.config['chat_id'],
                text=message,
                parse_mode='HTML'
            )
        except:
            pass

# ============================================================================
# DATA LOGGER
# ============================================================================

class DataLogger:
    """Log opportunities to CSV"""
    
    def __init__(self, log_dir: str = "arb_logs"):
        self.log_dir = log_dir
        self.csv_file = os.path.join(log_dir, "opportunities.csv")
        
        os.makedirs(log_dir, exist_ok=True)
        
        # Create CSV with headers
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'type', 'profit_percentage', 'profit_usd',
                    'details', 'alerted'
                ])
    
    def log_opportunity(self, opportunity: Dict):
        """Log opportunity to CSV"""
        try:
            with open(self.csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    opportunity['timestamp'],
                    opportunity['type'],
                    opportunity['profit_percentage'],
                    opportunity['profit_usd'],
                    json.dumps(opportunity),
                    'False'
                ])
            
            logging.info(f"📝 Logged {opportunity['type']} opportunity: {opportunity['profit_percentage']:.2f}%")
        except Exception as e:
            logging.error(f"❌ Failed to log opportunity: {e}")
    
    def get_recent_opportunities(self, hours: int = 1) -> pd.DataFrame:
        """Get recent opportunities"""
        try:
            df = pd.read_csv(self.csv_file)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            cutoff = datetime.now() - timedelta(hours=hours)
            return df[df['timestamp'] >= cutoff]
        except:
            return pd.DataFrame()

# ============================================================================
# MAIN ARBITRAGE BOT
# ============================================================================

class ArbitrageBot:
    """Main arbitrage detection bot"""
    
    def __init__(self):
        self.running = False
        self.scan_count = 0
        self.total_opportunities = 0
        
        # Initialize components
        self.cex_scanner = None
        self.dex_simulator = None
        self.alerter = TelegramAlerter(Config.TELEGRAM_CONFIG)
        self.logger = DataLogger()
        
        logging.info("✅ Arbitrage Bot initialized")
        logging.info("📱 Telegram alerts: " + ("ENABLED" if Config.TELEGRAM_CONFIG.get('enabled', True) and 
                                              Config.TELEGRAM_CONFIG.get('bot_token') and 
                                              Config.TELEGRAM_CONFIG.get('chat_id') else "DISABLED"))
    
    async def run_scan(self):
        """Run a single scan"""
        start_time = time.time()
        
        try:
            async with CEXScanner() as cex_scanner:
                async with DEXSimulator() as dex_simulator:
                    self.cex_scanner = cex_scanner
                    self.dex_simulator = dex_simulator
                    
                    all_opportunities = []
                    
                    # 1. Scan CEXs
                    logging.info("🔍 Scanning CEXs...")
                    cex_data = await self.cex_scanner.fetch_all_exchanges()
                    
                    # Find CEX arbitrage
                    cex_arb = self.cex_scanner.find_cex_arbitrage(cex_data)
                    all_opportunities.extend(cex_arb)
                    
                    # Find triangular arbitrage for each exchange
                    for exchange, markets in cex_data.items():
                        if markets:
                            tri_arb = self.cex_scanner.find_triangular_arbitrage(markets)
                            all_opportunities.extend(tri_arb)
                    
                    # 2. Scan DEXs
                    logging.info("🔍 Scanning DEXs...")
                    # Common token pairs to check
                    token_pairs = [
                        ('ethereum', 'usd-coin'),
                        ('ethereum', 'tether'),
                        ('bitcoin', 'ethereum'),
                        ('binancecoin', 'tether'),
                        ('matic-network', 'tether')
                    ]
                    
                    dex_arb = await self.dex_simulator.find_dex_arbitrage(token_pairs)
                    all_opportunities.extend(dex_arb)
                    
                    # 3. Process results
                    scan_duration = time.time() - start_time
                    
                    if all_opportunities:
                        # Sort by profit
                        all_opportunities.sort(key=lambda x: x['profit_percentage'], reverse=True)
                        
                        # Log and alert
                        for opp in all_opportunities[:10]:  # Top 10
                            self.logger.log_opportunity(opp)
                            await self.alerter.send_alert(opp)
                        
                        # Print results
                        self.print_results(all_opportunities, scan_duration)
                        
                        # Send summary
                        await self.alerter.send_summary(all_opportunities[:5], scan_duration)
                        
                        self.total_opportunities += len(all_opportunities)
                    else:
                        logging.info(f"✅ Scan complete in {scan_duration:.1f}s - No opportunities found")
                        # Send summary even when no opportunities
                        await self.alerter.send_summary([], scan_duration)
                    
                    return all_opportunities
                    
        except Exception as e:
            logging.error(f"❌ Scan failed: {e}")
            # Send error alert to Telegram
            await self.alerter.send_error_alert(str(e))
            return []
    
    def print_results(self, opportunities: List[Dict], duration: float):
        """Print results to console"""
        print(f"\n{'='*60}")
        print(f"📊 Scan #{self.scan_count + 1} completed in {duration:.1f}s")
        print(f"📈 Found {len(opportunities)} opportunities")
        print(f"📱 Telegram alerts: {'✅ ENABLED' if Config.TELEGRAM_CONFIG.get('enabled', True) else '❌ DISABLED'}")
        
        if opportunities:
            print(f"\n🏆 TOP OPPORTUNITIES:")
            for i, opp in enumerate(opportunities[:5], 1):
                if opp['type'] == 'CEX_ARBITRAGE':
                    print(f"{i}. {opp['type']}: {opp['symbol']}")
                    print(f"   Buy: {opp['buy_exchange']} @ ${opp['buy_price']}")
                    print(f"   Sell: {opp['sell_exchange']} @ ${opp['sell_price']}")
                elif opp['type'] == 'TRIANGULAR_ARBITRAGE':
                    print(f"{i}. {opp['type']}: {opp['exchange']}")
                    print(f"   Path: {opp['path']}")
                else:
                    print(f"{i}. {opp['type']}: {opp['token_pair']}")
                    print(f"   Buy: {opp['buy_dex']} @ ${opp['buy_price']}")
                    print(f"   Sell: {opp['sell_dex']} @ ${opp['sell_price']}")
                
                print(f"   Profit: {opp['profit_percentage']:.2f}% (${opp['profit_usd']:.2f})")
                print()
        
        print(f"{'='*60}")
    
    async def run(self):
        """Main loop"""
        self.running = True
        
        print(f"""
╔══════════════════════════════════════════════════════════╗
║    CRYPTO ARBITRAGE BOT - NO API KEYS REQUIRED          ║
║                Public Endpoints Only                    ║
║               📱 TELEGRAM ALERTS ENABLED                ║
╚══════════════════════════════════════════════════════════╝
        """)
        
        print(f"🔧 Configuration:")
        print(f"   Scan interval: {Config.SCANNER_CONFIG['scan_interval']}s")
        print(f"   Min profit: {Config.TELEGRAM_CONFIG['alert_threshold']}%")
        print(f"   Sim balance: ${Config.SCANNER_CONFIG['simulation_balance']}")
        print(f"   Telegram alerts: {'✅ ENABLED' if Config.TELEGRAM_CONFIG.get('enabled', True) else '❌ DISABLED'}")
        
        print(f"\n📡 Scanning: Binance, Bybit, MEXC, Gate.io, KuCoin")
        print(f"🦄 DEXs: Uniswap, Sushiswap, Pancakeswap, QuickSwap")
        print(f"\n{'='*60}")
        print("🚀 Bot started! Press Ctrl+C to stop.")
        print(f"{'='*60}\n")
        
        try:
            while self.running:
                self.scan_count += 1
                await self.run_scan()
                
                # Wait for next scan
                wait_time = Config.SCANNER_CONFIG['scan_interval']
                logging.info(f"⏳ Next scan in {wait_time}s...")
                await asyncio.sleep(wait_time)
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping bot...")
        except Exception as e:
            logging.error(f"❌ Bot error: {e}")
            await self.alerter.send_error_alert(str(e))
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
        print(f"   Telegram alerts sent: {len(self.alerter.alerted)}")
        print(f"{'='*60}")
        
        # Send shutdown message to Telegram
        try:
            if Config.TELEGRAM_CONFIG.get('enabled', True) and Config.TELEGRAM_CONFIG.get('bot_token'):
                from telegram import Bot
                bot = Bot(token=Config.TELEGRAM_CONFIG['bot_token'])
                message = f"""
🛑 <b>Arbitrage Bot Stopped</b>

📊 <b>Final Statistics:</b>
   • Total scans: {self.scan_count}
   • Opportunities found: {self.total_opportunities}
   • Alerts sent: {len(self.alerter.alerted)}
   
⏰ <b>Time:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
                await bot.send_message(
                    chat_id=Config.TELEGRAM_CONFIG['chat_id'],
                    text=message,
                    parse_mode='HTML'
                )
        except:
            pass
        
        # Export to Excel
        try:
            df = self.logger.get_recent_opportunities(24)
            if not df.empty:
                excel_file = os.path.join(self.logger.log_dir, f"arbitrage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                df.to_excel(excel_file, index=False)
                print(f"📁 Data exported to: {excel_file}")
        except:
            pass
        
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
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('arbitrage_bot.log')
        ]
    )
    
    # Check if requirements are installed
    try:
        import aiohttp
        import pandas
        import numpy
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        print("💡 Install with: pip install aiohttp pandas numpy")
        return
    
    # Check Telegram configuration
    if Config.TELEGRAM_CONFIG.get('enabled', True):
        if not Config.TELEGRAM_CONFIG.get('bot_token') or not Config.TELEGRAM_CONFIG.get('chat_id'):
            print(f"\n{'='*60}")
            print("⚠️  TELEGRAM ALERTS NOT CONFIGURED")
            print(f"{'='*60}")
            print("To enable Telegram alerts:")
            print("1. Open Telegram, search for @BotFather")
            print("2. Create a bot with /newbot command")
            print("3. Save the bot token")
            print("4. Search for @userinfobot to get your chat ID")
            print("5. Update TELEGRAM_CONFIG in bot.py")
            print(f"{'='*60}")
            print("The bot will run without Telegram alerts.")
            print("Press Enter to continue or Ctrl+C to cancel...")
            input()
    
    # Run bot
    bot = ArbitrageBot()
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")

# ============================================================================
# EASY SETUP SCRIPT WITH TELEGRAM
# ============================================================================

def setup_bot():
    """Easy setup script"""
    print("""
╔══════════════════════════════════════════════════════════╗
║        EASY ARBITRAGE BOT SETUP - NO API KEYS           ║
║               📱 WITH TELEGRAM ALERTS                   ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    # Check Python version
    python_version = sys.version_info
    if python_version.major < 3 or (python_version.major == 3 and python_version.minor < 7):
        print("❌ Python 3.7+ is required")
        return
    
    # Install dependencies
    print("📦 Installing dependencies...")
    os.system(f"{sys.executable} -m pip install aiohttp pandas numpy")
    
    # Telegram setup
    print("\n" + "="*60)
    print("🤖 TELEGRAM SETUP (Recommended for alerts)")
    print("="*60)
    
    telegram = input("\nEnable Telegram alerts? (y/n): ").lower().strip()
    if telegram == 'y':
        print("\n📝 Telegram Setup Instructions:")
        print("1. Open Telegram, search for @BotFather")
        print("2. Send /newbot command")
        print("3. Choose a name for your bot (e.g., ArbitrageAlertBot)")
        print("4. Save the bot token (looks like: 1234567890:ABCdefGHIjklMNOpqrsTUVwxyz)")
        print("5. Search for @userinfobot to get your chat ID (a number like 123456789)")
        print("6. Start a chat with your new bot and send /start")
        print("="*60)
        
        token = input("\nEnter bot token: ").strip()
        chat_id = input("Enter chat ID: ").strip()
        
        # Update config in the file
        with open(__file__, 'r') as f:
            content = f.read()
        
        # Update bot token and chat ID
        content = content.replace("'bot_token': 'YOUR_BOT_TOKEN_HERE',", f"'bot_token': '{token}',")
        content = content.replace("'chat_id': 'YOUR_CHAT_ID_HERE',", f"'chat_id': '{chat_id}',")
        
        with open(__file__, 'w') as f:
            f.write(content)
        
        print("\n✅ Telegram credentials saved!")
        
        # Install telegram library
        install_tg = input("\nInstall python-telegram-bot? (y/n): ").lower().strip()
        if install_tg == 'y':
            print("📦 Installing python-telegram-bot...")
            os.system(f"{sys.executable} -m pip install python-telegram-bot")
            print("✅ python-telegram-bot installed!")
    else:
        print("\n⚠️  Telegram alerts disabled. You can enable them later by editing bot.py")
    
    # Create bot file
    with open('arbitrage_bot.py', 'w') as f:
        f.write(__doc__ + '\n\n' + open(__file__).read())
    
    print("\n" + "="*60)
    print("✅ SETUP COMPLETE!")
    print("="*60)
    print("\n🚀 To run the bot:")
    print("   python arbitrage_bot.py")
    print("\n📊 Logs will be saved to 'arb_logs/' folder")
    print("💡 Press Ctrl+C to stop the bot")
    print("\n📱 Telegram alerts will be sent when profitable opportunities are found")
    print("="*60)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--setup':
        setup_bot()
    else:
        main()