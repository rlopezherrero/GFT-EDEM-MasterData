#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#Created on Wed Nov 13 13:41:28 2019

#@author: edem

class tableSchema():
    quotes_schema = 'symbol:STRING,companyName:STRING,primaryExchange:STRING,calculationPrice:STRING,open:NUMERIC,openTime:NUMERIC,close:NUMERIC,closeTime:NUMERIC,high:NUMERIC,low:NUMERIC,latestPrice:NUMERIC,latestSource:STRING,latestTime:STRING,latestUpdate:NUMERIC,latestVolume:NUMERIC,iexRealtimePrice:NUMERIC,iexRealtimeSize:NUMERIC,iexLastUpdated:NUMERIC,delayedPrice:NUMERIC,delayedPriceTime:NUMERIC,extendedPrice:NUMERIC,extendedChange:NUMERIC,extendedChangePercent:NUMERIC,extendedPriceTime:NUMERIC,previousClose:NUMERIC,previousVolume:NUMERIC,change:NUMERIC,changePercent:NUMERIC,volume:NUMERIC,iexMarketPercent:NUMERIC,iexVolume:NUMERIC,avgTotalVolume:NUMERIC,iexBidPrice:NUMERIC,iexBidSize:NUMERIC,iexAskPrice:NUMERIC,iexAskSize:NUMERIC,marketCap:NUMERIC,peRatio:NUMERIC,week52High:NUMERIC,week52Low:NUMERIC,ytdChange:NUMERIC,lastTradeTime:NUMERIC,isUSMarketOpen:BOOL'