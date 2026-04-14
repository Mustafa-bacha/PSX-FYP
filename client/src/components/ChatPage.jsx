import React, { useEffect, useState } from 'react';
import { apiClient } from '../api/client.js';
import { ChatPanel } from './ChatPanel.jsx';
import { StocksPanel } from './StocksPanel.jsx';

export function ChatPage({ token }) {
  const [stocks, setStocks] = useState([]);
  const [watchlistSet, setWatchlistSet] = useState(new Set());

  const [symbol, setSymbol] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [marketMode, setMarketMode] = useState(true);
  const [chatMessages, setChatMessages] = useState([]);
  const [chatLoading, setChatLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    let mounted = true;
    apiClient.stocks().then(s => {
      if (mounted) setStocks(Array.isArray(s?.snapshots) ? s.snapshots : []);
    }).catch(() => {});

    if (token) {
      apiClient.watchlist(token).then(res => {
        if (mounted && Array.isArray(res?.items)) {
          setWatchlistSet(new Set(res.items.map(x => x.symbol)));
        }
      }).catch(() => {});
    }
    return () => { mounted = false; };
  }, [token]);

  useEffect(() => {
    let active = true;
    const scope = marketMode ? 'MARKET' : symbol;
    if (!scope || !token) {
      setChatMessages([]);
      return;
    }

    (async () => {
      try {
        setError('');
        const data = await apiClient.chatHistory(scope, token, 30);
        if (!active) return;
        setChatMessages(Array.isArray(data?.messages) ? data.messages : []);
      } catch (err) {
        if (active) {
          setError(err.message || 'Failed to fetch chat history');
          setChatMessages([]);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [symbol, marketMode, token]);

  const handleChatSend = async (question) => {
    const scope = marketMode ? 'MARKET' : symbol;
    if (!scope || !question) return;

    const userMsg = { role: 'user', content: question };
    const nextHistory = [...chatMessages, userMsg];
    setChatMessages(nextHistory);
    setChatLoading(true);
    setError('');

    try {
      const resp = await apiClient.chat({
        stock: scope,
        question,
        history: nextHistory.slice(-12),
        token: token || undefined
      });
      const assistant = { role: 'assistant', content: resp?.answer || 'No response generated.' };
      setChatMessages((prev) => [...prev, assistant]);
    } catch (err) {
      setChatMessages((prev) => [...prev, {
        role: 'assistant',
        content: `I could not generate a reply: ${err.message || 'Unknown error'}`
      }]);
    } finally {
      setChatLoading(false);
    }
  };

  const handleClearChatHistory = async () => {
    const scope = marketMode ? 'MARKET' : symbol;
    if (!scope || !token) return;

    setChatMessages([]);
    try {
      await apiClient.clearChatHistory(scope, token);
    } catch (err) {
      setError(err.message || 'Failed to clear chat history');
    }
  };

  const toggleMode = () => {
    setMarketMode((prev) => !prev);
  };

  const handleSearchSubmit = (e) => {
    e.preventDefault();
    const query = searchQuery.trim().toUpperCase();
    if (query) {
      setSymbol(query);
      setMarketMode(false);
    }
  };

  return (
    <section className="page-stack">
      <div className="card dashboard-toolbar">
        <div>
          <h2>AI Stock Analysis</h2>
          <p className="muted-line">
            Chat with the AI assistant about specific stocks or broad market trends.
          </p>
        </div>
        <div className="toolbar">

          <button 
            className={`toggle-btn ${marketMode ? 'active' : ''}`} 
            onClick={toggleMode}
          >
            {marketMode ? 'Market Mode Active' : 'Switch to Market Mode'}
          </button>
        </div>
      </div>
      
      {error && <div className="error-box">{error}</div>}

      <div className="chat-page-layout">
        <StocksPanel 
          stocks={stocks}
          selected={!marketMode ? symbol : null}
          onSelect={(sym) => {
            setSymbol(sym);
            setMarketMode(false);
          }}
          watchlistSet={watchlistSet}
        />
        
        <div className="chat-room">
          <ChatPanel 
            stock={symbol}
            messages={chatMessages}
            onSend={handleChatSend}
            loading={chatLoading}
            marketMode={marketMode}
            onToggleMode={toggleMode}
            onClearHistory={handleClearChatHistory}
          />
        </div>

        <aside className="chat-sidebar">
          <div className="card" style={{ padding: '20px' }}>
            <h3 style={{ marginTop: 0 }}>Analysis Tools</h3>
            <p className="muted-line" style={{ fontSize: '0.85rem' }}>Select a quick prompt below to instantly trigger an AI deep dive.</p>
            <div className="prompt-list">
               <button className="prompt-btn" onClick={() => handleChatSend('Which stocks look strongest today and why?')} disabled={chatLoading}>
                 <b>Market Overview</b>
                 Analyze current market trends and top performers.
               </button>
               <button className="prompt-btn" onClick={() => handleChatSend('Draft a swing-trade setup for currently active stocks.')} disabled={chatLoading}>
                 <b>Swing Trade Setup</b>
                 Generate actionable swing trade ideas.
               </button>
               <button className="prompt-btn" onClick={() => handleChatSend('Analyze latest financials and technical indicators for top volume stocks.')} disabled={chatLoading}>
                 <b>Technical Deep Dive</b>
                 Review RSI, MACD, and fundamentals for heavy movers.
               </button>
               <button className="prompt-btn" onClick={() => handleChatSend('What are the major risks in the PSX right now?')} disabled={chatLoading}>
                 <b>Risk Assessment</b>
                 Identify current sector or macroeconomic risks.
               </button>
            </div>
          </div>
        </aside>
      </div>
    </section>
  );
}
