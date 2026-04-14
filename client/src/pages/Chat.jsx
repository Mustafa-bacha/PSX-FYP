import { useState, useRef, useEffect } from 'react';
import { chatbot as chatbotApi } from '../api.js';
import { cardClass } from '../lib/constants.js';
import StockSearch from '../components/StockSearch.jsx';

function renderInlineRich(text) {
  const parts = String(text || '').split(/(\*\*[^*]+\*\*)/g).filter(Boolean);
  return parts.map((part, idx) => {
    if (/^\*\*[^*]+\*\*$/.test(part)) {
      return <strong key={`b-${idx}`} className="text-white font-semibold">{part.slice(2, -2)}</strong>;
    }
    return <span key={`t-${idx}`}>{part}</span>;
  });
}

function normalizeAssistantText(content) {
  let text = String(content || '').replace(/\r\n?/g, '\n').trim();
  if (!text) return text;

  text = text
    // Ensure markdown headings start on their own line even when model returns one long paragraph.
    .replace(/([^\n])\s+(##\s+)/g, '$1\n$2')
    .replace(/([^\n])\s+(###\s+)/g, '$1\n$2')
    // Add line breaks before key section labels if model emits inline sections.
    .replace(/\s+(Technical Analysis|Fundamental Analysis|Sentiment Analysis|News and Sentiment|Headlines|Conclusion|Risk Factors|Actionable Interpretation|Disclaimer):/gi, '\n$1:')
    // Convert star bullets into markdown bullets when they appear after punctuation/labels.
    .replace(/(:)\s+\*\s+/g, '$1\n- ')
    .replace(/([.!?])\s+\*\s+/g, '$1\n- ')
    // Normalize list markers at line start.
    .replace(/^\*\s+/gm, '- ')
    .replace(/^•\s+/gm, '- ')
    // Prevent excessive blank lines.
    .replace(/\n{3,}/g, '\n\n');

  return text;
}

function renderAssistantStructured(content) {
  const lines = normalizeAssistantText(content).split('\n');
  const nodes = [];
  let paragraph = [];
  let listItems = [];

  const isPlainHeading = (line) => {
    const key = line.toLowerCase().replace(/[:\-]+\s*$/, '').trim();
    return [
      'direct answer',
      'evidence snapshot',
      'action plan & risks',
      'action plan and risks',
      'historical data evidence',
      'latest news (daily refreshed)',
      'latest news',
      'actionable interpretation',
      'risks & uncertainty',
      'risks and uncertainty',
      'recommendation',
      'risk assessment',
      'actionable conclusion',
      'balanced conclusion',
      'outlook summary',
      'fundamental conclusion',
      'sentiment conclusion',
      'risk ranking & mitigation',
      'key levels to watch',
    ].includes(key);
  };

  const flushParagraph = () => {
    if (!paragraph.length) return;
    const text = paragraph.join(' ').trim();
    if (text) {
      nodes.push(
        <p key={`p-${nodes.length}`} className="text-slate-200 leading-relaxed">
          {renderInlineRich(text)}
        </p>
      );
    }
    paragraph = [];
  };

  const flushList = () => {
    if (!listItems.length) return;
    nodes.push(
      <ul key={`ul-${nodes.length}`} className="list-disc pl-5 space-y-1 text-slate-200">
        {listItems.map((item, idx) => (
          <li key={`li-${nodes.length}-${idx}`}>{renderInlineRich(item)}</li>
        ))}
      </ul>
    );
    listItems = [];
  };

  for (const raw of lines) {
    const line = String(raw || '').trim();

    if (!line) {
      flushParagraph();
      flushList();
      continue;
    }

    if (/^###\s+/.test(line)) {
      flushParagraph();
      flushList();
      nodes.push(
        <h3 key={`h3-${nodes.length}`} className="text-sm md:text-base font-semibold text-brand-200 mt-1">
          {renderInlineRich(line.replace(/^###\s+/, ''))}
        </h3>
      );
      continue;
    }

    if (/^##\s+/.test(line)) {
      flushParagraph();
      flushList();
      nodes.push(
        <h2 key={`h2-${nodes.length}`} className="text-base md:text-lg font-bold text-white mt-3 mb-1 border-b border-slate-700/40 pb-1">
          {renderInlineRich(line.replace(/^##\s+/, ''))}
        </h2>
      );
      continue;
    }

    if (isPlainHeading(line)) {
      flushParagraph();
      flushList();
      nodes.push(
        <h2 key={`ph-${nodes.length}`} className="text-base md:text-lg font-bold text-white mt-3 mb-1 border-b border-slate-700/40 pb-1">
          {renderInlineRich(line.replace(/[:\-]+\s*$/, ''))}
        </h2>
      );
      continue;
    }

    if (/^[-*]\s+/.test(line)) {
      flushParagraph();
      listItems.push(line.replace(/^[-*]\s+/, ''));
      continue;
    }

    // Numbered list items (1. 2. etc.)
    if (/^\d+\.\s+/.test(line)) {
      flushParagraph();
      listItems.push(line.replace(/^\d+\.\s+/, ''));
      continue;
    }

    paragraph.push(line);
  }

  flushParagraph();
  flushList();

  return (
    <div className="space-y-2">
      {nodes.length ? nodes : <p className="text-slate-200 whitespace-pre-wrap">{content}</p>}
    </div>
  );
}

function SentimentBadge({ sentiment }) {
  const colors = {
    positive: 'bg-green-500/20 text-green-300 border-green-500/40',
    negative: 'bg-red-500/20 text-red-300 border-red-500/40',
    neutral: 'bg-slate-500/20 text-slate-300 border-slate-500/40',
  };
  const cls = colors[sentiment] || colors.neutral;
  return (
    <span className={`px-2 py-0.5 rounded border text-xs font-medium ${cls}`}>
      {sentiment}
    </span>
  );
}

export default function Chat() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [mode, setMode] = useState('MARKET');
  const [symbol, setSymbol] = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const activeScope = mode === 'STOCK'
    ? (symbol.trim().toUpperCase() || 'MARKET')
    : 'MARKET';

  const activeScopeLabel = mode === 'STOCK'
    ? (symbol.trim().toUpperCase() || 'Select symbol')
    : 'MARKET';

  const send = async (override = '') => {
    const q = String(override || input).trim();
    if (!q || loading) return;

    if (mode === 'STOCK' && !symbol.trim()) {
      setMessages((m) => [
        ...m,
        {
          role: 'assistant',
          content: 'Please enter a stock symbol first (e.g., ENGRO, OGDC, MCB) or switch to Market mode.',
          sources: [],
        },
      ]);
      return;
    }

    setInput('');
    const userMsg = { role: 'user', content: q };
    const historyForModel = [...messages, userMsg]
      .slice(-12)
      .map((m) => ({ role: m.role, content: m.content }));

    setMessages((m) => [...m, userMsg]);
    setLoading(true);

    try {
      const res = await chatbotApi.ask(q, {
        stock: activeScope,
        history: historyForModel,
      });

      if (!res || !res.answer) {
        throw new Error('No answer received from chatbot');
      }

      setMessages((m) => [
        ...m,
        {
          role: 'assistant',
          content: res.answer,
          sources: res.sources || [],
          scope: res.scope || activeScope,
          sentiment: res.sentiment || 'neutral',
          retrieval: res.retrieval || null,
        },
      ]);
    } catch (e) {
      const errorMsg = e.error || e.message || 'Sorry, I could not get an answer. Please try again.';
      setMessages((m) => [
        ...m,
        {
          role: 'assistant',
          content: errorMsg,
          sources: [],
        },
      ]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-5xl mx-auto px-4 py-8 space-y-4">
  <section className={`${cardClass} p-4 border-slate-700/60 relative z-30 overflow-visible`}>
        <h1 className="text-2xl font-bold text-white mb-1">RAG Chat Assistant</h1>
        <p className="text-slate-400 text-sm">
          Comprehensive PSX Q&amp;A using deep historical analysis, PSX fundamentals, technical indicators &amp; daily refreshed business news.
        </p>

        <div className="mt-4 grid md:grid-cols-[auto_auto_minmax(0,1fr)] gap-2 items-center">
          <div className="flex rounded-lg border border-slate-600/80 overflow-hidden w-fit">
            <button
              type="button"
              onClick={() => setMode('MARKET')}
              className={`px-3 py-2 text-sm ${mode === 'MARKET' ? 'bg-brand-600 text-white' : 'bg-slate-900/40 text-slate-300 hover:bg-slate-700/50'}`}
            >
              Market
            </button>
            <button
              type="button"
              onClick={() => setMode('STOCK')}
              className={`px-3 py-2 text-sm border-l border-slate-600 ${mode === 'STOCK' ? 'bg-brand-600 text-white' : 'bg-slate-900/40 text-slate-300 hover:bg-slate-700/50'}`}
            >
              Stock
            </button>
          </div>

          <div className={mode !== 'STOCK' ? 'opacity-60 pointer-events-none' : ''}>
            <StockSearch
              placeholder="Search symbol (e.g., OGDC)"
              onSelect={(ticker) => setSymbol(String(ticker || '').toUpperCase())}
              showDefaultWhenEmpty
              defaultListLimit={5000}
              queryLimit={5000}
              listHeightClass="max-h-52"
            />
          </div>

          <div className="text-xs text-slate-400">
            Active scope: <span className="text-brand-300 font-mono">{activeScopeLabel}</span>
            {mode === 'STOCK' && symbol ? <span className="ml-2 text-slate-500">Selected from search</span> : null}
          </div>
        </div>

        <div className="mt-3 flex flex-wrap gap-2">
          {[
            { label: 'Full Analysis', prompt: `Give me a complete analysis of ${activeScope} with technical levels, sentiment, and recommendation.` },
            { label: 'Market Outlook', prompt: 'Give me a full PSX market outlook for today with key risks and top movers.' },
            { label: 'Buy/Sell Signal', prompt: `Should I buy ${activeScope}? Analyze trend, momentum, volume, sentiment and give entry/exit levels.` },
            { label: 'News & Sentiment', prompt: `What are the latest news and sentiment trends for ${activeScope}? Include headline analysis.` },
            { label: 'Risk Assessment', prompt: `What are the key risks for ${activeScope}? Include volatility, support levels, and stop-loss suggestions.` },
            { label: 'Historical Trend', prompt: `Show me the complete historical performance of ${activeScope} with weekly and monthly breakdowns.` },
          ].map((item, idx) => (
            <button
              key={`${idx}-${item.label}`}
              type="button"
              onClick={() => send(item.prompt)}
              disabled={loading}
              className="px-2.5 py-1.5 rounded border border-slate-600 text-slate-300 text-xs hover:bg-slate-700/50 disabled:opacity-50 transition-colors"
            >
              {item.label}
            </button>
          ))}
        </div>
      </section>

  <div className={`${cardClass} flex flex-col min-h-[500px] relative z-10`}>
        <div className="flex-1 overflow-y-auto p-4 space-y-4 min-h-[380px]">
          {messages.length === 0 && (
            <div className="text-slate-500 text-sm space-y-2">
              <p>Try asking detailed questions like:</p>
              <ul className="list-disc pl-5 space-y-1 text-slate-500/80">
                <li>&quot;Give me a complete technical and fundamental analysis of ENGRO with entry/exit levels&quot;</li>
                <li>&quot;What is the PSX market breadth today? Show top gainers and losers&quot;</li>
                <li>&quot;Analyze OGDC sentiment trend — is news flow improving or deteriorating?&quot;</li>
                <li>&quot;What are the support and resistance levels for MCB with pivot points?&quot;</li>
              </ul>
            </div>
          )}
          {messages.map((msg, i) => (
            <div key={i} className={msg.role === 'user' ? 'text-right' : ''}>
              <div
                className={
                  msg.role === 'user'
                    ? 'inline-block bg-brand-600/30 text-brand-200 rounded-lg px-4 py-2 text-sm max-w-[90%]'
                    : 'inline-block bg-slate-700/50 border border-slate-600/60 text-slate-200 rounded-lg px-4 py-3 text-sm max-w-[95%] text-left'
                }
              >
                {msg.role === 'assistant' ? renderAssistantStructured(msg.content) : msg.content}
              </div>

              {msg.role === 'assistant' && (msg.sentiment || msg.scope || msg.retrieval) && (
                <div className="mt-1.5 text-xs text-slate-500 flex flex-wrap gap-2 items-center">
                  {msg.scope ? (
                    <span className="px-2 py-0.5 rounded border border-slate-600 bg-slate-800/50">
                      Scope: <span className="text-brand-300 font-mono">{msg.scope}</span>
                    </span>
                  ) : null}
                  {msg.sentiment ? (
                    <SentimentBadge sentiment={msg.sentiment} />
                  ) : null}
                  {msg.retrieval?.used_chunks != null ? (
                    <span className="px-2 py-0.5 rounded border border-slate-600 bg-slate-800/50">
                      RAG: {msg.retrieval.used_chunks} chunks
                      <span className="text-slate-600 mx-1">|</span>
                      {msg.retrieval.historical_chunks || 0} historical
                      {msg.retrieval.fundamentals_chunks ? (
                        <>
                          <span className="text-slate-600 mx-1">|</span>
                          {msg.retrieval.fundamentals_chunks} fundamentals
                        </>
                      ) : null}
                      <span className="text-slate-600 mx-1">|</span>
                      {msg.retrieval.news_chunks || 0} news
                    </span>
                  ) : null}
                  {msg.sources?.length > 0 ? (
                    <span className="px-2 py-0.5 rounded border border-slate-600 bg-slate-800/50">
                      Sources: {msg.sources.map(s => s.source || s.doc_type).filter(Boolean).slice(0, 4).join(', ')}
                    </span>
                  ) : null}
                </div>
              )}

            </div>
          ))}
          {loading && (
            <div className="flex items-center gap-2 text-slate-400 text-sm">
              <div className="flex gap-1">
                <span className="w-1.5 h-1.5 rounded-full bg-brand-400 animate-bounce" style={{ animationDelay: '0ms' }} />
                <span className="w-1.5 h-1.5 rounded-full bg-brand-400 animate-bounce" style={{ animationDelay: '150ms' }} />
                <span className="w-1.5 h-1.5 rounded-full bg-brand-400 animate-bounce" style={{ animationDelay: '300ms' }} />
              </div>
              Analyzing data and generating response...
            </div>
          )}
          <div ref={bottomRef} />
        </div>
        <div className="p-4 border-t border-slate-700/50 flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && send()}
            placeholder={mode === 'MARKET'
              ? 'Ask about PSX market outlook, breadth, top movers, sector trends...'
              : `Ask about ${activeScope} — analysis, buy/sell, news, risk, fundamentals...`}
            className="flex-1 bg-surface-900 border border-slate-600 rounded-lg px-4 py-2.5 text-white placeholder-slate-500 focus:ring-2 focus:ring-brand-500"
          />
          <button
            onClick={() => send()}
            disabled={loading || !input.trim()}
            className="bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white px-5 py-2.5 rounded-lg font-medium transition-colors"
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}
