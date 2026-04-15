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

function renderAssistantStructured(content) {
  const lines = String(content || '').split('\n');
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
      'recommendation'
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
        <h2 key={`h2-${nodes.length}`} className="text-base md:text-lg font-bold text-white mt-2">
          {renderInlineRich(line.replace(/^##\s+/, ''))}
        </h2>
      );
      continue;
    }

    if (isPlainHeading(line)) {
      flushParagraph();
      flushList();
      nodes.push(
        <h2 key={`ph-${nodes.length}`} className="text-base md:text-lg font-bold text-white mt-2">
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

  const send = async (override) => {
    const qSource = typeof override === 'string' ? override : input;
    const q = String(qSource || '').trim();
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
          Comprehensive PSX Q&amp;A using historical market data + daily refreshed business news.
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
              onSelect={(ticker) => {
                const s = typeof ticker === 'string' ? ticker : String(ticker?.symbol ?? ticker ?? '');
                setSymbol(s.trim().toUpperCase());
              }}
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
            'Give me a full market outlook for today with key risks.',
            `Analyze ${activeScope} using historical trend + latest news and give balanced view.`,
            `What is the best and worst case scenario for ${activeScope} this week?`,
            'Which PSX stocks are strongest based on both momentum and sentiment?'
          ].map((prompt, idx) => (
            <button
              key={`${idx}-${prompt.slice(0, 12)}`}
              type="button"
              onClick={() => send(prompt)}
              disabled={loading}
              className="px-2.5 py-1.5 rounded border border-slate-600 text-slate-300 text-xs hover:bg-slate-700/50 disabled:opacity-50"
            >
              {idx === 0 ? 'Market Outlook' : idx === 1 ? 'Deep Stock Analysis' : idx === 2 ? 'Scenario Analysis' : 'Strongest Stocks'}
            </button>
          ))}
        </div>
      </section>

  <div className={`${cardClass} flex flex-col min-h-[500px] relative z-10`}>
        <div className="flex-1 overflow-y-auto p-4 space-y-4 min-h-[380px]">
          {messages.length === 0 && (
            <p className="text-slate-500 text-sm">
              Try: &quot;Analyze OGDC with historical and latest news evidence&quot; or &quot;Give a structured PSX market outlook with risks&quot;.
            </p>
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

              {msg.role === 'assistant' && (msg.sentiment || msg.scope) && (
                <div className="mt-1 text-xs text-slate-500 flex flex-wrap gap-2">
                  {msg.scope ? <span className="px-2 py-0.5 rounded border border-slate-600">Scope: {msg.scope}</span> : null}
                  {msg.sentiment ? <span className="px-2 py-0.5 rounded border border-slate-600">Sentiment: {msg.sentiment}</span> : null}
                  {msg.retrieval?.used_chunks != null ? (
                    <span className="px-2 py-0.5 rounded border border-slate-600">
                      RAG chunks: {msg.retrieval.used_chunks} (
                      {msg.retrieval.historical_chunks || 0} historical
                      {msg.retrieval.report_chunks != null ? `, ${msg.retrieval.report_chunks} PSX DPS` : ''}
                      , {msg.retrieval.news_chunks || 0} news)
                    </span>
                  ) : null}
                </div>
              )}

            </div>
          ))}
          {loading && <p className="text-slate-500 text-sm">Thinking…</p>}
          <div ref={bottomRef} />
        </div>
        <div className="p-4 border-t border-slate-700/50 flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && send()}
            placeholder={mode === 'MARKET'
              ? 'Ask anything about overall PSX with evidence...'
              : `Ask anything about ${activeScope} with evidence...`}
            className="flex-1 bg-surface-900 border border-slate-600 rounded-lg px-4 py-2.5 text-white placeholder-slate-500 focus:ring-2 focus:ring-brand-500"
          />
          <button
            type="button"
            onClick={() => send()}
            disabled={loading || !input.trim()}
            className="bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white px-5 py-2.5 rounded-lg font-medium"
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}
