import { useState, useRef, useEffect } from 'react';
import { chatbot as chatbotApi } from '../api.js';
import { cardClass } from '../lib/constants.js';

export default function Chatbot() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const send = async () => {
    const q = input.trim();
    if (!q || loading) return;
    setInput('');
    setMessages((m) => [...m, { role: 'user', content: q }]);
    setLoading(true);
    try {
      const res = await chatbotApi.ask(q);
      setMessages((m) => [
        ...m,
        { role: 'assistant', content: res.answer, sources: res.sources || [] },
      ]);
    } catch {
      setMessages((m) => [
        ...m,
        { role: 'assistant', content: 'Sorry, I could not get an answer. Try again.', sources: [] },
      ]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className={`${cardClass} flex flex-col`} style={{ minHeight: 360 }}>
      <div className="px-4 py-3 border-b border-slate-700/50">
        <h3 className="font-semibold text-white">Chatbot</h3>
        <p className="text-slate-400 text-xs">Ask about stocks, predictions, or sentiment.</p>
      </div>
      <div className="flex-1 overflow-y-auto p-4 space-y-3 min-h-[200px]">
        {messages.length === 0 && (
          <p className="text-slate-500 text-sm">Ask a question, e.g. What is the prediction for OGDC?</p>
        )}
        {messages.map((msg, i) => (
          <div key={i} className={msg.role === 'user' ? 'text-right' : ''}>
            <div
              className={
                msg.role === 'user'
                  ? 'inline-block bg-brand-600/30 text-brand-200 rounded-lg px-3 py-2 text-sm max-w-[85%]'
                  : 'inline-block bg-slate-700/50 text-slate-200 rounded-lg px-3 py-2 text-sm max-w-[85%] text-left'
              }
            >
              {msg.content}
            </div>
          </div>
        ))}
        {loading && <p className="text-slate-500 text-sm">Thinking…</p>}
        <div ref={bottomRef} />
      </div>
      <div className="p-3 border-t border-slate-700/50 flex gap-2">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && send()}
          placeholder="Type your question..."
          className="flex-1 bg-surface-900 border border-slate-600 rounded-lg px-3 py-2 text-white text-sm placeholder-slate-500 focus:ring-2 focus:ring-brand-500"
        />
        <button
          onClick={send}
          disabled={loading || !input.trim()}
          className="bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white px-4 py-2 rounded-lg text-sm font-medium"
        >
          Send
        </button>
      </div>
    </div>
  );
}
