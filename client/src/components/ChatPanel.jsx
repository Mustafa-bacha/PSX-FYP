import React, { useState } from 'react';

export function ChatPanel({
  stock,
  messages,
  onSend,
  loading,
  marketMode = false,
  onToggleMode = () => {},
  onClearHistory = () => {}
}) {
  const [input, setInput] = useState('');

  const submit = (e) => {
    e.preventDefault();
    const q = input.trim();
    if (!q || loading) return;
    onSend(q);
    setInput('');
  };

  return (
    <section className="card chat">
      <div className="chat-head">
        <h3>AI Stock Chat ({marketMode ? 'Overall Market' : (stock || '—')})</h3>
        <div className="chat-actions">
          <button type="button" className="toggle-btn" onClick={onClearHistory}>
            Clear History
          </button>
          <button type="button" className={`toggle-btn ${marketMode ? 'active' : ''}`} onClick={onToggleMode}>
            {marketMode ? 'Market Mode' : 'Stock Mode'}
          </button>
        </div>
      </div>
      <div className="chat-log">
        {!messages.length ? <p className="muted-line">Ask about this stock, or switch to market mode for buy/sell ideas across PSX.</p> : null}
        {messages.map((m, i) => (
          <div key={i} className={`msg ${m.role}`}>
            <b>{m.role === 'user' ? 'You' : 'Assistant'}:</b>
            <p>{m.content}</p>
          </div>
        ))}
      </div>
      <form onSubmit={submit} className="chat-form">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder={marketMode
            ? 'Ask: which stocks look strongest today and why?'
            : 'Ask: why is prediction upward/downward for this stock?'}
        />
        <button type="submit" disabled={loading}>{loading ? 'Thinking...' : 'Send'}</button>
      </form>
    </section>
  );
}
