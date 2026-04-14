import React from 'react';

const FAQ = [
  {
    q: 'How do I start paper trading?',
    a: 'Go to Dashboard → Paper Trading Simulator, select a symbol, choose side/order type, set quantity, and place order.'
  },
  {
    q: 'What is the difference between Market and Limit order?',
    a: 'Market executes immediately at latest available simulated price. Limit stays pending until market reaches your limit.'
  },
  {
    q: 'How is P/L calculated?',
    a: 'Unrealized P/L uses current market price vs average cost. Realized P/L updates when a position is sold.'
  },
  {
    q: 'Can I reset my simulation account?',
    a: 'Yes. Use “Reset Simulation” to clear orders/positions/trades and restore initial virtual cash.'
  }
];

export function HelpCenterPage({ onOpenDashboard }) {
  return (
    <section className="page-stack">
      <div className="card hero-card">
        <div>
          <h2>Help Center</h2>
          <p className="muted-line">Quick guidance for market analysis, AI chat usage, and simulation workflows.</p>
        </div>
        <div className="hero-actions">
          <button className="toggle-btn active" onClick={onOpenDashboard}>Go to Dashboard</button>
        </div>
      </div>

      <div className="feature-grid feature-grid-2">
        {FAQ.map((item) => (
          <article className="card feature-card" key={item.q}>
            <h3>{item.q}</h3>
            <p className="muted-line">{item.a}</p>
          </article>
        ))}
      </div>
    </section>
  );
}
