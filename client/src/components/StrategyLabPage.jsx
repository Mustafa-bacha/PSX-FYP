import React, { useState } from 'react';
import { apiClient } from '../api/client.js';

export function StrategyLabPage({ token, onOpenDashboard, onOpenMarket }) {
  // Calculator State
  const [capital, setCapital] = useState(500000);
  const [riskPercent, setRiskPercent] = useState(2);
  const [entryPrice, setEntryPrice] = useState(150);
  const [stopLoss, setStopLoss] = useState(142);
  
  // Playbook State
  const [activeTab, setActiveTab] = useState('momentum');
  
  // AI Consultant State
  const [aiPrompt, setAiPrompt] = useState('I want to swing trade high dividend stocks. What indicators should I use?');
  const [aiResponse, setAiResponse] = useState('');
  const [aiLoading, setAiLoading] = useState(false);

  // Math
  const riskAmount = (capital * riskPercent) / 100;
  const riskPerShare = entryPrice - stopLoss;
  const sharesToBuy = riskPerShare > 0 ? Math.floor(riskAmount / riskPerShare) : 0;
  const totalCost = sharesToBuy * entryPrice;

  const handleAskAI = async () => {
    if (!aiPrompt.trim()) return;
    setAiLoading(true);
    setAiResponse('');
    try {
      const resp = await apiClient.chat({
        stock: 'MARKET',
        question: `Act as an expert trading consultant. Formulate a brief, actionable trading roadmap for this user goal: ${aiPrompt}`,
        history: [],
        token: token || undefined
      });
      setAiResponse(resp?.answer || 'No strategy generated.');
    } catch (err) {
      setAiResponse('Failed to generate strategy: ' + err.message);
    } finally {
      setAiLoading(false);
    }
  };

  return (
    <section className="page-stack">
      <div className="card dashboard-toolbar">
        <div>
          <h2>Strategy Lab Toolkit</h2>
          <p className="muted-line">Build disciplined workflows, calculate precise risk sizes, and consult the AI for custom setups.</p>
        </div>
        <div className="toolbar">
          <button className="toggle-btn" onClick={onOpenMarket}>Live Market</button>
          <button className="toggle-btn active" onClick={onOpenDashboard}>Analysis Dashboard</button>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)', gap: '24px' }}>
        
        {/* Left Column: Risk Calculator & AI Consultant */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
          
          <article className="card">
            <h3 style={{ marginTop: 0, marginBottom: '16px' }}>Position Size Risk Calculator</h3>
            <p className="muted-line" style={{ marginBottom: '24px' }}>Protect your capital by mathematically defining your position size before you buy.</p>
            
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px', marginBottom: '24px' }}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                <label style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--color-text-muted)' }}>Total Capital (PKR)</label>
                <input type="number" min="1000" step="1000" value={capital} onChange={(e) => setCapital(Number(e.target.value))} style={{ padding: '10px' }} />
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                <label style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--color-text-muted)' }}>Max Risk %</label>
                <input type="number" min="0.1" step="0.1" max="100" value={riskPercent} onChange={(e) => setRiskPercent(Number(e.target.value))} style={{ padding: '10px' }} />
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                <label style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--color-text-muted)' }}>Target Entry Price</label>
                <input type="number" min="0.01" step="0.01" value={entryPrice} onChange={(e) => setEntryPrice(Number(e.target.value))} style={{ padding: '10px' }} />
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                <label style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--color-text-muted)' }}>Stop Loss Price</label>
                <input type="number" min="0.01" step="0.01" value={stopLoss} onChange={(e) => setStopLoss(Number(e.target.value))} style={{ padding: '10px' }} />
              </div>
            </div>

            <div style={{ background: 'var(--color-bg-base)', padding: '16px', border: '1px solid var(--border-color)', borderRadius: 'var(--radius-md)', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
              <div>
                <span style={{ display: 'block', fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>Max Capital at Risk</span>
                <strong style={{ fontSize: '1.2rem', color: 'var(--color-danger-text)' }}>PKR {riskAmount.toFixed(2)}</strong>
              </div>
              <div>
                <span style={{ display: 'block', fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>Risk Per Share</span>
                <strong style={{ fontSize: '1.2rem' }}>PKR {riskPerShare > 0 ? riskPerShare.toFixed(2) : 0}</strong>
              </div>
              <div>
                <span style={{ display: 'block', fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>Allowed Position Size</span>
                <strong style={{ fontSize: '1.5rem', color: 'var(--color-primary-dark)' }}>{sharesToBuy} Shares</strong>
              </div>
              <div>
                <span style={{ display: 'block', fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>Total Position Cost</span>
                <strong style={{ fontSize: '1.2rem' }}>PKR {totalCost.toFixed(2)}</strong>
              </div>
              {riskPerShare <= 0 && (
                <div style={{ gridColumn: 'span 2', color: 'var(--color-danger-text)', fontSize: '0.85rem', marginTop: '8px' }}>
                   * Stop loss must be lower than entry price for a long position.
                </div>
              )}
            </div>
          </article>

          <article className="card">
            <h3 style={{ marginTop: 0, marginBottom: '8px' }}>AI Mastermind Consultant</h3>
            <p className="muted-line" style={{ marginBottom: '16px' }}>Describe your trading goal and let the AI build a specific action plan.</p>
            <textarea 
              value={aiPrompt}
              onChange={(e) => setAiPrompt(e.target.value)}
              style={{ width: '100%', minHeight: '80px', padding: '12px', border: '1px solid var(--border-color)', borderRadius: 'var(--radius-md)', fontFamily: 'var(--font-primary)', marginBottom: '12px' }}
            />
            <button className="toggle-btn active" onClick={handleAskAI} disabled={aiLoading} style={{ width: '100%', justifyContent: 'center' }}>
              {aiLoading ? 'Generating Roadmap...' : 'Generate Roadmap'}
            </button>
            {aiResponse && (
              <div style={{ marginTop: '16px', padding: '16px', background: 'var(--color-bg-base)', border: '1px solid var(--color-primary-light)', borderRadius: 'var(--radius-md)', fontSize: '0.95rem', whiteSpace: 'pre-wrap' }}>
                <b style={{ color: 'var(--color-primary-dark)', display: 'block', marginBottom: '8px' }}>AI Strategy Roadmap:</b>
                {aiResponse}
              </div>
            )}
          </article>

        </div>

        {/* Right Column: Playbooks */}
        <div style={{ display: 'flex', flexDirection: 'column' }}>
          <article className="card" style={{ flex: 1 }}>
            <h3 style={{ marginTop: 0, marginBottom: '16px' }}>Interactive Playbooks</h3>
            
            <div style={{ display: 'flex', gap: '8px', borderBottom: '2px solid var(--border-color)', paddingBottom: '12px', marginBottom: '24px', overflowX: 'auto' }}>
              <button onClick={() => setActiveTab('momentum')} className="toggle-btn" style={{ background: activeTab === 'momentum' ? 'var(--color-primary-light)' : 'transparent', color: activeTab === 'momentum' ? 'var(--color-primary-dark)' : 'var(--color-text-main)', border: 'none', fontWeight: 600 }}>Momentum Filter</button>
              <button onClick={() => setActiveTab('sentiment')} className="toggle-btn" style={{ background: activeTab === 'sentiment' ? 'var(--color-primary-light)' : 'transparent', color: activeTab === 'sentiment' ? 'var(--color-primary-dark)' : 'var(--color-text-main)', border: 'none', fontWeight: 600 }}>Sentiment Reversal</button>
              <button onClick={() => setActiveTab('swing')} className="toggle-btn" style={{ background: activeTab === 'swing' ? 'var(--color-primary-light)' : 'transparent', color: activeTab === 'swing' ? 'var(--color-primary-dark)' : 'var(--color-text-main)', border: 'none', fontWeight: 600 }}>Slow Swing</button>
            </div>

            {activeTab === 'momentum' && (
              <div>
                <p className="muted-line" style={{ marginBottom: '24px' }}>Capture rapid price changes by identifying stocks forming new local highs with strong volume.</p>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                   <div style={{ padding: '16px', background: 'var(--color-bg-base)', borderLeft: '4px solid #0ea5e9', borderRadius: '4px' }}>
                     <b style={{ display: 'block', marginBottom: '4px' }}>Step 1: Dashboard Scan</b>
                     Check the Dashboard for symbols with `Price Trend = Positive` and `Prediction Confidence {'>'} 70%`.
                   </div>
                   <div style={{ padding: '16px', background: 'var(--color-bg-base)', borderLeft: '4px solid #0ea5e9', borderRadius: '4px' }}>
                     <b style={{ display: 'block', marginBottom: '4px' }}>Step 2: Technical Confirmation</b>
                     Verify that the moving averages are fanning out upwards (MA7 {'>'} MA20). Ensure Bollinger Bands are expanding, not contracting.
                   </div>
                   <div style={{ padding: '16px', background: 'var(--color-bg-base)', borderLeft: '4px solid #0ea5e9', borderRadius: '4px' }}>
                     <b style={{ display: 'block', marginBottom: '4px' }}>Step 3: Execution</b>
                     Use the Risk Calculator on this page. Enter 1% risk rule. Only deploy the absolute allowed share count into the Paper Simulator.
                   </div>
                </div>
              </div>
            )}

            {activeTab === 'sentiment' && (
              <div>
                <p className="muted-line" style={{ marginBottom: '24px' }}>Fade the crowd by finding stocks that have highly negative sentiment but positive price divergence, or vice versa.</p>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                   <div style={{ padding: '16px', background: 'var(--color-bg-base)', borderLeft: '4px solid #f59e0b', borderRadius: '4px' }}>
                     <b style={{ display: 'block', marginBottom: '4px' }}>Step 1: Find Disconnects</b>
                     Open the live market pulse and cross-reference with the Daily News summary. Look for companies flooded with "negative headlines" that are actually slightly green today.
                   </div>
                   <div style={{ padding: '16px', background: 'var(--color-bg-base)', borderLeft: '4px solid #f59e0b', borderRadius: '4px' }}>
                     <b style={{ display: 'block', marginBottom: '4px' }}>Step 2: AI Verification</b>
                     Open the AI Chat and ask: "Is the negative sentiment around [SYMBOL] priced in?" to gauge deeper fundamental risk.
                   </div>
                   <div style={{ padding: '16px', background: 'var(--color-bg-base)', borderLeft: '4px solid #f59e0b', borderRadius: '4px' }}>
                     <b style={{ display: 'block', marginBottom: '4px' }}>Step 3: Accumulation</b>
                     Do not buy in block. If the Risk Calculator permits 1000 shares, buy 300 today. Wait for tomorrow's close before accumulating more.
                   </div>
                </div>
              </div>
            )}

            {activeTab === 'swing' && (
              <div>
                <p className="muted-line" style={{ marginBottom: '24px' }}>Focus entirely on broader multi-week trajectories by anchoring analysis to MA_50 trends instead of daily noise.</p>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                   <div style={{ padding: '16px', background: 'var(--color-bg-base)', borderLeft: '4px solid #10b981', borderRadius: '4px' }}>
                     <b style={{ display: 'block', marginBottom: '4px' }}>Step 1: Setup 3M / 6M Chart</b>
                     Go to the Dashboard and switch the scope from 1M to 3M or 6M. Turn off MA_7 and only leave MA_20 and MA_50 on.
                   </div>
                   <div style={{ padding: '16px', background: 'var(--color-bg-base)', borderLeft: '4px solid #10b981', borderRadius: '4px' }}>
                     <b style={{ display: 'block', marginBottom: '4px' }}>Step 2: Pullback Sourcing</b>
                     Wait for the current price to naturally drift downwards to physically touch the MA_50 support line. No touch, no trade.
                   </div>
                   <div style={{ padding: '16px', background: 'var(--color-bg-base)', borderLeft: '4px solid #10b981', borderRadius: '4px' }}>
                     <b style={{ display: 'block', marginBottom: '4px' }}>Step 3: Fixed Targets</b>
                     Use the Simulator to place LIMIT orders exclusively. Target 2.5x RR (Reward to Risk). Do not look at everyday P/L until the target hits.
                   </div>
                </div>
              </div>
            )}

            <div style={{ marginTop: '24px', paddingTop: '24px', borderTop: '1px solid var(--border-color)', textAlign: 'center' }}>
               <p style={{ margin: 0, fontWeight: 500, color: 'var(--color-text-main)' }}>Always document your executed plays in a journal!</p>
            </div>
          </article>
        </div>
      </div>
    </section>
  );
}
