import React, { useState } from 'react';
import { apiClient } from '../api/client.js';

export function AuthPage({ onAuthSuccess }) {
  const [mode, setMode] = useState('login');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [fullName, setFullName] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const submit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      const payload = mode === 'signup'
        ? await apiClient.signup({ email, password, full_name: fullName })
        : await apiClient.login({ email, password });

      onAuthSuccess(payload.token, payload.user);
    } catch (err) {
      setError(err.message || 'Authentication failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className="auth-wrap">
      <section className="auth-card">
        <h1>{mode === 'signup' ? 'Create account' : 'Log in'}</h1>
        <p className="auth-subtext">Sign in to continue with PSX analytics workspace.</p>

        <div className="auth-tabs">
          <button className={mode === 'login' ? 'active' : ''} onClick={() => setMode('login')}>Login</button>
          <button className={mode === 'signup' ? 'active' : ''} onClick={() => setMode('signup')}>Sign Up</button>
        </div>

        <form className="auth-form" onSubmit={submit}>
          {mode === 'signup' && (
            <label>
              Full Name
              <input
                type="text"
                value={fullName}
                onChange={(e) => setFullName(e.target.value)}
                placeholder="Your name"
                autoComplete="name"
              />
            </label>
          )}

          <label>
            Email
            <input
              type="text"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="name@example.com"
              autoComplete="username"
              required
            />
          </label>

          <label>
            Password
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
              autoComplete={mode === 'signup' ? 'new-password' : 'current-password'}
              required
            />
          </label>

          {mode === 'signup' && (
            <small className="muted-line">Use at least 8 chars with uppercase, lowercase, and number.</small>
          )}

          {error && <div className="error-box">{error}</div>}

          <button type="submit" className="auth-submit" disabled={loading}>
            {loading ? 'Please wait…' : mode === 'signup' ? 'Create account' : 'Sign in'}
          </button>
        </form>

  <div className="auth-divider"><span>or</span></div>

        <a className="google-btn" href={apiClient.googleStartUrl()}>
          Continue with Google
        </a>
      </section>
    </main>
  );
}
