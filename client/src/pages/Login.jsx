import { useState } from 'react';
import { Link, Navigate, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext.jsx';
import { auth } from '../api.js';
import { inputClass, labelClass, btnPrimaryClass, cardClass } from '../lib/constants.js';

export default function Login() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const { login, isAuthenticated } = useAuth();
  const navigate = useNavigate();

  if (isAuthenticated) {
    return <Navigate to="/" replace />;
  }

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const res = await auth.login(email, password);
      login(res.user, res.access_token);
      navigate('/dashboard');
    } catch (err) {
      setError(err.error || err.message || 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-[calc(100vh-72px)] grid place-items-center px-4 py-8">
      <section className={`${cardClass} w-full max-w-[560px] border-brand-500/40 px-7 py-8`}>
        <p className="text-xs tracking-[0.35em] uppercase text-brand-400 font-semibold mb-3">Secure Workspace</p>
        <h1 className="text-5xl md:text-4xl font-extrabold text-white leading-tight mb-3">PSX Insight Dashboard</h1>
        <p className="text-slate-300 text-2xl md:text-xl mb-6 max-w-[46ch]">
          Sign in to keep portfolios, chats, and activity separated per user.
        </p>

        <div className="grid grid-cols-2 gap-2 mb-5 rounded-xl border border-slate-600/70 bg-slate-900/40 p-1">
          <button
            type="button"
            className="rounded-lg px-4 py-2.5 text-sm font-semibold bg-brand-500/20 text-brand-300 border border-brand-500/40"
            aria-current="page"
          >
            Login
          </button>
          <Link
            to="/signup"
            className="rounded-lg px-4 py-2.5 text-center text-sm font-semibold text-slate-200 border border-transparent hover:border-slate-500 hover:bg-slate-700/40 transition-colors"
          >
            Sign Up
          </Link>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          {error && <p className="text-red-400 text-sm">{error}</p>}

          <div>
            <label className={labelClass}>Email</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className={`${inputClass} bg-slate-900/60`}
              required
            />
          </div>

          <div>
            <label className={labelClass}>Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className={`${inputClass} bg-slate-900/60`}
              required
            />
          </div>

          <button type="submit" disabled={loading} className={`w-full ${btnPrimaryClass}`}>
            {loading ? 'Signing in...' : 'Sign in'}
          </button>
        </form>

        <p className="mt-5 text-slate-400 text-sm text-center">
          Don&apos;t have an account? <Link to="/signup" className="text-brand-400 hover:underline">Sign up</Link>
        </p>
      </section>
    </div>
  );
}
