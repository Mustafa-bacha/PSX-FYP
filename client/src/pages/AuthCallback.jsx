import { useEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext.jsx';

export default function AuthCallback() {
  const { loading, isAuthenticated, login } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const token = params.get('token');
    const email = params.get('email');
    const fullName = params.get('name');
    const oauthError = params.get('error');
    const oauthMessage = params.get('message');

    if (oauthError) {
      const next = new URLSearchParams();
      next.set('oauth_error', oauthError);
      if (oauthMessage) next.set('oauth_message', oauthMessage);
      navigate(`/login?${next.toString()}`, { replace: true });
      return;
    }

    if (token && !isAuthenticated) {
      login({ email: email || '', full_name: fullName || '' }, token);
      navigate('/', { replace: true });
      return;
    }

    if (!loading && isAuthenticated) {
      navigate('/', { replace: true });
    }
  }, [location.search, navigate, login, isAuthenticated, loading]);

  if (loading || !isAuthenticated) {
    return (
      <div className="max-w-md mx-auto px-4 py-16 text-center">
        <h1 className="text-xl font-semibold text-white">Completing Google sign-in…</h1>
        <p className="text-slate-400 text-sm mt-2">Please wait while we verify your session.</p>
      </div>
    );
  }

  return null;
}
