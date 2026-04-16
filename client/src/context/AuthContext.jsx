import { createContext, useContext, useState, useCallback, useEffect } from 'react';
import { getToken } from '../api.js';
import { apiClient } from '../api/client.js';

const AuthContext = createContext(null);
const OAUTH_JUST_LOGGED_KEY = 'oauth_just_logged_in';

function persistToken(token) {
  if (!token) {
    localStorage.removeItem('token');
    localStorage.removeItem('psx_auth_token');
    return;
  }
  localStorage.setItem('token', token);
  localStorage.setItem('psx_auth_token', token);
}

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [token, setToken] = useState('');
  const [loading, setLoading] = useState(true);

  const logout = useCallback(() => {
    localStorage.removeItem('user');
    persistToken('');
    setUser(null);
    setToken('');
  }, []);

  const login = useCallback((userData, nextToken) => {
    persistToken(nextToken || '');
    localStorage.setItem('user', JSON.stringify(userData || {}));
    setToken(nextToken || '');
    setUser(userData || null);
  }, []);

  useEffect(() => {
    (async () => {
      const params = new URLSearchParams(window.location.search);
      const oauthToken = params.get('token');
      const oauthEmail = params.get('email');
      const oauthName = params.get('name');
      const oauthError = params.get('error');

      if (oauthError) {
        setLoading(false);
        return;
      }

      if (oauthToken) {
        const nextUser = { email: oauthEmail || '', full_name: oauthName || '' };
        login(nextUser, oauthToken);
        try {
          sessionStorage.setItem(OAUTH_JUST_LOGGED_KEY, '1');
        } catch {
          // ignore storage issues
        }
        window.history.replaceState({}, '', '/');
        setLoading(false);
        return;
      }

      const savedToken = getToken();
      const savedUser = localStorage.getItem('user');
      if (!savedToken || !savedUser) {
        setLoading(false);
        return;
      }

      try {
        const justLoggedInWithOAuth = sessionStorage.getItem(OAUTH_JUST_LOGGED_KEY) === '1';
        if (justLoggedInWithOAuth) {
          const parsed = JSON.parse(savedUser);
          setUser(parsed || null);
          setToken(savedToken);
          sessionStorage.removeItem(OAUTH_JUST_LOGGED_KEY);
          setLoading(false);
          return;
        }
      } catch {
        // continue with normal verification
      }

      try {
        const parsed = JSON.parse(savedUser);
        const me = await apiClient.me(savedToken);
        const verifiedUser = me?.user || parsed;
        setUser(verifiedUser);
        setToken(savedToken);
      } catch {
        logout();
      } finally {
        setLoading(false);
      }
    })();
  }, [login, logout]);

  return (
    <AuthContext.Provider value={{ user, token, loading, login, logout, isAuthenticated: !!user && !!token }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
