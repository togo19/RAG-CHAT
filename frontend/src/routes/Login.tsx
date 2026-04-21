import { Link } from 'react-router-dom';
import { API_BASE } from '@/lib/apiBase';
import ShaderBackground from '@/components/ui/shader-background';

export default function Login() {
  return (
    <main className="theme-dark relative isolate flex min-h-screen items-center justify-center p-6 overflow-hidden">
      <ShaderBackground />
      <div className="relative z-10 w-full max-w-sm rounded-2xl border border-white/10 bg-white/5 p-8 shadow-2xl backdrop-blur-xl">
        <h1 className="text-2xl font-semibold tracking-tight text-foreground">Sign in</h1>
        <p className="mt-2 text-sm text-muted-foreground">
          Continue with your Google account to chat with the knowledge base.
        </p>
        <a
          href={`${API_BASE}/auth/google/login`}
          className="mt-6 inline-flex w-full items-center justify-center rounded-md bg-accent px-4 py-2.5 text-sm font-medium text-accent-foreground transition hover:bg-accent-hover focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-transparent"
        >
          Continue with Google
        </a>
        <div className="mt-6 text-center">
          <Link
            to="/admin/login"
            className="text-xs font-medium text-muted-foreground transition hover:text-foreground"
          >
            Admin sign in
          </Link>
        </div>
      </div>
    </main>
  );
}
