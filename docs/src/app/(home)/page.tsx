import Link from "next/link";

export default function HomePage() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center text-center px-4">
      <h1 className="text-4xl font-bold mb-4">Omnihedron</h1>
      <p className="text-fd-muted-foreground text-lg mb-8 max-w-xl">
        A high-performance Rust rewrite of <code>@subql/query</code>. Same
        PostgreSQL input, same GraphQL output — just faster.
      </p>
      <Link
        href="/docs"
        className="rounded-lg bg-fd-primary px-6 py-3 text-fd-primary-foreground font-medium"
      >
        Read the Docs
      </Link>
    </main>
  );
}
