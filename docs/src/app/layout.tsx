import "./global.css";
import { RootProvider } from "fumadocs-ui/provider/next";
import type { ReactNode } from "react";

export const metadata = {
  title: "Omnihedron Docs",
  description:
    "Documentation for omnihedron — a high-performance Rust rewrite of @subql/query",
};

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <RootProvider>{children}</RootProvider>
      </body>
    </html>
  );
}
