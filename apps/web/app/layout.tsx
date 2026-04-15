import "./globals.css";
import type { Metadata } from "next";
import type { ReactNode } from "react";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Aligna Markets",
  description:
    "Market intelligence for prediction markets. Detect structural weakness, demand mismatch, and intervention candidates.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="bg-zinc-50 text-zinc-900 antialiased">
        <div className="min-h-screen">
          <header className="border-b border-zinc-200 bg-white">
            <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
              <div>
                <Link href="/" className="block">
                  <h1 className="text-xl font-bold">Aligna Markets</h1>
                </Link>
                <p className="text-sm text-zinc-500">
                  Structural intelligence for prediction markets
                </p>
              </div>

              <nav className="flex items-center gap-6 text-sm font-medium text-zinc-700">
                <Link href="/" className="hover:text-zinc-900">
                  Home
                </Link>
               <Link href="/explorer" className="hover:text-zinc-900">
  Explorer
</Link>
<Link href="/launch-review" className="hover:text-zinc-900">
  Launch Review
</Link>
<Link href="/methodology" className="hover:text-zinc-900">
  Methodology
</Link>
              </nav>
            </div>
          </header>

          <main className="mx-auto max-w-7xl px-6 py-8">{children}</main>
        </div>
      </body>
    </html>
  );
}