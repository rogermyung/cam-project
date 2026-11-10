# WARN tracker site

React + TypeScript + Vite. See the Website section of the repository README for commands.

- `src/types.ts` is the data contract with `cam/warn/build.py`.
- `src/lib/notices.ts` holds the tested page logic and must keep type-only imports so `node --test` can run it.
- `public/data/` is sample data for `npm run dev`.
