export function publicUrl(path: string): string {
  const base = import.meta.env.BASE_URL.endsWith("/")
    ? import.meta.env.BASE_URL
    : `${import.meta.env.BASE_URL}/`;
  const normalizedPath = String(path).replace(/^\/+/, "");
  return `${base}${normalizedPath}`;
}
