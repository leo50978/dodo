const targetUrl = "./championnat.html?v=championnat-mopyon-v1";

try {
  window.location.replace(targetUrl);
} catch (_) {
  window.location.href = targetUrl;
}
