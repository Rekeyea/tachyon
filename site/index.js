export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    
    // For SPA-style routing, always serve index.html for non-file paths
    if (url.pathname.endsWith('/') || url.pathname.split('.').length === 1) {
      url.pathname = '/index.html';
    }
    
    return env.ASSETS.fetch(request);
  },
};
