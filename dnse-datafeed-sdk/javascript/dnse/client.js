'use strict';

const { randomUUID } = require('crypto');
const { buildSignature, formatDateHeader } = require('./common');

class DNSEClient {
  constructor({ apiKey, apiSecret, baseUrl = 'https://openapi.dnse.com.vn', algorithm = 'hmac-sha256', hmacNonceEnabled = true }) {
    this.apiKey = apiKey;
    this.apiSecret = apiSecret;
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.algorithm = algorithm;
    this.hmacNonceEnabled = hmacNonceEnabled;
  }

  getOhlc(type, { query = {}, dryRun = false } = {}) {
    return this.#request('GET', '/price/ohlc', {
      query: { ...query, type },
      dryRun,
    });
  }

  async #request(method, path, { query, body, headers, dryRun } = {}) {
    const debug = String(process.env.DEBUG || '').toLowerCase() === 'true';
    const url = this.#buildUrl(path, query);
    const { dateValue, signatureHeaderValue } = this.#signatureHeaders(method, path);

    const requestHeaders = {
      Date: dateValue,
      'X-Signature': signatureHeaderValue,
      'x-api-key': this.apiKey,
    };

    if (body !== undefined) {
      requestHeaders['Content-Type'] = 'application/json';
    }

    if (headers) {
      Object.assign(requestHeaders, headers);
    }

    if (debug || dryRun) {
      const prefix = dryRun ? 'DRY RUN' : 'DEBUG';
      const queryParams = query || {};
      console.log(`${prefix} url:`, url);
      console.log(`${prefix} method:`, method);
      console.log(`${prefix} query_params:`, queryParams);
      console.log(`${prefix} headers:`, requestHeaders);
      console.log(`${prefix} body:`, body);
    }

    if (dryRun) {
      return { status: null, body: null };
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000); // Increased timeout to 30s

    try {
      const response = await fetch(url, {
        method,
        headers: requestHeaders,
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });

      clearTimeout(timeoutId);
      const responseBody = await response.text();
      return { status: response.status, body: responseBody };
    } catch (err) {
      clearTimeout(timeoutId);
      throw err;
    }
  }

  #buildUrl(path, query) {
    const url = new URL(`${this.baseUrl}${path}`);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        if (value !== undefined && value !== null) {
          url.searchParams.set(key, String(value));
        }
      }
    }
    return url.toString();
  }

  #signatureHeaders(method, path) {
    const dateValue = formatDateHeader(new Date());
    const nonce = this.hmacNonceEnabled ? randomUUID().replace(/-/g, '') : null;
    const { headers, signature } = buildSignature(
      this.apiSecret,
      method,
      path,
      dateValue,
      this.algorithm,
      nonce,
    );

    let signatureHeaderValue = `Signature keyId="${this.apiKey}",algorithm="${this.algorithm}",headers="${headers}",signature="${signature}"`;
    if (nonce) {
      signatureHeaderValue += `,nonce="${nonce}"`;
    }

    return { dateValue, signatureHeaderValue };
  }
}

module.exports = {
  DNSEClient,
};
