//! Minimal URL parsing for `ws://`, `wss://`, `http://` and `https://` endpoints.

use std::fmt;

/// URL scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// `ws://`
    Ws,
    /// `wss://`
    Wss,
    /// `http://`
    Http,
    /// `https://`
    Https,
}

impl Scheme {
    /// Whether the scheme runs over TLS.
    pub fn is_tls(self) -> bool {
        matches!(self, Scheme::Wss | Scheme::Https)
    }

    /// The port used when the URL doesn't name one.
    pub fn default_port(self) -> u16 {
        if self.is_tls() {
            443
        } else {
            80
        }
    }
}

/// A parsed endpoint URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Url {
    /// Scheme.
    pub scheme: Scheme,
    /// Host name or IP literal, without IPv6 brackets.
    pub host: String,
    /// Port (explicit or the scheme default).
    pub port: u16,
    /// Path plus query, always starting with `/`. The fragment is dropped.
    pub path: String,
}

/// Why a URL was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UrlError {
    /// No `scheme://` prefix.
    MissingScheme,
    /// A scheme other than ws, wss, http, https, or one the connection type doesn't support.
    UnsupportedScheme(String),
    /// Empty host.
    MissingHost,
    /// Port isn't a number in 1..=65535.
    BadPort(String),
}

impl fmt::Display for UrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UrlError::MissingScheme => f.write_str("missing scheme (expected e.g. wss://)"),
            UrlError::UnsupportedScheme(s) => write!(f, "unsupported scheme {s:?}"),
            UrlError::MissingHost => f.write_str("missing host"),
            UrlError::BadPort(p) => write!(f, "bad port {p:?}"),
        }
    }
}

impl std::error::Error for UrlError {}

impl Url {
    /// Parses `scheme://host[:port][/path][?query][#fragment]`.
    pub fn parse(s: &str) -> Result<Url, UrlError> {
        let (scheme, rest) = s.trim().split_once("://").ok_or(UrlError::MissingScheme)?;
        let scheme = match scheme.to_ascii_lowercase().as_str() {
            "ws" => Scheme::Ws,
            "wss" => Scheme::Wss,
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            other => return Err(UrlError::UnsupportedScheme(other.to_string())),
        };

        let split = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(split);
        // Drop any userinfo; credentials never go in the URL here.
        let authority = authority.rsplit_once('@').map_or(authority, |(_, a)| a);

        let (host, port) = if let Some(after) = authority.strip_prefix('[') {
            let (host, after) = after.split_once(']').ok_or(UrlError::MissingHost)?;
            match after.strip_prefix(':') {
                Some(p) => (host, Some(p)),
                None if after.is_empty() => (host, None),
                None => return Err(UrlError::BadPort(after.to_string())),
            }
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h, Some(p)),
                None => (authority, None),
            }
        };
        if host.is_empty() {
            return Err(UrlError::MissingHost);
        }
        let port = match port {
            Some(p) => match p.parse::<u16>() {
                Ok(n) if n > 0 => n,
                _ => return Err(UrlError::BadPort(p.to_string())),
            },
            None => scheme.default_port(),
        };

        let tail = tail.split_once('#').map_or(tail, |(t, _)| t);
        let path = if tail.is_empty() {
            "/".to_string()
        } else if tail.starts_with('?') {
            format!("/{tail}")
        } else {
            tail.to_string()
        };

        Ok(Url { scheme, host: host.to_string(), port, path })
    }

    /// Value for the HTTP `Host` header: the port is included only when it isn't the scheme
    /// default, and IPv6 literals are bracketed.
    pub fn host_header(&self) -> String {
        let host = if self.host.contains(':') { format!("[{}]", self.host) } else { self.host.clone() };
        if self.port == self.scheme.default_port() {
            host
        } else {
            format!("{host}:{}", self.port)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_forms() {
        let u = Url::parse("wss://fstream.binance.com/public/ws").unwrap();
        assert_eq!((u.scheme, u.host.as_str(), u.port, u.path.as_str()), (Scheme::Wss, "fstream.binance.com", 443, "/public/ws"));
        assert_eq!(u.host_header(), "fstream.binance.com");

        let u = Url::parse("wss://stream.binance.com:9443/ws").unwrap();
        assert_eq!(u.port, 9443);
        assert_eq!(u.host_header(), "stream.binance.com:9443");

        let u = Url::parse("ws://localhost").unwrap();
        assert_eq!((u.port, u.path.as_str()), (80, "/"));

        let u = Url::parse("wss://ws.chain.link?feedIDs=abc#frag").unwrap();
        assert_eq!(u.path, "/?feedIDs=abc");

        let u = Url::parse("WSS://a.b/x?y=1").unwrap();
        assert_eq!((u.scheme, u.path.as_str()), (Scheme::Wss, "/x?y=1"));
    }

    #[test]
    fn parses_ipv6_literals() {
        let u = Url::parse("ws://[::1]:8080/ws").unwrap();
        assert_eq!((u.host.as_str(), u.port), ("::1", 8080));
        assert_eq!(u.host_header(), "[::1]:8080");
        let u = Url::parse("wss://[2001:db8::1]/").unwrap();
        assert_eq!((u.host.as_str(), u.port), ("2001:db8::1", 443));
    }

    #[test]
    fn rejects_bad_input() {
        assert_eq!(Url::parse("stream.binance.com"), Err(UrlError::MissingScheme));
        assert!(matches!(Url::parse("ftp://x"), Err(UrlError::UnsupportedScheme(_))));
        assert_eq!(Url::parse("wss:///path"), Err(UrlError::MissingHost));
        assert!(matches!(Url::parse("wss://h:0/"), Err(UrlError::BadPort(_))));
        assert!(matches!(Url::parse("wss://h:99999/"), Err(UrlError::BadPort(_))));
    }
}
