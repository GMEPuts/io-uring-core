use std::io::{self, Read, Write};
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection};

use crate::error::CloseReason;
use crate::rxbuf::RxBuf;

/// A client config with the Mozilla root store (webpki-roots) and the crypto provider chosen
/// by the crate features. Share one config across connections so reconnects resume sessions.
#[cfg(any(feature = "ring", feature = "aws-lc-rs"))]
pub fn default_tls_config() -> Arc<ClientConfig> {
    #[cfg(feature = "ring")]
    let provider = rustls::crypto::ring::default_provider();
    #[cfg(all(feature = "aws-lc-rs", not(feature = "ring")))]
    let provider = rustls::crypto::aws_lc_rs::default_provider();

    let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.into() };
    Arc::new(
        ClientConfig::builder_with_provider(Arc::new(provider))
            .with_safe_default_protocol_versions()
            .expect("provider supports the default TLS versions")
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

pub(crate) struct Tls {
    conn: ClientConnection,
}

impl Tls {
    pub(crate) fn new(config: Arc<ClientConfig>, host: &str) -> Result<Self, CloseReason> {
        let name = ServerName::try_from(host.to_string()).map_err(|_| CloseReason::Tls)?;
        let conn = ClientConnection::new(config, name).map_err(|_| CloseReason::Tls)?;
        Ok(Self { conn })
    }

    #[inline]
    pub(crate) fn is_handshaking(&self) -> bool {
        self.conn.is_handshaking()
    }

    #[inline]
    pub(crate) fn wants_write(&self) -> bool {
        self.conn.wants_write()
    }

    /// Feeds one chunk of `ciphertext` (advancing the slice) and appends the decrypted plaintext
    /// to `rx`. Call in a loop until `ciphertext` is empty, consuming frames from `rx` between
    /// calls: draining after every chunk keeps rustls's bounded plaintext buffer from overflowing.
    #[inline]
    pub(crate) fn read_chunk(&mut self, ciphertext: &mut &[u8], rx: &mut RxBuf) -> Result<(), CloseReason> {
        let n = self.conn.read_tls(ciphertext).map_err(|_| CloseReason::Tls)?;
        if n == 0 {
            return Err(CloseReason::Tls);
        }
        self.conn.process_new_packets().map_err(|_| CloseReason::Tls)?;
        loop {
            if !rx.ensure_spare() {
                return Err(CloseReason::RxOverflow);
            }
            match self.conn.reader().read(rx.spare_mut()) {
                Ok(0) => return Err(CloseReason::PeerClosed),
                Ok(n) => rx.commit(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(_) => return Err(CloseReason::Tls),
            }
        }
    }

    #[inline]
    pub(crate) fn write_plaintext(&mut self, data: &[u8]) -> Result<(), CloseReason> {
        self.conn.writer().write_all(data).map_err(|_| CloseReason::Tls)
    }

    /// Appends pending ciphertext to `out`.
    #[inline]
    pub(crate) fn write_ciphertext(&mut self, out: &mut Vec<u8>) -> Result<(), CloseReason> {
        while self.conn.wants_write() {
            if self.conn.write_tls(out).map_err(|_| CloseReason::Tls)? == 0 {
                break;
            }
        }
        Ok(())
    }
}
